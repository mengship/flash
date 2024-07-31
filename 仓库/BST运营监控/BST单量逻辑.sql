-- 单量逻辑 发货单量+流入单量+销退单量+入库单量
select
dt.date 日期
,'BST' 仓库
,out1.发货单量
,out2.流入单量
,out2.审核单量
,out2.未审核单量
,in1.入库单量
,in2.销退单量
from
(SELECT date FROM tmpale.ods_th_dim_date where date>= date_sub(date(now() + interval -1 hour),interval 30 day) and date<date(now() + interval -1 hour)) dt
  left join
(
        -- BST发货单量
    select
    date(快递已签收时间)  订单创建日期
    ,仓库
    ,count(if(订单审核时间 is not null, 发货单号, null)) 发货单量
    from
    (
            SELECT
                w.`name` 仓库名称,
                'wms发货单' 类型,
                vp.physicalwarehouse_name 仓库,
                s.`name` as 货主名称,
                do.`delivery_sn` 发货单号,
                dog.kinds_num 品种数量,
                dog.商品数量 商品数量,
                dog.修正商品数量,
                dog.dogvolume 商品体积,
                dog.dogweight 商品重量,
                do.express_name 快递公司,
                case
                    do.`status`
                    when 1000 then '取消发货'
                    when 1002 then '等待激活'
                    when 1003 then '预售订单'
                    when 1005 then '待分仓'
                    when 1007 then '已分仓' -- 货主审核订单前，已经知道系统缺货进行的状态提示。正常状态客户不应该审核通过这些订单
                    when 1010 then '缺货'
                    when 1015 then '已分仓(废弃)'
                    when 1020 then '等待审核'
                    when 1030 then '审核完成' -- 调用快递系统。多长时间提示? 金额是否有小数点？收件人，寄件人，电话是否有？ 号单是否匹配？  超过30分钟获取面单失败的数据
                    when 1035 then '获取电子面单号失败'
                    when 1040 then '获取电子面单号成功' -- 审核通过且获取面单成功后，系统分配库存失败
                    when 2000 then '库存分配暂停' -- 审核通过且获取面单成功后，系统分配库存失败
                    when 2005 then '分配库存失败'
                    when 2010 then '分配库存成功'
                    when 2015 then '分配预打包成功'
                    when 2016 then '生成波次成功'
                    when 2020 then '等待拣货'
                    when 2030 then '拣货完成'
                    when 2035 then '换单待打印'
                    when 2040 then '打包完成'
                    when 2050 then '开始交接'
                    when 2060 then '发货完成'
                    when 3010 then '配送中'
                    when 3013 then '配送异常'
                    when 3015 then '已拒收'
                    when 3018 then '部分拒收'
                    when 3020 then '已签收'
                    when 3050 then '虚拟发货'
                    else '其他'
                end 订单状态,
                date_add(do.`created`, interval -60 minute) 订单创建时间,
                date_add(do.`audit_time`, interval -60 minute) 订单审核时间,
                nvl(do.wait_pick, do.`allocation_time`) 生成拣货单时间,
                do.`succ_pick` 拣货完成时间,
                do.`pack_time` 打包完成时间,
                do.`start_receipt ` 绑定交接单时间,
                date_add(do.`delivery_time`, interval -60 minute) 快递已签收时间,
                case
                    do.`system_short`
                    when 1 then '系统报缺'
                    when 2 then '待补货'
                    when 3 then '待上架'
                end 系统提示,
                if(m.job_number = '323100', '32310', m.job_number) 复核人ID,
                -- 复核人
                if(do.status in ('1035', '2005'), 'error', 'normal') 是否为异常单,
                date(date_add(now(), interval -60 minute)) today,
                date_add(now(), interval -60 minute) 运行时间,
                pick.pid pick_id,
                -- 拣货人
                mp.job_number pack_id,
                -- 打包人
                mo.job_number handover_id,
                -- 交接人
                md.job_number sign_id, -- 签收人
                express_sn,
                do.logistic_company_id,
                sps.store_name,
                case when ps.code in ('009', 'STTGWH') then 'TikTok'
                    when ps.code in ('002') then 'Shopee'
                else ps.name end as salePlatformName
            FROM
                (
                    SELECT
                        *
                    FROM
                        `wms_production`.`delivery_order` do
                    WHERE 1=1
                ) do
                LEFT JOIN `wms_production`.`seller` s ON do.`seller_id` = s.`id`
                LEFT JOIN `wms_production`.`warehouse` w ON do.`warehouse_id` = w.`id`
                left join tmpale.dim_th_ffm_virtualphysical vp on w.name = vp.virtualwarehouse_name
                left join (
                    -- 拣货人
                    select
                        do.delivery_sn,
                        group_concat(
                            mb.`job_number`
                            order by
                                po.created desc separator ';'
                        ) pid
                    from
                        `wms_production`.`delivery_order` do
                        left join `wms_production`.`pick_order_delivery_ref` podr on do.`id` = podr.`delivery_order_id`
                        left join `wms_production`.`pick_order` po on podr.`pick_order_id` = po.`id`
                        LEFT JOIN `wms_production`.`member` mb on po.`picker_id` = mb.id
                    where
                        do.created >= '2021-12-01'
                    group by
                        do.delivery_sn
                ) pick on do.delivery_sn = pick.delivery_sn
                LEFT JOIN (
                    -- 打包人信息
                    select
                        delivery_order_id,
                        pack_id,
                        creator_id,
                        `workstation_id`,
                        times,
                        count(1) as boxNum
                    from
                        wms_production.delivery_box
                        where created >= '2021-12-01'
                    group by
                        delivery_order_id,
                        pack_id,
                        creator_id,
                        `workstation_id`,
                        times
                ) db on db.delivery_order_id = do.id
                and db.times = do.times
                LEFT JOIN wms_production.`member` m on m.id = db.creator_id -- 复核人
                left join wms_production.member mo on do.operator_id = mo.id -- 交接人
                left join wms_production.member mp on db.pack_id = mp.id -- 打包人
                left join wms_production.member md on do.delivery_out_id = md.id -- 签收人
                LEFT JOIN (
                            SELECT
                                dog.`delivery_order_id`,
                                count(distinct dog.`seller_goods_id`) SKU,
                                sum(dog.`goods_number`) 商品数量,
                                sum(dog.`three_num` + dog.`two_num` + dog.`one_num`) 修正商品数量,
                                sum(goods_number * volume) dogvolume,
                                sum(goods_number * weight) dogweight,
                                count(distinct seller_goods_id) kinds_num
                            FROM
                                (
                                    SELECT
                                        delivery_order_id,
                                        seller_goods_id,
                                        goods_number,
                                        two_conversion,
                                        three_conversion,
                                        if(
                                            three_conversion > 0,
                                            floor(goods_number / three_conversion),
                                            0
                                        ) three_num,
                                        case
                                            when three_conversion > 0
                                            AND two_conversion > 0 then floor(goods_number % three_conversion / two_conversion)
                                            when three_conversion > 0
                                            AND two_conversion = 0 then floor(goods_number / three_conversion)
                                            when three_conversion = 0
                                            AND two_conversion > 0 then floor(goods_number / two_conversion)
                                            else 0
                                        end two_num,
                                        case
                                            when three_conversion > 0
                                            AND two_conversion > 0 then (goods_number % three_conversion % two_conversion)
                                            when three_conversion > 0
                                            AND two_conversion = 0 then (goods_number % three_conversion)
                                            when three_conversion = 0
                                            AND two_conversion > 0 then (goods_number % two_conversion)
                                            else goods_number
                                        end one_num,
                                        volume,
                                        weight
                                    FROM
                                        (
                                            SELECT
                                                dog.`delivery_order_id` delivery_order_id,
                                                dog.`seller_goods_id` seller_goods_id,
                                                dog.`goods_number` goods_number,
                                                ifnull(sg.`two_conversion`, 0) two_conversion,
                                                ifnull(sg.`three_conversion`, 0) three_conversion,
                                                volume,
                                                weight
                                            FROM
                                                `wms_production`.`delivery_order_goods` dog
                                                LEFT JOIN `wms_production`.`seller_goods` sg ON dog.`seller_goods_id` = sg.`id`
                                                where dog.created >= '2021-12-01'
                                        ) dog
                                ) dog
                            GROUP BY
                                dog.`delivery_order_id`
                        ) dog on dog.`delivery_order_id` = do.`id`
                left join wms_production.seller_platform_source sps on do.platform_source_id = sps.id
                left join wms_production.platform_source ps on sps.platform_source_id = ps.id
            WHERE
                w.`name` in (
                    'BangsaoThong',
                    'Fans-B2B',
                    'Fans-B2C',
                    'Fans-Demo',
                    '广州前置仓',
                    'PMD-WH',
                    'BKK-WH-LAS物料仓',
                    'BKK-WH-Ecommerce',
                    'AutoWarehouse',
                    'BPL-Return Warehouse',
                    'BKK-WH-LAS2电商仓'
                )
                AND s.name not in (
                    'CG - Medela(lazada)',
                    'CG - P&G',
                    'CG - Dole (Shopee)',
                    'CG - Nestle',
                    'CG - Philips',
                    'Welink',
                    'MAX SPEED',
                    'Pongo',
                    'VT SHOP',
                    'J SHOP',
                    'JSHOP',
                    'Auto-TestingPartner',
                    'YXS',
                    'Intrepid - Darlie',
                    'Think of Style'
                )
                AND (s.name REGEXP '^CG -.*$') != 1
                AND do.`created` >= '2023-07-01'
        ) t0
        where 1=1
        and    仓库='BST'
        and     date(快递已签收时间)>=date_sub(date(now() + interval -1 hour),interval 30 day)
        group by date(快递已签收时间),仓库
        /* order by date(快递已签收时间) desc */
) out1 on dt.date = out1.订单创建日期
left join
(
   -- BST流入单量
    select
    date(订单创建时间)  订单创建日期
    ,仓库
    ,count(发货单号)    流入单量
    ,count(if(订单审核时间 is not null, 发货单号, null)) 审核单量
    ,count(发货单号) - count(if(订单审核时间 is not null, 发货单号, null)) 未审核单量
    from
    (
        SELECT
            w.`name` 仓库名称,
            'wms发货单' 类型,
            vp.physicalwarehouse_name 仓库,
            s.`name` as 货主名称,
            do.`delivery_sn` 发货单号,
            dog.kinds_num 品种数量,
            dog.商品数量 商品数量,
            dog.修正商品数量,
            dog.dogvolume 商品体积,
            dog.dogweight 商品重量,
            do.express_name 快递公司,
            case
                do.`status`
                when 1000 then '取消发货'
                when 1002 then '等待激活'
                when 1003 then '预售订单'
                when 1005 then '待分仓'
                when 1007 then '已分仓' -- 货主审核订单前，已经知道系统缺货进行的状态提示。正常状态客户不应该审核通过这些订单
                when 1010 then '缺货'
                when 1015 then '已分仓(废弃)'
                when 1020 then '等待审核'
                when 1030 then '审核完成' -- 调用快递系统。多长时间提示? 金额是否有小数点？收件人，寄件人，电话是否有？ 号单是否匹配？  超过30分钟获取面单失败的数据
                when 1035 then '获取电子面单号失败'
                when 1040 then '获取电子面单号成功' -- 审核通过且获取面单成功后，系统分配库存失败
                when 2000 then '库存分配暂停' -- 审核通过且获取面单成功后，系统分配库存失败
                when 2005 then '分配库存失败'
                when 2010 then '分配库存成功'
                when 2015 then '分配预打包成功'
                when 2016 then '生成波次成功'
                when 2020 then '等待拣货'
                when 2030 then '拣货完成'
                when 2035 then '换单待打印'
                when 2040 then '打包完成'
                when 2050 then '开始交接'
                when 2060 then '发货完成'
                when 3010 then '配送中'
                when 3013 then '配送异常'
                when 3015 then '已拒收'
                when 3018 then '部分拒收'
                when 3020 then '已签收'
                when 3050 then '虚拟发货'
                else '其他'
            end 订单状态,
            date_add(do.`created`, interval -60 minute) 订单创建时间,
            date_add(do.`audit_time`, interval -60 minute) 订单审核时间,
            nvl(do.wait_pick, do.`allocation_time`) 生成拣货单时间,
            do.`succ_pick` 拣货完成时间,
            do.`pack_time` 打包完成时间,
            do.`start_receipt ` 绑定交接单时间,
            date_add(do.`delivery_time`, interval -60 minute) 快递已签收时间,
            case
                do.`system_short`
                when 1 then '系统报缺'
                when 2 then '待补货'
                when 3 then '待上架'
            end 系统提示,
            if(m.job_number = '323100', '32310', m.job_number) 复核人ID,
            -- 复核人
            if(do.status in ('1035', '2005'), 'error', 'normal') 是否为异常单,
            date(date_add(now(), interval -60 minute)) today,
            date_add(now(), interval -60 minute) 运行时间,
            pick.pid pick_id,
            -- 拣货人
            mp.job_number pack_id,
            -- 打包人
            mo.job_number handover_id,
            -- 交接人
            md.job_number sign_id, -- 签收人
            express_sn,
            do.logistic_company_id,
            sps.store_name,
            case when ps.code in ('009', 'STTGWH') then 'TikTok'
                when ps.code in ('002') then 'Shopee'
            else ps.name end as salePlatformName
        FROM
            (
                SELECT
                    *
                FROM
                    `wms_production`.`delivery_order` do
                WHERE 1=1
            ) do
            LEFT JOIN `wms_production`.`seller` s ON do.`seller_id` = s.`id`
            LEFT JOIN `wms_production`.`warehouse` w ON do.`warehouse_id` = w.`id`
            left join tmpale.dim_th_ffm_virtualphysical vp on w.name = vp.virtualwarehouse_name
            left join (
                -- 拣货人
                select
                    do.delivery_sn,
                    group_concat(
                        mb.`job_number`
                        order by
                            po.created desc separator ';'
                    ) pid
                from
                    `wms_production`.`delivery_order` do
                    left join `wms_production`.`pick_order_delivery_ref` podr on do.`id` = podr.`delivery_order_id`
                    left join `wms_production`.`pick_order` po on podr.`pick_order_id` = po.`id`
                    LEFT JOIN `wms_production`.`member` mb on po.`picker_id` = mb.id
                where
                    do.created >= '2021-12-01'
                group by
                    do.delivery_sn
            ) pick on do.delivery_sn = pick.delivery_sn
            LEFT JOIN (
                -- 打包人信息
                select
                    delivery_order_id,
                    pack_id,
                    creator_id,
                    `workstation_id`,
                    times,
                    count(1) as boxNum
                from
                    wms_production.delivery_box
                    where created >= '2021-12-01'
                group by
                    delivery_order_id,
                    pack_id,
                    creator_id,
                    `workstation_id`,
                    times
            ) db on db.delivery_order_id = do.id
            and db.times = do.times
            LEFT JOIN wms_production.`member` m on m.id = db.creator_id -- 复核人
            left join wms_production.member mo on do.operator_id = mo.id -- 交接人
            left join wms_production.member mp on db.pack_id = mp.id -- 打包人
            left join wms_production.member md on do.delivery_out_id = md.id -- 签收人
            LEFT JOIN (
                        SELECT
                            dog.`delivery_order_id`,
                            count(distinct dog.`seller_goods_id`) SKU,
                            sum(dog.`goods_number`) 商品数量,
                            sum(dog.`three_num` + dog.`two_num` + dog.`one_num`) 修正商品数量,
                            sum(goods_number * volume) dogvolume,
                            sum(goods_number * weight) dogweight,
                            count(distinct seller_goods_id) kinds_num
                        FROM
                            (
                                SELECT
                                    delivery_order_id,
                                    seller_goods_id,
                                    goods_number,
                                    two_conversion,
                                    three_conversion,
                                    if(
                                        three_conversion > 0,
                                        floor(goods_number / three_conversion),
                                        0
                                    ) three_num,
                                    case
                                        when three_conversion > 0
                                        AND two_conversion > 0 then floor(goods_number % three_conversion / two_conversion)
                                        when three_conversion > 0
                                        AND two_conversion = 0 then floor(goods_number / three_conversion)
                                        when three_conversion = 0
                                        AND two_conversion > 0 then floor(goods_number / two_conversion)
                                        else 0
                                    end two_num,
                                    case
                                        when three_conversion > 0
                                        AND two_conversion > 0 then (goods_number % three_conversion % two_conversion)
                                        when three_conversion > 0
                                        AND two_conversion = 0 then (goods_number % three_conversion)
                                        when three_conversion = 0
                                        AND two_conversion > 0 then (goods_number % two_conversion)
                                        else goods_number
                                    end one_num,
                                    volume,
                                    weight
                                FROM
                                    (
                                        SELECT
                                            dog.`delivery_order_id` delivery_order_id,
                                            dog.`seller_goods_id` seller_goods_id,
                                            dog.`goods_number` goods_number,
                                            ifnull(sg.`two_conversion`, 0) two_conversion,
                                            ifnull(sg.`three_conversion`, 0) three_conversion,
                                            volume,
                                            weight
                                        FROM
                                            `wms_production`.`delivery_order_goods` dog
                                            LEFT JOIN `wms_production`.`seller_goods` sg ON dog.`seller_goods_id` = sg.`id`
                                            where dog.created >= '2021-12-01'
                                    ) dog
                            ) dog
                        GROUP BY
                            dog.`delivery_order_id`
                    ) dog on dog.`delivery_order_id` = do.`id`
            left join wms_production.seller_platform_source sps on do.platform_source_id = sps.id
            left join wms_production.platform_source ps on sps.platform_source_id = ps.id
        WHERE
            w.`name` in (
                'BangsaoThong',
                'Fans-B2B',
                'Fans-B2C',
                'Fans-Demo',
                '广州前置仓',
                'PMD-WH',
                'BKK-WH-LAS物料仓',
                'BKK-WH-Ecommerce',
                'AutoWarehouse',
                'BPL-Return Warehouse',
                'BKK-WH-LAS2电商仓'
            )
            AND s.name not in (
                'CG - Medela(lazada)',
                'CG - P&G',
                'CG - Dole (Shopee)',
                'CG - Nestle',
                'CG - Philips',
                'Welink',
                'MAX SPEED',
                'Pongo',
                'VT SHOP',
                'J SHOP',
                'JSHOP',
                'Auto-TestingPartner',
                'YXS',
                'Intrepid - Darlie',
                'Think of Style'
            )
            AND (s.name REGEXP '^CG -.*$') != 1
            AND do.`created` >= '2023-07-01'
    ) t0
    where 1=1
    and    仓库='BST'
    and date(订单创建时间)>=date_sub(date(now() + interval -1 hour),interval 30 day)
    group by date(订单创建时间),仓库
/* order by date(订单创建时间) desc  */
) out2 on dt.date  = out2.订单创建日期
        -- and out1.仓库 = out2.仓库
left join
(
    -- wms入库部分
select date(已到货时间) 到货日期
     ,仓库
     , count(入库单号) 入库单量
     , date_add(now(), interval -60 minute)       运行时间
from (select  w.name                              as                                       仓库名称
           , vp.physicalwarehouse_name                                                     仓库
           , case an.`status`
                 when 0 then '删除'
                 when 10 then '待审核'
                 when 20 then '审核(到货通知)'
                 when 30 then '到货登记'
                 when 40 then '收货中'
                 when 50 then '收货完成'
                 when 60 then '上架中'
                 when 70 then '上架完成'
                 else an.`status`
        end                                      as                                       订单状态
           , case an.`from_order_type`
                 when 1 then '采购入库'
                 when 2 then '调拨入库'
                 when 3 then '退货入库'
                 when 4 then '其他入库'
                 else an.`from_order_type`
        end                                                                               入库类型
           , case an.`other_warehouse_status`
                 when 0 then '未下发'
                 when 1 then '下发成功'
                 when 2 then '下发失败'
                 when 3 then '取消下发失败'
                 when 4 then '部分收货'
                 when 5 then '全部收货'
                 when 6 then '部分收货'
                 else an.`other_warehouse_status`
        end                                                                               外部仓库状态
           , s.name                                                                       货主名称
           , an.notice_number                                                             入库单号
           , an.kinds_num                                                                 商品种类
           , an.goods_in_num                                                              商品数量
           , date_add(an.created, interval -60 minute)                                    入库单创建时间
           , date_add(an.auditor_time, interval -60 minute)                               入库单审核时间
           , if(date_add(an.reg_time, interval -60 minute) is null and an.start_receiving_time is not null,
                an.start_receiving_time,
                date_add(an.reg_time, interval -60 minute))                               已到货时间
           , an.start_receiving_time                                                      开始收货时间
           , an.complete_time                                                             收货完成时间
           , case
                 when w.`name` = 'AutoWarehouse' then an.complete_time
                 when an.quality_status = 'bad' then coalesce(an.shelf_complete_time, an.complete_time)
                 else an.shelf_complete_time end as                                       上架完成时间
           , w.must_twice_count -- 是否二次清点
           , w.notice_bind_port -- 是否必须绑定入港单

      from wms_production.arrival_notice an -- 入库单表
               left join wms_production.seller s
                         on an.seller_id = s.id
               LEFT JOIN `wms_production`.`warehouse` w ON an.`warehouse_id` = w.`id`
               left join tmpale.dim_th_ffm_virtualphysical vp on w.name = vp.virtualwarehouse_name
      where an.auditor_time >= '2023-07-01'
     ) t0
     where 仓库='BST'
     and date(已到货时间)>=date_sub(date(now() + interval -1 hour),interval 30 day)
     group by date(已到货时间),仓库
     /* order by date(已到货时间) desc */
) in1 on dt.date  = in1.到货日期
        -- and out1.仓库 = in1.仓库

left join (

    -- BST 销退单部分
select
date(t1.收货完成时间) 收货完成日期
,仓库
,count(销退入库单号)    销退单量
from
(
    select case
           dr.status
           when 1000 then '取消退货'
           when 1001 then '已删除此销退单'
           when 1010 then '无需寄回直接退款'
           when 1020 then '等待审核'
           when 1030 then '审核完成'
           when 1040 then '买家已寄回'
           when 1045 then '已到货'
           when 1050 then '收货中'
           when 1060 then '收货完成'
           when 1070 then '上架中'
           when 1080 then '上架完成'
           when 9000 then '取消'
           else dr.status
           end                                                      状态,
       dr.created + interval -1 hour                                创建时间,
       dr.created + interval -1 hour                                审核时间,
       dr.arrival_time                                              销退登记时间,

       complete_time                                                收货完成时间,
       t0.shelf_on_end                                              上架时间,
       case
           dr.shelf_status
           when 1070 then '上架中'
           when 1080 then '上架完成'
           end                                                      上架状态,
       dr.back_sn                                                   销退入库单号,
       dr.delivery_sn                                               原订单号,
       dr.express_sn                                                原运单号,
       case
           dr.order_source_type
           when 1 then '人工录入'
           when 2 then '批量导入'
           when 3 then '接口获取'
           when 4 then '系统生成'
           else dr.order_source_type
           end                                                      订单来源,
       dr.external_order_sn                                         外部单号,
       s.name                                                       货主,
       case
           dr.back_type
           when 'primary' then '普通退货'
           when 'backgoods' then '退货换货'
           when 'allRejected' then '全部拒收'
           when 'package' then '包裹销退'
           when 'crossBorder' then '跨境订单'
           when 'interceptCrossBorder' then '拦截跨境销退'
           else dr.back_type
           end                                                      销退单类型,
       case
           dr.back_status
           when 1 then '待退款'
           when 2 then '已退款'
           when 3 then '不退款'
           else dr.back_status
           end                                                      退款状态,
       dr.bank_id                                                   退款账号,
       dr.back_man                                                  退货人,
       dr.back_man_phone                                            退货人联系方式,
       dr.back_express_name                                         承运商,
       UPPER(dr.back_express_sn)                                    运单号,
       m.real_name                                                  创建人,

       #     audit_time 货主审核时间,
       #     delivery_back_time 货主寄回时间,

       #     dr.modified + interval -1 hour 更新时间,
       dr.back_express_status_remark                                客服备注,
       dr.kinds_num                                                 商品种类,
       dr.goods_num                                                 商品数量,
       w.name                                                    as 仓库名称,
       case
           when w.`name` = 'Fans-B2B' then 'BST'
           when w.`name` = 'Fans-B2C' then 'BST'
           when w.`name` = 'PMD-WH' then 'LAS'
           when w.`name` = 'BKK-WH-LAS物料仓' then 'LAS'
           when w.`name` = 'AutoWarehouse' then 'AGV'
           when w.`name` = 'BPL-Return Warehouse' then 'BPL-Return'
           when w.`name` = 'BangsaoThong' then 'BST'
           when w.`name` = 'Fans-Demo' then 'BST'
           when w.`name` = 'BKK-WH-Ecommerce' then 'LAS'
           when w.`name` = 'BPL3- Bangphli3 Livestream Warehouse' then 'BPL3'
           when w.`name` = 'BPL4-LIVESTREAM-02' then 'BPL3'
           when w.`name` = 'BPL3-LIVESTREAM' then 'BPL3' -- MY
           when w.`name` = 'Puncak Alam' then 'PA'
           when w.`name` = 'PA-FEX' then 'PA'
           when w.`name` = 'PH-Santarosawarehouse'
               then 'Santa rosa warehouse' -- when w.`name`='Flash-toy虚拟仓' then 'cabuyao warehouse'
           when w.`name` = 'Santa Rosa Material Warehouse' then 'cabuyao warehouse' -- ID
           when w.`name` = '印尼直播仓01' then 'Royal Kosambi仓库' -- VN ALIFAN,越之星使用的WMS系统
           when w.`name` = 'GLK仓库' then 'GLK' 
           else w.`name`
           end                                                      仓库
    from (select *
        from wms_production.delivery_rollback_order
        where status > 1020
            and status <> 9000
            and back_type in ('primary', 'backgoods', 'allRejected')) dr -- 销退明细主表
            left join wms_production.seller s on dr.seller_id = s.id
            left join wms_production.member m on dr.creator_id = m.id
            left join wms_production.warehouse w ON dr.warehouse_id = w.id
            left join (
        SELECT dro.back_sn,
            coalesce(max(soo.shelf_on_end), max(ref.in_warehouse_date)) shelf_on_end, -- 上架时间
            max(soo.shelf_on_end),
            max(ref.in_warehouse_date)
        FROM wms_production.delivery_rollback_order dro
                LEFT JOIN wms_production.seller_goods_location_ref ref ON ref.in_warehouse_sn = dro.back_sn
                LEFT JOIN wms_production.shelf_on_order_goods soog ON soog.seller_goods_location_ref_id = ref.id
                LEFT JOIN wms_production.shelf_on_order soo ON soo.id = soog.shelf_on_order_id
        WHERE (dro.shelf_status = 1080)
        group by dro.back_sn
    ) t0 on dr.back_sn = t0.back_sn
    where date(dr.created + interval -1 hour) >= '2023-05-18'
    and w.name != 'BPL-Return Warehouse'
    ) t1
    where 1=1
    and 仓库='BST'
    and date(t1.收货完成时间)>=date_sub(date(now() + interval -1 hour),interval 30 day)
    group by date(t1.收货完成时间),仓库
/* order by date(t1.收货完成时间) desc */
) in2 on dt.date  = in2.收货完成日期
        -- and out1.仓库 = in2.仓库
order by dt.date  desc