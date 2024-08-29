############################################## 第一部分 ##############################################
select
    统计月份,
    仓库,
    发货商品件量,
    发货单量,
    sku_num,
    日均单量,
    峰值单量,
    单均件数,
    单量净增长比例,
    sku增长比例,
    月日均实物库存,
    周转天数
from
(
    select
        out.统计月份,
        out.仓库,
        out.发货商品件量,
        out.发货单量,
        dog.sku_num,
        out.日均单量,
        out.峰值单量,
        out.单均件数,
        lag(out.发货单量,1) over(order by out.统计月份) 上个月单量,
        round((out.发货单量- lag(out.发货单量,1) over(order by out.统计月份))/lag(out.发货单量,1) over(order by out.统计月份) , 4) 单量净增长比例,
        lag(dog.sku_num,1) over(order by out.统计月份) 上个月sku_num,
        round((dog.sku_num - lag(dog.sku_num,1) over(order by out.统计月份))/lag(dog.sku_num,1) over(order by out.统计月份), 4) sku增长比例,
        sgss.月日均实物库存,
        round(sgss.月日均实物库存 / out.发货商品件量, 4) 周转天数
    from
    (
        select -- 月粒度数据
            left(统计日期, 7) 统计月份,
            仓库,
            sum(发货单量) 发货单量,
            sum(发货商品件量) 发货商品件量,
            round(sum(发货单量)/max(每月天数), 4) 日均单量,
            max(发货单量) 峰值单量,
            round(sum(发货商品件量) / sum(发货单量), 4) 单均件数
        from
        (
            SELECT -- 日期粒度的数据
                date(快递已签收时间) 统计日期,
                仓库,
                count(发货单号) 发货单量,
                sum(商品数量)  发货商品件量,
                day(last_day(快递已签收时间)) 每月天数
            from
                dwm.dwd_th_ffm_outbound_day
            WHERE
                right(类型, 3) = '发货单'
                AND date(快递已签收时间) >= '2023-12-01'
            group by
                1,
                2
            -- order by
            --     1,
            --     2
        ) out
        where 仓库='BST'
        group by left(统计日期, 7),
                仓库
        order by left(统计日期, 7),
                仓库

    ) out 
    left join
    ( -- barcode数量
        select
            left(快递已签收日期,7) 统计月份,
            count(distinct bar_code) sku_num
        from
        (
            select
                    do.delivery_sn,
                    do.快递已签收时间,
                    date(do.快递已签收时间) 快递已签收日期,
                    dog.bar_code
                from
                    (
                        select
                            do.id,
                            delivery_sn,
                            date_add(do.`delivery_time`, interval -60 minute) 快递已签收时间
                        from
                            `wms_production`.`delivery_order` do
                            LEFT JOIN `wms_production`.`seller` s ON do.`seller_id` = s.`id`
                            LEFT JOIN `wms_production`.`warehouse` w ON do.`warehouse_id` = w.`id`
                            where do.`delivery_time` >= convert_tz('2023-12-01', '+07:00', '+08:00')
                                and w.name = 'BangsaoThong'
                    ) do
                    LEFT JOIN 
                    (
                        SELECT
                            delivery_order_id,
                            seller_goods_id sku,
                            goods_number,
                            two_conversion,
                            three_conversion,
                            volume,
                            weight,
                            bar_code
                        FROM
                            (
                                SELECT
                                    dog.`delivery_order_id` delivery_order_id,
                                    dog.`seller_goods_id` seller_goods_id,
                                    dog.`goods_number` goods_number,
                                    ifnull(sg.`two_conversion`, 0) two_conversion,
                                    ifnull(sg.`three_conversion`, 0) three_conversion,
                                    volume,
                                    weight,
                                    sg.bar_code
                                FROM
                                    `wms_production`.`delivery_order_goods` dog
                                    LEFT JOIN `wms_production`.`seller_goods` sg ON dog.`seller_goods_id` = sg.`id`
                                where
                                    dog.created >= '2023-12-01'
                            ) dog
                    ) dog on dog.`delivery_order_id` = do.`id`
        ) dog
        group by left(快递已签收日期,7) 
    ) dog on out.统计月份 = dog.统计月份
    left join
    (
        select
            left(日期, 7) 统计月份,
            仓库,
            round(avg(实物库存), 4) 月日均实物库存
        from
        (
            -- 每日库存快照
            select 
                sgss.date '日期',
                w.name  '仓库',
                sum(sgss.total_inventory+sgss.scrap_inventory) '实物库存',
                sum(sgss.total_inventory)  '正品实物库存',
                sum(sgss.scrap_inventory)  '残品实物库存'
            from wms_production.seller_goods_stock_snapshot sgss
            left join wms_production.seller s on s.id = sgss.seller_id
            left join wms_production.warehouse w on w.id = sgss.warehouse_id
            where sgss.`date` between date('2023-12-01') and curdate()
            and s.name  not in ('Trolmaster', 'Beisi-B2B（倍思）') # 这两个货主是2B的，去掉
            and w.name = 'BangsaoThong'
            group by 1,2
        ) sgss
        group by left(日期, 7),仓库
    )sgss on out.统计月份 = sgss.统计月份
) t0
where t0.统计月份 >='2024-01';


############################################## 第二部分 ##############################################
select
    t1.sellerName,
    t1.bar_code,
    t1.TYPE,
    t0.goods_number_sum30,
    t0.goods_number_avg30,
    t1.inventoryAll 本日库存,
    t1.inventoryPick 拣货区库存,
    t1.inventoryStock 备货区库存,
    t1.inventoryBad 残品,
    round(t1.inventoryAll / t0.goods_number_avg30 , 4) 周转天数,
    round(t1.inventoryPick / t0.goods_number_avg30 , 4) 拣货区天数,
    round(t1.inventoryStock / t0.goods_number_avg30 , 4) 备货区天数
from
(
    # 库存逻辑
    select
        w.name warehouse_name,
        s.name sellerName,
        sg.bar_code,
        CASE    WHEN greatest(sg.LENGTH, sg.width, sg.height)<=250 AND  sg.weight <= 3000 THEN '小件'
                WHEN greatest(sg.LENGTH, sg.width, sg.height)<=500 AND  sg.weight <= 5000 THEN '中件'
                WHEN greatest(sg.LENGTH, sg.width, sg.height)<=1000 AND sg.weight <= 15000 THEN '大件'
                WHEN greatest(sg.LENGTH, sg.width, sg.height)>1000 OR   sg.weight > 15000 THEN '超大件' 
            ELSE '信息不全' END TYPE,
        sum(sglr.inventory)  inventoryAll,
        sum(if(rp.use_attribute='pick', sglr.inventory, 0)) inventoryPick,
        sum(if(rp.use_attribute='stock', sglr.inventory, 0)) inventoryStock,
        sum(if(sglr.quality_status='bad', sglr.inventory, 0 )) inventoryBad
    from
        wms_production.seller_goods_location_ref sglr
        LEFT JOIN `wms_production`.`repository` rp on sglr.`repository_id` = rp.`id`
        LEFT JOIN `wms_production`.`warehouse` w on sglr.`warehouse_id` = w.`id`
        left join wms_production.seller_goods sg on sg.id = sglr.`seller_goods_id`
        left join wms_production.seller s on s.id = sglr.seller_id
    where
        1 = 1
        and w.name in ('BangsaoThong')
        and s.disabled=0
        -- and s.name in ('dahanchao（大汉朝）')
        -- and sg.bar_code = '6923520248230'
    group by w.name,
        s.name,
        sg.bar_code
    having sum(sglr.inventory)>0
) t1
left join
(
    select
        -- do.快递已签收时间,
        -- date(do.快递已签收时间) 快递已签收日期,
        do.sellerName,
        dog.bar_code,
        -- count(do.delivery_sn) deliveryCnt,
        round(sum(dog.goods_number)/30, 4) goods_number_avg30,
        sum(dog.goods_number) goods_number_sum30
    from
        (
            select
                do.id,
                delivery_sn,
                s.name sellerName,
                date_add(do.`delivery_time`, interval -60 minute) 快递已签收时间
            from
                `wms_production`.`delivery_order` do
                LEFT JOIN `wms_production`.`seller` s ON do.`seller_id` = s.`id`
                LEFT JOIN `wms_production`.`warehouse` w ON do.`warehouse_id` = w.`id`
                where do.`delivery_time` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 30 day), '+07:00', '+08:00')
                    and w.name = 'BangsaoThong'
        ) do
        LEFT JOIN 
        (
            SELECT
                delivery_order_id,
                seller_goods_id sku,
                goods_number,
                two_conversion,
                three_conversion,
                volume,
                weight,
                bar_code
            FROM
                (
                    SELECT
                        dog.`delivery_order_id` delivery_order_id,
                        dog.`seller_goods_id` seller_goods_id,
                        dog.`goods_number` goods_number,
                        ifnull(sg.`two_conversion`, 0) two_conversion,
                        ifnull(sg.`three_conversion`, 0) three_conversion,
                        volume,
                        weight,
                        sg.bar_code
                    FROM
                        `wms_production`.`delivery_order_goods` dog
                        LEFT JOIN `wms_production`.`seller_goods` sg ON dog.`seller_goods_id` = sg.`id`
                    where
                        dog.created >= convert_tz(date_sub(date(now() + interval -1 hour),interval 30 day), '+07:00', '+08:00')
                ) dog
        ) dog on dog.`delivery_order_id` = do.`id`
        group by 
      -- date(do.快递已签收时间),
                    dog.bar_code,
                    do.sellerName
) t0
 on t0.sellerName = t1.sellerName
    and t0.bar_code = t1.bar_code