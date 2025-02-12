-- 客户画像逻辑        
        
        
-- 1 有库存的 sku数量 库存 商品体积 货位面积 货位体积 
select
    a.仓库名称
    ,a.seller_name
    ,left(a.date, 7) datemonth
    ,count(distinct a.bar_code) skunum
    ,sum(a.inventory) inventory
    ,sum(a.volume) goods_volume
    ,sum(b.total_area) location_area
    ,sum(b.total_volume) location_volume
from
(
        select 
            case when w.name='AutoWarehouse' then 'AGV'
                when w.name='BPL-Return Warehouse' then 'BPL-Return'
                when w.name='BPL3-LIVESTREAM' then 'BPL3'
                when w.name='BangsaoThong' then 'BST'
                when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS'
                when w.name='LCP Warehouse' then 'LCP' end 仓库名称
            ,s.name seller_name
            ,a.seller_goods_id 
            ,sg.bar_code
            ,a.date
            ,a.in_days
            ,sg.length/1000*sg.width/1000*sg.height /1000*a.inventory  as volume 
            ,a.inventory
            ,case when a.in_days <=60 then '0-60天'
                when a.in_days between 61 and 120 then '61-120'
                when a.in_days between 121 and 180 then '121-180'
                else '180以上'
                end age
            ,CASE   WHEN greatest(SG.LENGTH, SG.width, SG.height)<=250 AND weight <= 3000 and weight>0 THEN '小件'
                    WHEN greatest(SG.LENGTH, SG.width, SG.height)<=500 AND weight <= 5000 and weight>0 THEN '中件'
                    WHEN greatest(SG.LENGTH, SG.width, SG.height)<=1000 AND weight <= 15000 and weight>0 THEN '大件'
                    WHEN (greatest(SG.LENGTH,SG.width,SG.height)>1000 and weight>0) OR (weight > 15000 and SG.LENGTH>0 and SG.width>0 and SG.height>0 ) THEN '超大件' 
            ELSE '信息不全' END TYPE
        from seller_goods_days_stock_snapshot  as a
        left join seller s on a.seller_id =s.id
        left join seller_goods sg on sg.id =a.seller_goods_id 
        left join wms_production.warehouse w on a.warehouse_id =w.id 
        where a.date >= date('2024-01-01')
) a
left join 
(
    select 
        case when w.name='AutoWarehouse' then 'AGV'
                when w.name='BPL-Return Warehouse' then 'BPL-Return'
                when w.name='BPL3-LIVESTREAM' then 'BPL3'
                when w.name='BangsaoThong' then 'BST'
                when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS'
                when w.name='LCP Warehouse' then 'LCP' end 仓库名称
        ,a.date
        ,s.name
        ,decode(l.location_attribute,  1 ,'小货架',2, '中货架',3 ,'大货架',4, '落地货架',5, '其他',location_attribute)货位属性
        ,sum(l.length / 1000 * l.width / 1000 * ( 100 + r.share_ratio ) / 100) AS total_area
        ,sum(l.length / 1000 * l.width / 1000 *l.height/ 1000 * ( 100 + r.share_ratio ) / 100) AS total_volume
    FROM
    (
        select 
            distinct date 
            ,location_id 
            ,seller_id 
        from 
        seller_goods_location_ref_snapshot a
        where a.date >= date('2024-01-01')
    ) as a
    left join         location l    on a.location_id=l.id        
    left join  wms_production.warehouse w on w.id=l.warehouse_id 
    left join         repository r  on l.repository_id = r.id 
    left join         seller as s   on s.id=a.seller_id 
    where r.use_attribute NOT IN ( 'temporary', 'waitingTemporary' )   
    --   and s.name ='XiShengJi 喜苼记'
    group by case when w.name='AutoWarehouse' then 'AGV'
                when w.name='BPL-Return Warehouse' then 'BPL-Return'
                when w.name='BPL3-LIVESTREAM' then 'BPL3'
                when w.name='BangsaoThong' then 'BST'
                when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS'
                when w.name='LCP Warehouse' then 'LCP' end,a.date,s.name
) as b
on a.date=b.date and a.seller_name=b.name and a.仓库名称=b.仓库名称
  where a.仓库名称 is not null
group by 1,2,3
order by 1,2,3

-- 2 大小件占比
select
    仓库名称
    ,seller_name
    ,left(date, 7) datemonth
    ,TYPE
    ,sum(inventory) inventory
from
(
        select 
            case when w.name='AutoWarehouse' then 'AGV'
                when w.name='BPL-Return Warehouse' then 'BPL-Return'
                when w.name='BPL3-LIVESTREAM' then 'BPL3'
                when w.name='BangsaoThong' then 'BST'
                when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS'
                when w.name='LCP Warehouse' then 'LCP' end 仓库名称
            ,s.name seller_name
            ,a.seller_goods_id 
            ,sg.bar_code
            ,a.date
            ,a.in_days
            ,sg.length/1000*sg.width/1000*sg.height /1000*a.inventory  as volume 
            ,a.inventory
            ,case when a.in_days <=60 then '0-60天'
                when a.in_days between 61 and 120 then '61-120'
                when a.in_days between 121 and 180 then '121-180'
                else '180以上'
                end age
            ,CASE   WHEN greatest(SG.LENGTH, SG.width, SG.height)<=250 AND weight <= 3000 and weight>0 THEN '小件'
                    WHEN greatest(SG.LENGTH, SG.width, SG.height)<=500 AND weight <= 5000 and weight>0 THEN '中件'
                    WHEN greatest(SG.LENGTH, SG.width, SG.height)<=1000 AND weight <= 15000 and weight>0 THEN '大件'
                    WHEN (greatest(SG.LENGTH,SG.width,SG.height)>1000 and weight>0) OR (weight > 15000 and SG.LENGTH>0 and SG.width>0 and SG.height>0 ) THEN '超大件' 
            ELSE '信息不全' END TYPE
        from seller_goods_days_stock_snapshot  as a
        left join seller s on a.seller_id =s.id
        left join seller_goods sg on sg.id =a.seller_goods_id 
        left join wms_production.warehouse w on a.warehouse_id =w.id 
        where a.date >= date('2024-01-01')
) t0
  where 仓库名称 is not null
group by 1,2,3,4
order by 1,2,3,4

-- 业务量 1入库单量
select
    t0.title
    ,t0.complete_month
    ,t0.仓库名称
    ,t0.seller_name
    -- ,t0.TYPE
    ,t0.in_num
    ,round(t0.in_num/date_month.month_day_cnt, 2) avgday
from
(
    select
        '入库单量pcs' title
        ,left(complete_date, 7) complete_month
        ,仓库名称
        ,seller_name
        -- ,TYPE
        ,sum(in_num) in_num
    from
    (
        -- 入库量   
        SELECT
            notice_number
            ,"采购订单" 单据
            ,warehouse_id
            ,s.name seller_name
            ,case when w.name='AutoWarehouse' then 'AGV'
                when w.name='BPL-Return Warehouse' then 'BPL-Return'
                when w.name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
                when w.name='BangsaoThong' then 'BST'
                when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS' # ,'PMD-WH','BKK-WH-Ecommerce','BKK-WH-LAS物料仓'
                when w.name='LCP Warehouse' then 'LCP' end 仓库名称
            ,reg_time - interval 1 hour reg_time
            ,left(reg_time - interval 1 hour,10) reg_date
            ,complete_time
            ,date(complete_time) complete_date
            ,if(w.name='AutoWarehouse', irb.finish_date, shelf_complete_time) shelf_complete_time
            ,ang.in_num
            ,ang.number
            ,sg.bar_code
            ,case an.`from_order_type`
                    when 1 then '采购入库'
                    when 2 then '调拨入库'
                    when 3 then '退货入库'
                    when 4 then '其他入库'
                    else an.`from_order_type`
            end 入库类型
            ,CASE   WHEN greatest(sg.LENGTH, sg.width, sg.height)<=250 AND sg.weight <= 3000 THEN '小件'
                    WHEN greatest(sg.LENGTH, sg.width, sg.height)<=500 AND sg.weight <= 5000 THEN '中件'
                    WHEN greatest(sg.LENGTH, sg.width, sg.height)<=1000 AND sg.weight <= 15000 THEN '大件'
                    WHEN greatest(sg.LENGTH, sg.width, sg.height)>1000 OR sg.weight > 15000 THEN '超大件' 
                    ELSE '信息不全' END TYPE
        FROM wms_production.arrival_notice an
        left join wms_production.arrival_notice_goods ang on an.id = ang.arrival_notice_id
        left join wms_production.seller_goods sg on sg.id =ang.seller_goods_id 
        left join (select receive_external_no,finish_date from was.inb_receive_bill where is_deleted=0 and create_time >= convert_tz(date_sub(date(now() + interval -1 hour),interval 130 day), '+07:00', '+08:00') )irb on an.notice_number = irb.receive_external_no
        left join wms_production.warehouse w ON an.warehouse_id=w.id
        left join wms_production.seller s on s.id = an.seller_id
            WHERE an.reg_time IS NOT NULL 
            AND an.status>='30'
            AND an.complete_time >= '2023-10-01'
            AND an.complete_time <  '2024-10-01'
            and s.name not in ('FFM-TH') -- , 'Flash -Thailand' 也算仓储的客户
    ) t0
    group by 1,2,3,4
) t0
left join 
(
    select 
        left(date, 7) month 
        ,month_day_cnt 
    from 
    tmpale.ods_th_dim_date 
    where date>='2023-10-01'
        and date<'2024-10-01'
    group by 1,2
) date_month on t0.complete_month = date_month.month
  where 仓库名称 is not null
order by 1,2,3,4;

-- 业务量 2销退入库
select
    title
    ,complete_month
    ,仓库名称
    ,selller_name
    ,order_num
    ,round(order_num / date_month.month_day_cnt, 2) avg_order
    ,goods_in_num
    ,round(goods_in_num / date_month.month_day_cnt, 2) avg_pcs
from
(
    select
        '2销退入库' title
        ,left(收货完成日期, 7) complete_month
        ,仓库名称
        ,selller_name
        ,count(distinct 销退入库单号) order_num
        ,sum(goods_in_num_fixup) goods_in_num
    from
    (
        select 
            '销退入库pcs'
            ,case when w.name='AutoWarehouse' then 'AGV'
                when w.name='BPL-Return Warehouse' then 'BPL-Return'
                when w.name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
                when w.name='BangsaoThong' then 'BST'
                when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS' # ,'PMD-WH','BKK-WH-Ecommerce','BKK-WH-LAS物料仓'
                when w.name='LCP Warehouse' then 'LCP' end 仓库名称
            ,case dr.status 
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
                else dr.status end 状态
            ,case dr.shelf_status when 1070 then '上架中' when 1080 then '上架完成' end 上架状态
            ,dr.back_sn 销退入库单号
            ,dr.delivery_sn 原订单号
            ,dr.express_sn 原运单号
            ,case dr.order_source_type 
            when 1 then '人工录入' 
            when 2 then '批量导入' 
            when 3 then '接口获取' 
            when 4 then '系统生成'
            else dr.order_source_type end 订单来源
            ,dr.external_order_sn 外部单号
            ,s.name selller_name
            ,case dr.back_type        
                when 'primary' then '普通退货' 
                when 'backgoods' then '退货换货' 
                when 'allRejected' then '全部拒收'
                when 'package' then '包裹销退'
                when 'crossBorder' then '跨境订单'
                when 'interceptCrossBorder' then '拦截跨境销退'
            else dr.back_type end 销退单类型
            ,dr.complete_time 收货完成时间
            ,date(dr.complete_time) 收货完成日期
            ,dr.goods_in_num
            ,if(dr.back_type='package', 1, goods_in_num) goods_in_num_fixup
        from wms_production.delivery_rollback_order dr
        left join wms_production.seller s on dr.seller_id = s.id
        left join wms_production.member m on dr.creator_id =m.id
        left join wms_production.warehouse w ON dr.warehouse_id=w.id
        where dr.complete_time>='2023-10-01'
            and dr.complete_time<'2024-10-01'
    ) t0
    where 仓库名称 is not null
    group by 1,2,3,4
) t0
left join 
(
    select 
        left(date, 7) month 
        ,month_day_cnt 
    from 
    tmpale.ods_th_dim_date 
    where date>='2023-10-01'
        and date<'2024-10-01'
    group by 1,2
) date_month on t0.complete_month = date_month.month
  where 仓库名称 is not null
order by 1,2,3;

-- 业务量 3 2C出库
select
    titile
    ,complete_month
    ,仓库名称
    ,seller_name
    ,order_num
    ,round(order_num / month_day_cnt, 2) avg_order
    ,goods_num
    ,round(goods_num / month_day_cnt, 2) avg_pcs
from
(
    select
        '3 2C出库' titile
        ,left(delivery_date, 7) complete_month
        ,仓库名称
        ,seller_name
        ,count(distinct delivery_sn) order_num
        ,sum(goods_num) goods_num
    from
    (
        SELECT 
            'B2C' TYPE
            ,do.delivery_sn
            ,goods_num 
            ,warehouse_id
            ,w.name warehouse_name
            ,case when w.name='AutoWarehouse' then 'AGV'
                when w.name='BPL-Return Warehouse' then 'BPL-Return'
                when w.name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
                when w.name='BangsaoThong' then 'BST'
                when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS' # ,'PMD-WH','BKK-WH-Ecommerce','BKK-WH-LAS物料仓'
                when w.name='LCP Warehouse' then 'LCP' end 仓库名称
            ,ps.`name` platform_source
            ,sl.`name` seller_name
            ,do.seller_id
            ,date_add(do.`created`, interval -60 minute) created_time
            ,left(date_add(do.`created`, interval -60 minute), 10) created_date
            ,date_add(do.`audit_time`, interval -60 minute) audit_time
            ,left(date_add(do.`audit_time`, interval -60 minute), 10) audit_date
            ,do.`succ_pick` pick_time
            ,do.`pack_time` pack_time
            ,do.`start_receipt ` handover_time
            ,date_add(do.`delivery_time`, interval -60 minute) delivery_time
            ,date(date_add(do.`delivery_time`, interval -60 minute)) delivery_date
            ,do.express_name
            ,do.operator_id out_operator
            ,case when do.`status` NOT IN ('1000','1010') and do.`platform_status` != 9 and do.prompt NOT in (1,2,3,4) and sl.name not in ('FFM-TH', 'Flash -Thailand') then 1
                else 0
                end as is_visible
            ,do.express_sn
        FROM `wms_production`.`delivery_order` do 
        LEFT JOIN `wms_production`.`seller_platform_source` sps on do.`platform_source_id`=sps.`id`
        LEFT JOIN `wms_production`.`platform_source` ps on sps.`platform_source_id`=ps.`id` 
        LEFT JOIN `wms_production`.`seller` sl on do.`seller_id`=sl.`id`
        LEFT JOIN wms_production.warehouse w ON do.warehouse_id=w.id
        WHERE 1=1
            and do.`delivery_time` >= convert_tz('2023-10-01', '+07:00', '+08:00')
            and sl.name not in ('FFM-TH') -- 剔除物料和资产
    )
    where 仓库名称 is not null
    group by 1,2,3,4
) t0
left join 
(
    select 
        left(date, 7) month 
        ,month_day_cnt 
    from 
    tmpale.ods_th_dim_date 
    where date>='2023-10-01'
        and date<'2024-10-01'
    group by 1,2
) date_month on t0.complete_month = date_month.month
    order by 1,2,3

-- 业务量 4 拦截
select
    '拦截单'
    ,complete_month
    ,warehouse_name
    ,seller_name
    ,sncnt
    ,round(sncnt/month_day_cnt, 2) avg_order
    ,pcscnt
    ,round(pcscnt/month_day_cnt, 2) avg_pcs
from
(
    select
        left(shelf_on_end_date, 7) complete_month
        ,t0.warehouse_name
        ,t0.seller_name
        ,count(distinct intercept_sn) sncnt
        ,sum(goods_num) pcscnt
    from
    (
        select
            'wms拦截单' source
            ,case when w.name='AutoWarehouse' then 'AGV'
                when w.name='BPL-Return Warehouse' then 'BPL-Return'
                when w.name='BPL3-LIVESTREAM' then 'BPL3'
                when w.name='BangsaoThong' then 'BST'
                when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS'
                when w.name='LCP Warehouse' then 'LCP' end warehouse_name
            ,ip.intercept_sn
            ,LEFT(ip.created - INTERVAL 1 HOUR,10) created_date
            ,ip.created - INTERVAL 1 HOUR created_time
            ,LEFT(ip.shelf_on_end_time,10)  shelf_on_end_date
            ,ip.shelf_on_end_time
            ,ip.goods_num
            ,sl.name seller_name
            from wms_production.intercept_place ip
            -- left join wms_production.intercept_place_goods ips on ip.id = ips.intercept_place_id
            LEFT JOIN wms_production.warehouse w ON ip.warehouse_id=w.id
            LEFT JOIN `wms_production`.`seller` sl on ip.`seller_id`=sl.`id`
        where 1=1
            and ip.status <>'1000'
            and ip.shelf_on_end_time >= convert_tz('2023-10-01', '+07:00', '+08:00')
    ) t0
    where warehouse_name is not null
    group by 1,2,3
) t0
left join 
(
    select 
        left(date, 7) month 
        ,month_day_cnt 
    from 
    tmpale.ods_th_dim_date 
    where date>='2023-10-01'
        and date<'2024-10-01'
    group by 1,2
) date_month on t0.complete_month = date_month.month
    order by 1,2,3

-- 业务量 5 2B出库
select
    complete_month
    ,仓库名称
    -- ,TYPE
    ,out_num
    ,round(out_num / month_day_cnt, 2) avg_day
from
(
    select
        '2B出库'
        ,left(delivery_date, 7) complete_month
        ,仓库名称
        ,seller_name
        -- ,TYPE
        ,sum(out_num) out_num
    from
    (
        SELECT 
            'B2B' TYPE0
            ,return_warehouse_sn
            ,total_goods_num
            ,do.warehouse_id
            ,case when w.name='AutoWarehouse' then 'AGV'
                when w.name='BPL-Return Warehouse' then 'BPL-Return'
                when w.name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
                when w.name='BangsaoThong' then 'BST'
                when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS' # ,'PMD-WH','BKK-WH-Ecommerce','BKK-WH-LAS物料仓'
                when w.name='LCP Warehouse' then 'LCP' end 仓库名称
            ,w.name warehouse_name
            ,'B2B'platform_source
            ,sl.name seller_name
            ,''seller_id
            ,date_add(do.`created`, interval -60 minute) created_time
            ,left(date_add(do.`created`, interval -60 minute), 10) created_date
            ,date_add(do.`verify_time`, interval -60 minute) audit_time
            ,left(date_add(do.`verify_time`, interval -60 minute), 10) audit_date
            ,do.picking_end_time
            ,do.pack_time + interval -1 hour pack_time
            ,date_add(do.`out_warehouse_time`, interval -60 minute) handover_time
            ,date_add(do.`out_warehouse_time`, interval -60 minute) delivery_time
            ,date(date_add(do.`out_warehouse_time`, interval -60 minute)) delivery_date
            ,if(locate('DO',do.express_sn)>0 or substring(do.express_sn,1,3)='LBX', 0, 1) is_time
            ,case 
                when locate('DO',do.express_sn)>0 then 'DO快递单号'
                when substring(do.express_sn,1,3)='LBX' then 'LBX快递单号'
                else '正常时效单'
                end as no_istime_type
            ,null is_tiktok
            ,null out_operator
            ,do.express_sn
            -- ,CASE   WHEN greatest(sg.LENGTH, sg.width, sg.height)<=250 AND sg.weight <= 3000 THEN '小件'
            --         WHEN greatest(sg.LENGTH, sg.width, sg.height)<=500 AND sg.weight <= 5000 THEN '中件'
            --         WHEN greatest(sg.LENGTH, sg.width, sg.height)<=1000 AND sg.weight <= 15000 THEN '大件'
            --         WHEN greatest(sg.LENGTH, sg.width, sg.height)>1000 OR sg.weight > 15000 THEN '超大件' 
            --         ELSE '信息不全' END TYPE
            -- ,rwg.out_num
            ,do.total_goods_num out_num
        from  wms_production.return_warehouse do 
        -- left join wms_production.return_warehouse_goods rwg on do.id = rwg.return_warehouse_id
        -- left join wms_production.seller_goods sg on sg.id =do.seller_goods_id 
        LEFT JOIN wms_production.warehouse w ON do.warehouse_id=w.id
        LEFT JOIN `wms_production`.`seller` sl on do.`seller_id`=sl.`id`
        WHERE 1=1
            and do.`out_warehouse_time` >= convert_tz('2023-10-01', '+07:00', '+08:00')
            and sl.name not in ('FFM-TH') -- 剔除物料和资产
    ) t0
    where 仓库名称 is not null
    group by 1,2,3,4
) t0
left join 
(
    select 
        left(date, 7) month 
        ,month_day_cnt 
    from 
    tmpale.ods_th_dim_date 
    where date>='2023-10-01'
        and date<'2024-10-01'
    group by 1,2
) date_month on t0.complete_month = date_month.month
order by 1,2,3,4;

-- 收入
select
    t0.月份
    ,t0.仓库名称
    ,t0.seller_name
    ,sum(if(t0.收入类型2='仓储费', t0.amount, 0)) 仓储费
    ,round(sum(if(t0.收入类型2='仓储费', t0.amount, 0)) / date_month.month_day_cnt, 2) avg_仓储费day
    ,round(sum(if(t0.收入类型2='仓储费', t0.amount, 0)) / stock.inventory, 2) avg_stockpcs
    ,round(sum(if(t0.收入类型2='仓储费', t0.amount, 0)) / stock.inventory / date_month.month_day_cnt , 2) avg_stockpcsday
    ,round(sum(if(t0.收入类型2='仓储费', t0.amount, 0)) / stock.location_area, 2) avg_stockarea
    ,round(sum(if(t0.收入类型2='仓储费', t0.amount, 0)) / stock.location_area / date_month.month_day_cnt, 2) avg_stockareaday
    ,round(sum(if(t0.收入类型2='仓储费', t0.amount, 0)) / stock.location_volume, 2) avg_stockvolume
    ,round(sum(if(t0.收入类型2='仓储费', t0.amount, 0)) / stock.location_volume / date_month.month_day_cnt, 2) avg_stockvolumeday
    ,sum(if(t0.收入类型2='入库费', t0.amount, 0)) 入库费
    ,round(sum(if(t0.收入类型2='入库费', t0.amount, 0)) / date_month.month_day_cnt, 2) 入库费day
    ,round(sum(if(t0.收入类型2='入库费', t0.amount, 0)) / instock.in_num, 2) avg_instockpcs
    ,round(sum(if(t0.收入类型2='入库费', t0.amount, 0)) / instock.in_num / date_month.month_day_cnt, 2) avg_instockpcsday
    ,sum(if(t0.收入类型2='卸货费', t0.amount, 0)) 卸货费
    ,round(sum(if(t0.收入类型2='卸货费', t0.amount, 0)) / date_month.month_day_cnt, 2) 卸货费day
    ,round(sum(if(t0.收入类型2='卸货费', t0.amount, 0)) / instock.in_num, 2) avg_instockpcs
    ,round(sum(if(t0.收入类型2='卸货费', t0.amount, 0)) / instock.in_num / date_month.month_day_cnt, 2) avg_instockpcsday
    ,sum(if(t0.收入类型2='销退入库费',t0.amount, 0)) 销退入库费
    ,round(sum(if(t0.收入类型2='销退入库费',t0.amount, 0)) / date_month.month_day_cnt, 2) 销退入库费day
    ,round(sum(if(t0.收入类型2='销退入库费',t0.amount, 0)) / rollback.order_num, 2) avg_rollbackorder
    ,round(sum(if(t0.收入类型2='销退入库费',t0.amount, 0)) / rollback.order_num / date_month.month_day_cnt, 2) avg_rollbackorderday
    ,round(sum(if(t0.收入类型2='销退入库费',t0.amount, 0)) / rollback.goods_in_num, 2) avg_rollbackpcs
    ,round(sum(if(t0.收入类型2='销退入库费',t0.amount, 0)) / rollback.goods_in_num / date_month.month_day_cnt, 2) avg_rollbackpcsday
    ,sum(if(t0.收入类型2='操作费', t0.amount, 0)) 操作费
    ,round(sum(if(t0.收入类型2='操作费', t0.amount, 0)) / date_month.month_day_cnt, 2) 操作费day
    ,round(sum(if(t0.收入类型2='操作费', t0.amount, 0)) / out2C.order_num, 2) avg_out2Corder
    ,round(sum(if(t0.收入类型2='操作费', t0.amount, 0)) / out2C.order_num / date_month.month_day_cnt, 2) avg_out2Corderday
    ,round(sum(if(t0.收入类型2='操作费', t0.amount, 0)) / out2C.goods_num, 2) avg_out2Cpcs
    ,round(sum(if(t0.收入类型2='操作费', t0.amount, 0)) / out2C.goods_num / date_month.month_day_cnt, 2) avg_out2Cpcsday
    ,sum(if(t0.收入类型2='拦截费', t0.amount, 0)) 拦截费
    ,round(sum(if(t0.收入类型2='拦截费', t0.amount, 0)) / date_month.month_day_cnt, 2) 拦截费day
    ,round(sum(if(t0.收入类型2='拦截费', t0.amount, 0)) / intercept.sncnt, 2) avg_ointerceptorder
    ,round(sum(if(t0.收入类型2='拦截费', t0.amount, 0)) / intercept.sncnt / date_month.month_day_cnt, 2) avg_ointerceptorderday
    ,round(sum(if(t0.收入类型2='拦截费', t0.amount, 0)) / intercept.pcscnt, 2) avg_ointerceptpcs
    ,round(sum(if(t0.收入类型2='拦截费', t0.amount, 0)) / intercept.pcscnt / date_month.month_day_cnt, 2) avg_ointerceptpcsday
    ,sum(if(t0.收入类型2='出库费', t0.amount, 0)) 出库费
    ,round(sum(if(t0.收入类型2='出库费', t0.amount, 0)) / date_month.month_day_cnt, 2) 出库费day
    ,round(sum(if(t0.收入类型2='出库费', t0.amount, 0)) / out2B.out_num, 2) avg_out2Bpcs
    ,round(sum(if(t0.收入类型2='出库费', t0.amount, 0)) / out2B.out_num / date_month.month_day_cnt, 2) avg_out2Bpcsday
    ,sum(if(t0.收入类型2='包材费', t0.amount, 0)) 包材费
    ,round(sum(if(t0.收入类型2='包材费', t0.amount, 0)) / date_month.month_day_cnt, 2) 包材费day
    ,round(sum(if(t0.收入类型2='包材费', t0.amount, 0)) / out2C.order_num, 2) avg_materialorder
    ,round(sum(if(t0.收入类型2='包材费', t0.amount, 0)) / out2C.order_num / date_month.month_day_cnt, 2) avg_materialorderday
    ,round(sum(if(t0.收入类型2='包材费', t0.amount, 0)) / out2C.goods_num, 2) avg_materialpcs
    ,round(sum(if(t0.收入类型2='包材费', t0.amount, 0)) / out2C.goods_num / date_month.month_day_cnt, 2) avg_materialpcsday
    ,sum(if(t0.收入类型2='包材费（手动）', t0.amount, 0)) 包材费手动
    ,round(sum(if(t0.收入类型2='包材费（手动）', t0.amount, 0)) / date_month.month_day_cnt, 2) 包材费手动day
    ,round(sum(if(t0.收入类型2='包材费（手动）', t0.amount, 0)) / out2C.order_num, 2) avg_materialhandorder
    ,round(sum(if(t0.收入类型2='包材费（手动）', t0.amount, 0)) / out2C.order_num / date_month.month_day_cnt, 2) avg_materialhandorderday
    ,round(sum(if(t0.收入类型2='包材费（手动）', t0.amount, 0)) / out2C.goods_num, 2) avg_materialhandpcs
    ,round(sum(if(t0.收入类型2='包材费（手动）', t0.amount, 0)) / out2C.goods_num / date_month.month_day_cnt, 2) avg_materialhandpcsday
    ,sum(if(t0.收入类型2='增值服务（全部）', t0.amount, 0)) 增值服务全部
    ,round(sum(if(t0.收入类型2='增值服务（全部）', t0.amount, 0)) / date_month.month_day_cnt, 2) 增值服务全部day
    ,sum(if(t0.收入类型2='条码打印费用', t0.amount, 0)) 条码打印费用
    ,round(sum(if(t0.收入类型2='条码打印费用', t0.amount, 0)) / date_month.month_day_cnt ,2) 条码打印费用day
    ,sum(if(t0.收入类型2='贵品保管费', t0.amount, 0)) 贵品保管费
    ,round(sum(if(t0.收入类型2='贵品保管费', t0.amount, 0)) / date_month.month_day_cnt, 2) 贵品保管费day
    ,sum(if(t0.收入类型2='服务申请单', t0.amount, 0)) 服务申请单
    ,round(sum(if(t0.收入类型2='服务申请单', t0.amount, 0)) / date_month.month_day_cnt, 2) 服务申请单day
    ,sum(if(t0.收入类型2='盘点费', t0.amount, 0)) 盘点费
    ,round(sum(if(t0.收入类型2='盘点费', t0.amount, 0)) / date_month.month_day_cnt, 2) 盘点费day
    ,sum(if(t0.收入类型2='包装加工费', t0.amount, 0)) 包装加工费
    ,round(sum(if(t0.收入类型2='包装加工费', t0.amount, 0)) / date_month.month_day_cnt, 2) 包装加工费day
    ,sum(if(t0.收入类型2='粘贴条码、标签费用', t0.amount, 0)) 粘贴条码标签费用
    ,round(sum(if(t0.收入类型2='粘贴条码、标签费用', t0.amount, 0)) / date_month.month_day_cnt, 2) 粘贴条码标签费用day
    ,sum(if(t0.收入类型2='第三方发货费', t0.amount, 0)) 第三方发货费
    ,round(sum(if(t0.收入类型2='第三方发货费', t0.amount, 0)) / date_month.month_day_cnt, 2) 第三方发货费day
    ,sum(if(t0.收入类型2='装货费', t0.amount, 0)) 装货费
    ,round(sum(if(t0.收入类型2='装货费', t0.amount, 0)) / date_month.month_day_cnt, 2) 装货费day
from
(
    select 
        blp.billing_name_zh 收入类型,
        CASE WHEN LEFT(blp.billing_name_zh,3) IN ('仓储费','入库费','出库费','卸货费') THEN LEFT(blp.billing_name_zh,3)
            ELSE blp.billing_name_zh END 收入类型2,
        left(bld.business_date,7) 月份,
        -- week(left(bld.business_date,10)+ interval 1 day) 周,
        case when  w.name='AutoWarehouse' then 'AGV'
            when w.name='BPL-Return Warehouse' then 'BPL-Return'
            when w.name='BPL3-LIVESTREAM' then 'BPL3'
            when w.name='BangsaoThong' then 'BST'
            when w.name IN ('BKK-WH-LAS2电商仓') then 'LAS'
            when w.name='LCP Warehouse' then 'LCP' end 仓库名称,
        sl.name seller_name,
        sum(bld.settlement_amount)/100 amount, -- 结算金额
        sum(bld.discount_amount)/100 discount_amount
    from wms_production.billing_detail bld
    left join wms_production.billing_projects blp on bld.billing_projects_id= blp.id
    left join wms_production.warehouse w on bld.warehouse_id=w.id
    LEFT JOIN `wms_production`.`seller` sl on bld.`seller_id`=sl.`id`
    where 1=1
        -- and bl.type='1'
        -- and billing_name_zh='操作费'
        and left(bld.business_date,10) >='2023-10-01'
        and LEFT(blp.billing_name_zh,2) <> '快递'
        and bld.settlement_amount>0
    group by 1,2,3,4,5 
    having 仓库名称 is not null
) t0
left join -- stock
(
    -- 1 有库存的 sku数量 库存 商品体积 货位面积 货位体积 
    select
        a.仓库名称
        ,a.seller_name
        ,left(a.date, 7) datemonth
        ,count(distinct a.bar_code) skunum
        ,sum(a.inventory) inventory
        ,sum(a.volume) goods_volume
        ,sum(b.total_area) location_area
        ,sum(b.total_volume) location_volume
    from
    (
            select 
                case when w.name='AutoWarehouse' then 'AGV'
                    when w.name='BPL-Return Warehouse' then 'BPL-Return'
                    when w.name='BPL3-LIVESTREAM' then 'BPL3'
                    when w.name='BangsaoThong' then 'BST'
                    when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS'
                    when w.name='LCP Warehouse' then 'LCP' end 仓库名称
                ,s.name seller_name
                ,a.seller_goods_id 
                ,sg.bar_code
                ,a.date
                ,a.in_days
                ,sg.length/1000*sg.width/1000*sg.height /1000*a.inventory  as volume 
                ,a.inventory
                ,case when a.in_days <=60 then '0-60天'
                    when a.in_days between 61 and 120 then '61-120'
                    when a.in_days between 121 and 180 then '121-180'
                    else '180以上'
                    end age
                ,CASE   WHEN greatest(SG.LENGTH, SG.width, SG.height)<=250 AND weight <= 3000 and weight>0 THEN '小件'
                        WHEN greatest(SG.LENGTH, SG.width, SG.height)<=500 AND weight <= 5000 and weight>0 THEN '中件'
                        WHEN greatest(SG.LENGTH, SG.width, SG.height)<=1000 AND weight <= 15000 and weight>0 THEN '大件'
                        WHEN (greatest(SG.LENGTH,SG.width,SG.height)>1000 and weight>0) OR (weight > 15000 and SG.LENGTH>0 and SG.width>0 and SG.height>0 ) THEN '超大件' 
                ELSE '信息不全' END TYPE
            from seller_goods_days_stock_snapshot  as a
            left join seller s on a.seller_id =s.id
            left join seller_goods sg on sg.id =a.seller_goods_id 
            left join wms_production.warehouse w on a.warehouse_id =w.id 
            where a.date >= date('2024-01-01')
    ) a
    left join 
    (
        select 
            case when w.name='AutoWarehouse' then 'AGV'
                    when w.name='BPL-Return Warehouse' then 'BPL-Return'
                    when w.name='BPL3-LIVESTREAM' then 'BPL3'
                    when w.name='BangsaoThong' then 'BST'
                    when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS'
                    when w.name='LCP Warehouse' then 'LCP' end 仓库名称
            ,a.date
            ,s.name
            ,decode(l.location_attribute,  1 ,'小货架',2, '中货架',3 ,'大货架',4, '落地货架',5, '其他',location_attribute)货位属性
            ,sum(l.length / 1000 * l.width / 1000 * ( 100 + r.share_ratio ) / 100) AS total_area
            ,sum(l.length / 1000 * l.width / 1000 *l.height/ 1000 * ( 100 + r.share_ratio ) / 100) AS total_volume
        FROM
        (
            select 
                distinct date 
                ,location_id 
                ,seller_id 
            from 
            seller_goods_location_ref_snapshot a
            where a.date >= date('2024-01-01')
        ) as a
        left join         location l    on a.location_id=l.id        
        left join  wms_production.warehouse w on w.id=l.warehouse_id 
        left join         repository r  on l.repository_id = r.id 
        left join         seller as s   on s.id=a.seller_id 
        where r.use_attribute NOT IN ( 'temporary', 'waitingTemporary' )   
        --   and s.name ='XiShengJi 喜苼记'
        group by case when w.name='AutoWarehouse' then 'AGV'
                    when w.name='BPL-Return Warehouse' then 'BPL-Return'
                    when w.name='BPL3-LIVESTREAM' then 'BPL3'
                    when w.name='BangsaoThong' then 'BST'
                    when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS'
                    when w.name='LCP Warehouse' then 'LCP' end,a.date,s.name
    ) as b
    on a.date=b.date and a.seller_name=b.name and a.仓库名称=b.仓库名称
    where a.仓库名称 is not null
    group by 1,2,3
) stock on t0.月份 = stock.datemonth and t0.仓库名称 = stock.仓库名称 and t0.seller_name = stock.seller_name
left join -- 业务量 1入库单量
(
    -- 业务量 1入库单量
    select
        t0.title
        ,t0.complete_month
        ,t0.仓库名称
        ,t0.seller_name
        -- ,t0.TYPE
        ,t0.in_num
    from
    (
        select
            '入库单量pcs' title
            ,left(complete_date, 7) complete_month
            ,仓库名称
            ,seller_name
            -- ,TYPE
            ,sum(in_num) in_num
        from
        (
            -- 入库量   
            SELECT
                notice_number
                ,"采购订单" 单据
                ,warehouse_id
                ,s.name seller_name
                ,case when w.name='AutoWarehouse' then 'AGV'
                    when w.name='BPL-Return Warehouse' then 'BPL-Return'
                    when w.name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
                    when w.name='BangsaoThong' then 'BST'
                    when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS' # ,'PMD-WH','BKK-WH-Ecommerce','BKK-WH-LAS物料仓'
                    when w.name='LCP Warehouse' then 'LCP' end 仓库名称
                ,reg_time - interval 1 hour reg_time
                ,left(reg_time - interval 1 hour,10) reg_date
                ,complete_time
                ,date(complete_time) complete_date
                ,if(w.name='AutoWarehouse', irb.finish_date, shelf_complete_time) shelf_complete_time
                ,ang.in_num
                ,ang.number
                ,sg.bar_code
                ,case an.`from_order_type`
                        when 1 then '采购入库'
                        when 2 then '调拨入库'
                        when 3 then '退货入库'
                        when 4 then '其他入库'
                        else an.`from_order_type`
                end 入库类型
                ,CASE   WHEN greatest(sg.LENGTH, sg.width, sg.height)<=250 AND sg.weight <= 3000 THEN '小件'
                        WHEN greatest(sg.LENGTH, sg.width, sg.height)<=500 AND sg.weight <= 5000 THEN '中件'
                        WHEN greatest(sg.LENGTH, sg.width, sg.height)<=1000 AND sg.weight <= 15000 THEN '大件'
                        WHEN greatest(sg.LENGTH, sg.width, sg.height)>1000 OR sg.weight > 15000 THEN '超大件' 
                        ELSE '信息不全' END TYPE
            FROM wms_production.arrival_notice an
            left join wms_production.arrival_notice_goods ang on an.id = ang.arrival_notice_id
            left join wms_production.seller_goods sg on sg.id =ang.seller_goods_id 
            left join (select receive_external_no,finish_date from was.inb_receive_bill where is_deleted=0 and create_time >= convert_tz(date_sub(date(now() + interval -1 hour),interval 130 day), '+07:00', '+08:00') )irb on an.notice_number = irb.receive_external_no
            left join wms_production.warehouse w ON an.warehouse_id=w.id
            left join wms_production.seller s on s.id = an.seller_id
                WHERE an.reg_time IS NOT NULL 
                AND an.status>='30'
                AND an.complete_time >= '2023-10-01'
                AND an.complete_time <  '2024-10-01'
                and s.name not in ('FFM-TH') -- , 'Flash -Thailand' 也算仓储的客户
        ) t0
        group by 1,2,3,4
    ) t0
    where 仓库名称 is not null
)instock on t0.月份 = instock.complete_month and t0.仓库名称 = instock.仓库名称 and t0.seller_name = instock.seller_name
left join -- 业务量 2 销退
(
    select
        '2销退入库' title
        ,left(收货完成日期, 7) complete_month
        ,仓库名称
        ,seller_name
        ,count(distinct 销退入库单号) order_num
        ,sum(goods_in_num_fixup) goods_in_num
    from
    (
        select 
            '销退入库pcs'
            ,case when w.name='AutoWarehouse' then 'AGV'
                when w.name='BPL-Return Warehouse' then 'BPL-Return'
                when w.name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
                when w.name='BangsaoThong' then 'BST'
                when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS' # ,'PMD-WH','BKK-WH-Ecommerce','BKK-WH-LAS物料仓'
                when w.name='LCP Warehouse' then 'LCP' end 仓库名称
            ,case dr.status 
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
                else dr.status end 状态
            ,case dr.shelf_status when 1070 then '上架中' when 1080 then '上架完成' end 上架状态
            ,dr.back_sn 销退入库单号
            ,dr.delivery_sn 原订单号
            ,dr.express_sn 原运单号
            ,case dr.order_source_type 
            when 1 then '人工录入' 
            when 2 then '批量导入' 
            when 3 then '接口获取' 
            when 4 then '系统生成'
            else dr.order_source_type end 订单来源
            ,dr.external_order_sn 外部单号
            ,s.name seller_name
            ,case dr.back_type        
                when 'primary' then '普通退货' 
                when 'backgoods' then '退货换货' 
                when 'allRejected' then '全部拒收'
                when 'package' then '包裹销退'
                when 'crossBorder' then '跨境订单'
                when 'interceptCrossBorder' then '拦截跨境销退'
            else dr.back_type end 销退单类型
            ,dr.complete_time 收货完成时间
            ,date(dr.complete_time) 收货完成日期
            ,dr.goods_in_num
            ,if(dr.back_type='package', 1, goods_in_num) goods_in_num_fixup
        from wms_production.delivery_rollback_order dr
        left join wms_production.seller s on dr.seller_id = s.id
        left join wms_production.member m on dr.creator_id =m.id
        left join wms_production.warehouse w ON dr.warehouse_id=w.id
        where dr.complete_time>='2023-10-01'
            and dr.complete_time<'2024-10-01'
    ) t0
    where 仓库名称 is not null
    group by 1,2,3,4
) rollback on t0.月份 = rollback.complete_month and t0.仓库名称 = rollback.仓库名称 and t0.seller_name = rollback.seller_name
left join -- 业务量 3 2C
(
    select
        '3 2C出库' titile
        ,left(delivery_date, 7) complete_month
        ,仓库名称
        ,seller_name
        ,count(distinct delivery_sn) order_num
        ,sum(goods_num) goods_num
    from
    (
        SELECT 
            'B2C' TYPE
            ,do.delivery_sn
            ,goods_num 
            ,warehouse_id
            ,w.name warehouse_name
            ,case when w.name='AutoWarehouse' then 'AGV'
                when w.name='BPL-Return Warehouse' then 'BPL-Return'
                when w.name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
                when w.name='BangsaoThong' then 'BST'
                when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS' # ,'PMD-WH','BKK-WH-Ecommerce','BKK-WH-LAS物料仓'
                when w.name='LCP Warehouse' then 'LCP' end 仓库名称
            ,ps.`name` platform_source
            ,sl.`name` seller_name
            ,do.seller_id
            ,date_add(do.`created`, interval -60 minute) created_time
            ,left(date_add(do.`created`, interval -60 minute), 10) created_date
            ,date_add(do.`audit_time`, interval -60 minute) audit_time
            ,left(date_add(do.`audit_time`, interval -60 minute), 10) audit_date
            ,do.`succ_pick` pick_time
            ,do.`pack_time` pack_time
            ,do.`start_receipt ` handover_time
            ,date_add(do.`delivery_time`, interval -60 minute) delivery_time
            ,date(date_add(do.`delivery_time`, interval -60 minute)) delivery_date
            ,do.express_name
            ,do.operator_id out_operator
            ,case when do.`status` NOT IN ('1000','1010') and do.`platform_status` != 9 and do.prompt NOT in (1,2,3,4) and sl.name not in ('FFM-TH', 'Flash -Thailand') then 1
                else 0
                end as is_visible
            ,do.express_sn
        FROM `wms_production`.`delivery_order` do 
        LEFT JOIN `wms_production`.`seller_platform_source` sps on do.`platform_source_id`=sps.`id`
        LEFT JOIN `wms_production`.`platform_source` ps on sps.`platform_source_id`=ps.`id` 
        LEFT JOIN `wms_production`.`seller` sl on do.`seller_id`=sl.`id`
        LEFT JOIN wms_production.warehouse w ON do.warehouse_id=w.id
        WHERE 1=1
            and do.`delivery_time` >= convert_tz('2023-10-01', '+07:00', '+08:00')
            and sl.name not in ('FFM-TH') -- 剔除物料和资产
    )
    where 仓库名称 is not null
    group by 1,2,3,4
) out2C on t0.月份 = out2C.complete_month and t0.仓库名称 = out2C.仓库名称 and t0.seller_name = out2C.seller_name
left join -- 业务量 拦截
(
    select
        left(shelf_on_end_date, 7) complete_month
        ,t0.warehouse_name 仓库名称
        ,t0.seller_name
        ,count(distinct intercept_sn) sncnt
        ,sum(goods_num) pcscnt
    from
    (
        select
            'wms拦截单' source
            ,case when w.name='AutoWarehouse' then 'AGV'
                when w.name='BPL-Return Warehouse' then 'BPL-Return'
                when w.name='BPL3-LIVESTREAM' then 'BPL3'
                when w.name='BangsaoThong' then 'BST'
                when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS'
                when w.name='LCP Warehouse' then 'LCP' end warehouse_name
            ,ip.intercept_sn
            ,LEFT(ip.created - INTERVAL 1 HOUR,10) created_date
            ,ip.created - INTERVAL 1 HOUR created_time
            ,LEFT(ip.shelf_on_end_time,10)  shelf_on_end_date
            ,ip.shelf_on_end_time
            ,ip.goods_num
            ,sl.name seller_name
            from wms_production.intercept_place ip
            -- left join wms_production.intercept_place_goods ips on ip.id = ips.intercept_place_id
            LEFT JOIN wms_production.warehouse w ON ip.warehouse_id=w.id
            LEFT JOIN `wms_production`.`seller` sl on ip.`seller_id`=sl.`id`
        where 1=1
            and ip.status <>'1000'
            and ip.shelf_on_end_time >= convert_tz('2023-10-01', '+07:00', '+08:00')
    ) t0
    where warehouse_name is not null
    group by 1,2,3
) intercept on t0.月份 = intercept.complete_month and t0.仓库名称 = intercept.仓库名称 and t0.seller_name = intercept.seller_name
left join -- 业务量 2B
(
    select
        '入库单量pcs'
        ,left(delivery_date, 7) complete_month
        ,仓库名称
        ,seller_name
        -- ,TYPE
        ,sum(out_num) out_num
    from
    (
        SELECT 
            'B2B' TYPE0
            ,return_warehouse_sn
            ,total_goods_num
            ,do.warehouse_id
            ,case when w.name='AutoWarehouse' then 'AGV'
                when w.name='BPL-Return Warehouse' then 'BPL-Return'
                when w.name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
                when w.name='BangsaoThong' then 'BST'
                when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS' # ,'PMD-WH','BKK-WH-Ecommerce','BKK-WH-LAS物料仓'
                when w.name='LCP Warehouse' then 'LCP' end 仓库名称
            ,w.name warehouse_name
            ,'B2B'platform_source
            ,sl.name seller_name
            ,''seller_id
            ,date_add(do.`created`, interval -60 minute) created_time
            ,left(date_add(do.`created`, interval -60 minute), 10) created_date
            ,date_add(do.`verify_time`, interval -60 minute) audit_time
            ,left(date_add(do.`verify_time`, interval -60 minute), 10) audit_date
            ,do.picking_end_time
            ,do.pack_time + interval -1 hour pack_time
            ,date_add(do.`out_warehouse_time`, interval -60 minute) handover_time
            ,date_add(do.`out_warehouse_time`, interval -60 minute) delivery_time
            ,date(date_add(do.`out_warehouse_time`, interval -60 minute)) delivery_date
            ,if(locate('DO',do.express_sn)>0 or substring(do.express_sn,1,3)='LBX', 0, 1) is_time
            ,case 
                when locate('DO',do.express_sn)>0 then 'DO快递单号'
                when substring(do.express_sn,1,3)='LBX' then 'LBX快递单号'
                else '正常时效单'
                end as no_istime_type
            ,null is_tiktok
            ,null out_operator
            ,do.express_sn
            ,CASE   WHEN greatest(sg.LENGTH, sg.width, sg.height)<=250 AND sg.weight <= 3000 THEN '小件'
                    WHEN greatest(sg.LENGTH, sg.width, sg.height)<=500 AND sg.weight <= 5000 THEN '中件'
                    WHEN greatest(sg.LENGTH, sg.width, sg.height)<=1000 AND sg.weight <= 15000 THEN '大件'
                    WHEN greatest(sg.LENGTH, sg.width, sg.height)>1000 OR sg.weight > 15000 THEN '超大件' 
                    ELSE '信息不全' END TYPE
            ,rwg.out_num
        from  wms_production.return_warehouse do 
        left join wms_production.return_warehouse_goods rwg on do.id = rwg.return_warehouse_id
        left join wms_production.seller_goods sg on sg.id =rwg.seller_goods_id 
        LEFT JOIN wms_production.warehouse w ON do.warehouse_id=w.id
        LEFT JOIN `wms_production`.`seller` sl on do.`seller_id`=sl.`id`
        WHERE 1=1
            and do.`out_warehouse_time` >= convert_tz('2023-10-01', '+07:00', '+08:00')
            and sl.name not in ('FFM-TH') -- 剔除物料和资产
    ) t0
    where 仓库名称 is not null
    group by 1,2,3,4
) out2B on t0.月份 = out2B.complete_month and t0.仓库名称 = out2B.仓库名称 and t0.seller_name = out2B.seller_name
left join 
(
    select 
        left(date, 7) month 
        ,max(if(left(date, 7)< left(CURRENT_DATE, 7), cast(month_day_cnt as int), cast(day_of_month as int))) month_day_cnt
    from 
    tmpale.ods_th_dim_date 
    where date>='2023-10-01'
        and date < CURRENT_DATE
    group by 1
) date_month on t0.月份 = date_month.month
where t0.月份>='2024-01'
group by 1,2,3