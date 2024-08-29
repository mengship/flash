-- 出库商品维度汇总大小件比例
select
    warehouse_name
    ,delivery_month
    ,round(little_cnt / sum_cnt  ,2) little_rate
    ,round(middle_cnt / sum_cnt ,2) middle_rate
    ,round(big_cnt / sum_cnt ,2) big_rate
    ,round(superbig_cnt / sum_cnt ,2) superbig_rate
    ,round(defaultype / sum_cnt ,2) default_rate
    ,sum_cnt
    ,little_cnt
    ,middle_cnt
    ,big_cnt
    ,superbig_cnt
    ,defaultype
from
(
    select
        warehouse_name
        ,left(delivery_date, 7) delivery_month
        ,count(delivery_order_id) sum_cnt
        ,count(little)  little_cnt
        ,count(middle)   middle_cnt
        ,count(big)   big_cnt
        ,count(superbig)   superbig_cnt
        ,count(defaultype)   defaultype
    from
    (
        select
            dog.delivery_order_id
            ,if(TYPE='小件', dog.delivery_order_id, null) little
            ,if(TYPE='中件', dog.delivery_order_id, null) middle
            ,if(TYPE='大件', dog.delivery_order_id, null) big
            ,if(TYPE='超大件', dog.delivery_order_id, null) superbig
            ,if(TYPE='信息不全', dog.delivery_order_id, null) defaultype
            ,dog.goods_number
            ,do.delivery_date
            ,do.warehouse_name
        from
        (
            SELECT
                date_add(do.`delivery_time`, interval -60 minute) delivery_time
                ,date(date_add(do.`delivery_time`, interval -60 minute)) delivery_date
                ,do.id
                ,case when w.name='AutoWarehouse'   then 'AGV'
                    when w.name='BPL-Return Warehouse'  then 'BPL-Return'
                    when w.name='BPL3-LIVESTREAM'   then 'BPL3'
                    when w.name='BangsaoThong'  then 'BST'
                    when w.name IN ('BKK-WH-LAS2电商仓')    then 'LAS'
                    when w.name ='LCP Warehouse' then 'LCP' end warehouse_name
            FROM
                `wms_production`.`delivery_order` do
                LEFT JOIN wms_production.warehouse w ON do.warehouse_id=w.id
            WHERE
                do.`delivery_time` >= convert_tz('2024-03-01', '+07:00', '+08:00')
        )do
        join
        (
            SELECT
                dog.`delivery_order_id` delivery_order_id,
                dog.`seller_goods_id` seller_goods_id,
                dog.`goods_number` goods_number,
                ifnull(sg.`two_conversion`, 0) two_conversion,
                ifnull(sg.`three_conversion`, 0) three_conversion,
                volume,
                weight,
                CASE WHEN greatest(sg.LENGTH,sg.width,sg.height)<=250 AND weight <= 3000 THEN '小件'
                    WHEN greatest(sg.LENGTH,sg.width,sg.height)<=500 AND weight <= 5000 THEN '中件'
                    WHEN greatest(sg.LENGTH,sg.width,sg.height)<=1000 AND weight <= 15000 THEN '大件'
                    WHEN greatest(sg.LENGTH,sg.width,sg.height)>1000 OR weight > 15000 THEN '超大件' 
                    ELSE '信息不全' END TYPE
            FROM
                `wms_production`.`delivery_order_goods` dog
                LEFT JOIN `wms_production`.`seller_goods` sg ON dog.`seller_goods_id` = sg.`id`
                where dog.created >= '2024-02-01'
        ) dog on do.id = dog.delivery_order_id
        where warehouse_name in ('AGV', 'BPL-Return', 'BST', 'LAS')
    ) do2
    group by warehouse_name
        ,left(delivery_date, 7)
)


union

select
    warehouse_name
    ,delivery_month
    ,round(little_cnt / sum_cnt  ,2) little_rate
    ,round(middle_cnt / sum_cnt ,2) middle_rate
    ,round(big_cnt / sum_cnt ,2) big_rate
    ,round(superbig_cnt / sum_cnt ,2) superbig_rate
    ,round(defaultype / sum_cnt ,2) default_rate
    ,sum_cnt
    ,little_cnt
    ,middle_cnt
    ,big_cnt
    ,superbig_cnt
    ,defaultype
from
(
    select
        warehouse_name
        ,left(delivery_date, 7) delivery_month
        ,count(delivery_order_id) sum_cnt
        ,count(little)  little_cnt
        ,count(middle)   middle_cnt
        ,count(big)   big_cnt
        ,count(superbig)   superbig_cnt
        ,count(defaultype)   defaultype
    from
    (
        select
            dog.delivery_order_id
            ,if(TYPE='小件', dog.delivery_order_id, null) little
            ,if(TYPE='中件', dog.delivery_order_id, null) middle
            ,if(TYPE='大件', dog.delivery_order_id, null) big
            ,if(TYPE='超大件', dog.delivery_order_id, null) superbig
            ,if(TYPE='信息不全', dog.delivery_order_id, null) defaultype
            ,dog.goods_number
            ,do.delivery_date
            ,do.warehouse_name
        from
        (
            SELECT
                date_add(do.`delivery_time`, interval -60 minute) delivery_time
                ,date(date_add(do.`delivery_time`, interval -60 minute)) delivery_date
                ,do.id
                ,case when w.name='AutoWarehouse'   then 'AGV'
                    when w.name='BPL-Return Warehouse'  then 'BPL-Return'
                    when w.name='BPL3-LIVESTREAM'   then 'BPL3'
                    when w.name='BangsaoThong'  then 'BST'
                    when w.name IN ('BKK-WH-LAS2电商仓')    then 'LAS'
                    when w.name ='LCP Warehouse' then 'LCP' end warehouse_name
            FROM
                `erp_wms_prod`.`delivery_order` do
                LEFT JOIN erp_wms_prod.warehouse w ON do.warehouse_id=w.id
            WHERE
                do.`delivery_time` >= convert_tz('2024-03-01', '+07:00', '+08:00')
        )do
        join
        (
            SELECT
                dog.`delivery_order_id` delivery_order_id,
                dog.`seller_goods_id` seller_goods_id,
                dog.`goods_number` goods_number,
                volume,
                weight,
                CASE WHEN greatest(sg.LENGTH,sg.width,sg.height)<=250 AND weight <= 3000 THEN '小件'
                    WHEN greatest(sg.LENGTH,sg.width,sg.height)<=500 AND weight <= 5000 THEN '中件'
                    WHEN greatest(sg.LENGTH,sg.width,sg.height)<=1000 AND weight <= 15000 THEN '大件'
                    WHEN greatest(sg.LENGTH,sg.width,sg.height)>1000 OR weight > 15000 THEN '超大件' 
                    ELSE '信息不全' END TYPE
            FROM
                `erp_wms_prod`.`delivery_order_goods` dog
                LEFT JOIN `erp_wms_prod`.`seller_goods` sg ON dog.`seller_goods_id` = sg.`id`
                where dog.created >= '2024-02-01'
        ) dog on do.id = dog.delivery_order_id
        where warehouse_name in ('BPL3')
    ) do2
    group by warehouse_name
        ,left(delivery_date, 7)
)





 
 -- 单均收入
 -- wms
select
    warehouse_name
    ,left(business_date, 7) business_month
    ,sum(settlement_amount) settlement_amount
    ,count(business_sn) business_cnt
    ,sum(settlement_amount) / count(business_sn) avg_amount
from
(

    select 
        -- a.business_sn     as                         '包材单号',
        a.business_date   as                         business_date,
        -- s.name            as                         '货主',
        -- w.name            as                         '仓库',
        case when w.name='AutoWarehouse' then 'AGV'
            when w.name='BPL-Return Warehouse' then 'BPL-Return'
            when w.name='BPL3-LIVESTREAM' then 'BPL3'
            when w.name='BangsaoThong' then 'BST'
            when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS'
            when w.name='LCP Warehouse' then 'LCP' end warehouse_name,
        a.settlement_amount/100 settlement_amount,
        a.business_sn
    from wms_production.billing_detail a
        join wms_production.billing_projects b on a.billing_projects_id = b.id
        join wms_production.seller s on a.seller_id = s.id
        join wms_production.warehouse w on a.warehouse_id = w.id
    where 1=1
        /* and b.documents = 6 */
        and b.billing_name_zh='操作费'
        and a.business_date >= '2024-03-01'
        /* and d.seller_id = 0 */
        and s.disabled = 0
     
)
where warehouse_name in ('AGV', 'BPL-Return', 'BPL3', 'BST', 'LAS')
group by warehouse_name
,left(business_date, 7)

union
select
    amount.warehouse_name
    ,amount.business_month
    ,amount.amount
    ,cnt.delivery_cnt
    ,amount.amount / cnt.delivery_cnt avg_amount
from
(
    SELECT 
        'BPL3' warehouse_name
        ,left(b.`end_date`, 7)  business_month
        ,sum(a.order_operation_no_tax_amount - a.discount_reduce_no_tax_amount) amount
    from erp_wms_prod.billing_detail a
    left join erp_wms_prod.billing b on b.`id` =a.billing_id
    left join erp_wms_prod.`warehouse` w on b.`warehouse_id` = w.`id` 
    left join erp_wms_prod.`seller` s on b.`seller_id` = s.`id`
    WHERE b.`end_date` > '2024-03-01'
    and w.name = 'BPL3-LIVESTREAM'
    group by left(b.`end_date`, 7)
) amount
left join
(
    select
        warehouse_name
        ,left(delivery_time, 7) delivery_month
        ,count(delivery_sn) delivery_cnt
    from
    dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and warehouse_name='BPL3'
        and date(delivery_time)>='2024-03-01'
    group by warehouse_name
        ,left(delivery_time, 7)
) cnt on amount.business_month = cnt.delivery_month