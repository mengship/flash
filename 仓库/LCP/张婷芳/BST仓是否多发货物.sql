-- BST仓，指定SKU（VN-ZJ-white），发flash快递的包裹，0601~0730，取出DO单号，快递单号，重量，日期
select
dog1.created_date,
dog1.delivery_order_id,
do.delivery_sn,
do.express_sn,
pi.pno,
pi.weight 物品重量,
pi.courier_weight 快递员复秤物品重量,
pi.store_keeper_weight 仓管员复秤物品重量,
pi.exhibition_weight 展示物品重量用的偏多,
pi.store_weight 网点重量
from
(
    select
    date(created) created_date,
    delivery_order_id
    from
    (
        SELECT
            dog.created,
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
            where dog.created >= '2024-06-01'
            and sg.bar_code = 'VN-ZJ-white'
    ) dog
    group by date(created),delivery_order_id
) dog1
left join
(
    SELECT
        *
    FROM
        `wms_production`.`delivery_order` do
    WHERE
        do.`audit_time`>='2024-06-01'
) do on  dog1.delivery_order_id = do.id
join
(
    select
    pno,
    weight,
    courier_weight,
    store_keeper_weight,
    exhibition_weight,
    store_weight
    from
    fle_staging.parcel_info
    where date(created_at)>='2024-06-01'
) pi on do.express_sn = pi.pno
order by dog1.created_date