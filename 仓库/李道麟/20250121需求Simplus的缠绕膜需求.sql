Simplus
描述
1,取值范围：账单周期内操作的发货单
2.商品件数2件 及以上
3.未使用仓库包材的订单
你看下，这个规则，是否可以作为一个数据信息，每月1号发出

select
delivery_sn,
包材费,
billing_name_zh,
two_up,
container_sn
from
(
    select
        gg.delivery_sn,
        d.settlement_amount as 包材费,
        h.settlement_amount as 手工包材, -- 不需要
        d.billing_name_zh,
        h.billing_name_zh, -- 不需要
        if(gg.goods_num>=2, 1, 0) two_up,
        b.container_sn

    from
    (
        select
            a.delivery_sn
            ,g.name
            ,s.name as seller_name
            ,a.id
            ,a.goods_num
        from wms_production.delivery_order as a
        left join wms_production.seller_goods as g on g.id = a.seller_goods_id
        left join wms_production.seller s on a.seller_id=s.id
        where 1=1
            and s.name = 'Simplus'
            and date(delivery_time) between date_sub(date_sub(date_format(now(),'%y-%m-%d'),interval extract(day from now())-1 day),interval 1 month) and date_sub(date_sub(date_format(now(),'%y-%m-%d'),interval extract(day from now()) day),interval 0 month)
    ) gg
    left join
    (
        select 
            d.from_order_sn
            ,e.billing_name_zh 
            ,sum(settlement_amount/100) as settlement_amount
        from wms_production.billing_detail as d  -- 账单明细表
        left join billing_projects as e on e.id = d.billing_projects_id  
        where e.id  =10
        group by  d.from_order_sn,e.billing_name_zh
    )as d on d.from_order_sn=gg.delivery_sn
    left join  
    (
        select 
            d.`business_sn`
            ,e.billing_name_zh 
            ,sum(settlement_amount/100) as settlement_amount
        from wms_production.billing_detail as d  -- 账单明细表
        left join billing_projects as e on e.id  =d.billing_projects_id  
        where e.id  =17 -- 手工包材
            /* and d.charge_sn ='AR2409015451' */
        group by  d.`business_sn` ,e.billing_name_zh
    )as h on h.`business_sn`=gg.delivery_sn
    left join wms_production.container_order as b on gg.id = b.business_id
) t0
where two_up=1
and 包材费=0