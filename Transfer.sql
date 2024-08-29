select 
    billing_name_zh 收入类型,
    CASE WHEN LEFT(billing_name_zh,3) IN ('仓储费','入库费','出库费','包材费','卸货费') THEN LEFT(billing_name_zh,3)
    ELSE billing_name_zh END 收入类型2,
    left(business_date,10) 日期,
    week(left(business_date,10)+ interval 1 day) 周,
    case when  w.name='AutoWarehouse' then 'AGV'
            when w.name='BPL-Return Warehouse' then 'BPL-Return'
            when w.name='BPL3-LIVESTREAM' then 'BPL3'
            when w.name='BangsaoThong' then 'BST'
            when w.name IN ('BKK-WH-LAS2电商仓','PMD-WH','BKK-WH-Ecommerce','BKK-WH-LAS物料仓')         then 'LAS'
            when w.name='LCP Warehouse' then 'LCP' end 仓库名称,
    sum(settlement_amount)/100 amount -- 结算金额
from wms_production.billing bl
left join wms_production.billing_detail bld on bl.id=bld.billing_id
left join wms_production.billing_projects blp on bld.billing_projects_id= blp.id
left join wms_production.warehouse w on bld.warehouse_id=w.id
where 1=1
    and bl.type='1'
    -- and billing_name_zh='操作费'
    and left(business_date,10) >=left(now() - interval 70 day,10)
group by 1,2,3,4,5 
having 仓库名称 is not null;