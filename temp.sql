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







-- 低值易耗品 代码备份
select
    *
from
(
    SELECT 
        '普通付款' 付款类型
        ,bo.`name_cn` 付款项
        ,op.`apply_no` 付款单号
        -- ,op.`create_id` 
        -- ,op.`create_name`
        ,op.`apply_id` 申请人ID 
        ,op.`apply_name` 申请人名称
        ,op.`apply_company_name` 申请业务线
        -- ,op.`cost_department_name` 
        ,op.`apply_node_department_name` 申请部门
        ,op.`apply_store_name` 申请网点
        ,case when op.`apply_store_name` in ('AGV Warehouse','AGV  Warehouse') then 'AGV'
            when op.`apply_store_name` in ('Fulfillment Bang Sao Thong warehouse','BST- Bang Sao Thong warehouse') then 'BST'
            when op.`apply_store_name` in ('LAS-Lasalle Material Warehouse') then 'LAS'
            when op.`apply_store_name` in ('BPL3-Bangphli Live Stream Warehouse') then 'BPL3'
            when op.`apply_store_name` in ('BPL2-Bangphli Return Warehouse') then 'BPL_return'
            when op.`apply_store_name` in ('LCP Warehouse') then 'LCP'
            when op.`apply_store_name` in ('Head Office','Header Office') then 'Head Office'
            else op.`apply_store_name`
            end 仓库
        ,case op.`currency`
            when 1 then op.`amount_total_actually`
            when 2 then op.`amount_total_actually`*32
            when 3 then op.`amount_total_actually`*5
            end 金额
        -- ,op.`amount_total_actually` 金额
        ,op.`created_at` 创建时间
        ,op.`should_pay_date` 支付时间
        ,left(op.`should_pay_date`, 7) 支付月份
        ,op.`remark` 备注
        ,case op.`approval_status`
            when 1 then '待审核'
            when 2 then '已驳回'
            when 3 then '已通过'
            when 4 then '已撤回'
            end 审核状态
        ,case op.`pay_status`
            when 1 then '待支付'
            when 2 then '已支付'
            when 3 then '未支付'
            end 支付状态
        -- ,opd.`cost_start_date` 
    FROM `oa_production`.`ordinary_payment` op 
    LEFT JOIN `oa_production`.`ordinary_payment_detail` opd on opd.`ordinary_payment_id`=op.`id`
    LEFT JOIN `oa_production`.`budget_object` bo on opd.`budget_id`=bo.`id`
    WHERE 1=1
       -- op.`approval_status`=3 and op.`pay_status`=2
        -- and op.`apply_company_name`='Flash Fullfillment'
         and op.`should_pay_date` >= date_sub(date(now() + interval -1 hour),interval 70 day)
         and bo.`name_cn` in ('低值易耗品', '办公用品', '办公家具', '办公设备', '办公费')
        and op.`apply_company_name` = 'Flash Fullfillment'
)
    