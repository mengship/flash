# wms各个国家通用
select
    'TH' 国家
    ,仓库
    ,货主
    ,计费项
    ,账单结束日期
    ,SUM(结算金额)收入
from
(
    -- FFM 仓储账单明细
    -- 按计费项查看收入，用于填写仓储分析表
    SELECT b.billing_sn '应收单号'
    ,s.name '货主'
    ,w.name '仓库'
    ,case b.type
        when 1 then '仓储费'
        when 2 then '快递费'
        when 3 then '保险费'
        else b.type end '费用类型'
    ,b.billing_start '账单开始日期'
    ,b.billing_end '账单结束日期'
    ,bp.billing_name_zh '计费项'
    ,round(sum(bd.settlement_amount/100),2) '结算金额'
    ,case b.status -- '[0]删除 [10]已创建 [15]待处理 [20]已处理 [30]未结清 [40]已结清 [50] 不结算'
        when 0 then '删除'
        when 10 then '已创建'
        when 11 then '商务已审核'
        when 14 then '待确认'
        when 15 then '待处理'
        when 20 then '已处理'
        when 30 then '待结算'
        when 40 then '已结清'
        when 50 then '不结算'
        else b.status end '账单状态'
    ,if(b.data_audit_time is not null,'数据已审','数据未审') '数据是否审核'
    FROM wms_production.billing b
    left join wms_production.billing_detail bd on b.id = bd.billing_id
    left join wms_production.billing_projects bp on bp.id = bd.billing_projects_id
    left join wms_production.seller s on s.id = b.`seller_id`
    left join wms_production.warehouse w on w.id = bd.warehouse_id
    where b.billing_end >= '2024-07-01'
    and b.billing_end <= '2024-08-31'
    and b.status not in (0,50)
    and bd.settlement_amount <> 0
    and b.type=1
    # and b.data_audit_time is null
    # and b.status = 11
    group by 1,2,3,7
    order by b.type,w.name,b.billing_start,s.name
)
GROUP BY 1,2,3,4,5
;


erp 收入

with a  as
(
    select bd.`billing_id` ,'订单操作费' as '分类',bd.`order_operation_no_tax_amount` as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'包材使用流水' as '分类',bd.`container_use_no_tax_amount` as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'调整单流水' as '分类',bd.adjust_order_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'入库费用流水' as '分类',bd.inventory_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'退货入库费用流水' as '分类',bd.inventory_return_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'存储费用流水' as '分类',bd.storage_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'耗材使用流水' as '分类',bd.consumables_use_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'出库费用流水' as '分类',bd.stock_out_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'卸货费用流水' as '分类',bd.unloading_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'贴码费用流水' as '分类',bd.label_service_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'拦截归位费用流水' as '分类',bd.order_clear_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'盘点费用流水' as '分类',bd.location_inventory_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'销毁任务费用流水' as '分类',bd.destroy_task_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'换包装任务费用流水' as '分类',bd.change_package_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'组装拆卸费用流水(工单来源)' as '分类',bd.ad_task_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'组装拆卸费用流水(订单来源)' as '分类',bd.ad_task_order_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'手动添加流水' as '分类',bd.manual_add_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'退供出库费用流水' as '分类',bd.provider_return_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'用工加班任务流水' as '分类',bd.overtime_task_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'车辆使用费用流水' as '分类',bd.car_task_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'效期商品管理费用流水' as '分类',bd.validity_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'组装拆卸费用流水(订单来源)' as '分类',bd.ad_task_order_new_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'贴码费用流水(订单来源)' as '分类',bd.label_service_order_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'消耗品存储费流水' as '分类',bd.consumables_storage_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'折扣扣减不含税结算金额' as '分类',-bd.`discount_reduce_no_tax_amount` as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'SLA服务费（不含税）' as '分类',bd.`sla_no_tax_amount` as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
#    select bd.`billing_id` ,'不含税账单金额' as '分类',bd.`no_tax_amount` as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
#    union all
    select bd.`billing_id` ,'结算金额（不含税）' as '分类',-bd.`settle_no_tax_amount` as '账单金额（不含税）' from erp_wms_prod.billing_detail bd     -- '不含税账单金额'+'折扣扣减不含税结算金额'+'SLA服务费（不含税）'='结算金额（不含税）',校验计费项是否完整,所有合计为0

) 

SELECT 
	'TH' 国家
	,w.name '仓库'
	,s.name '货主'
	,a.'分类'
	,b.`end_date` '账单结束日期'
	,a.'账单金额（不含税）'
from a
left join erp_wms_prod.billing b on b.`id` =a.billing_id
left join erp_wms_prod.`warehouse` w on b.`warehouse_id` = w.`id` 
left join erp_wms_prod.`seller` s on b.`seller_id` = s.`id`
WHERE b.`end_date` > '2024-01-01'
and b.`end_date` <= '2024-06-30'
and a.'账单金额（不含税）'!= 0
and w.name = 'BPL3-LIVESTREAM';


with a  as
(
    select bd.`billing_id` ,'订单操作费' as '分类',bd.`order_operation_no_tax_amount` as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'包材使用流水' as '分类',bd.`container_use_no_tax_amount` as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'调整单流水' as '分类',bd.adjust_order_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'入库费用流水' as '分类',bd.inventory_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'退货入库费用流水' as '分类',bd.inventory_return_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'存储费用流水' as '分类',bd.storage_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'耗材使用流水' as '分类',bd.consumables_use_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'出库费用流水' as '分类',bd.stock_out_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'卸货费用流水' as '分类',bd.unloading_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'贴码费用流水' as '分类',bd.label_service_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'拦截归位费用流水' as '分类',bd.order_clear_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'盘点费用流水' as '分类',bd.location_inventory_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'销毁任务费用流水' as '分类',bd.destroy_task_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'换包装任务费用流水' as '分类',bd.change_package_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'组装拆卸费用流水(工单来源)' as '分类',bd.ad_task_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'组装拆卸费用流水(订单来源)' as '分类',bd.ad_task_order_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'手动添加流水' as '分类',bd.manual_add_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'退供出库费用流水' as '分类',bd.provider_return_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'用工加班任务流水' as '分类',bd.overtime_task_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'车辆使用费用流水' as '分类',bd.car_task_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'效期商品管理费用流水' as '分类',bd.validity_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'组装拆卸费用流水(订单来源)' as '分类',bd.ad_task_order_new_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'贴码费用流水(订单来源)' as '分类',bd.label_service_order_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'消耗品存储费流水' as '分类',bd.consumables_storage_fee_no_tax_amount as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'折扣扣减不含税结算金额' as '分类',-bd.`discount_reduce_no_tax_amount` as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
    union all
    select bd.`billing_id` ,'SLA服务费（不含税）' as '分类',bd.`sla_no_tax_amount` as '账单金额（不含税）' from erp_wms_prod.billing_detail bd
)
select
仓库
,left(账单结束日期, 7) 月份
,'收入' 大项
,分类 小项
,sum('账单金额（不含税）') 金额
from
(
    select 
        bd.`billing_id` 
        ,w.name '仓库'
        ,bd.分类
        ,b.`end_date` '账单结束日期'
        ,bd.'账单金额（不含税）'
    from a bd
    left join erp_wms_prod.billing b on b.`id` =bd.billing_id
    left join erp_wms_prod.`warehouse` w on b.`warehouse_id` = w.`id` 
    left join erp_wms_prod.`seller` s on b.`seller_id` = s.`id`
    WHERE b.`end_date` > '2024-07-01'
    and b.`end_date` <= '2024-08-31'
    and bd.'账单金额（不含税）'!= 0
    and w.name = 'BPL3-LIVESTREAM'    
) t_out
group by 1,2,3,4
;