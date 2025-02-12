
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

-- 工资
select
    仓库,
    月份,
    '人力成本' 大项,
    '工资' 小项,
    sum(人力总成本) 金额
from
(
    SELECT
        gz.一级部门,
        gz.二级部门,
        case when gz.二级部门 in ('AGV Warehouse','AGV  Warehouse') then 'AGV'
             when gz.二级部门 in ('Fulfillment Bang Sao Thong warehouse','BST-Bang Sao Thong Warehouse') then 'BST'
             when gz.二级部门 in ('LAS-Lasalle Material Warehouse') then 'LAS'
             when gz.二级部门 in ('Bangphli Livestream Warehouse') then 'BPL3'
             when gz.二级部门 in ('BPL2-Bangphli Return Warehouse') then 'BPL_return'
             when gz.二级部门 in ('LCP Warehouse') then 'LCP'
             when gz.二级部门 in ('Head Office','Header Office') then 'Head Office'
             else gz.二级部门
        end 仓库,
        gz.excel_month as 月份,
        sum(gz.total_income) + sum(gz.social) + sum(gz.bonus) as 人力总成本
    FROM
    (
        SELECT
            sg.`excel_month`,
            sd.一级部门,
            sd.二级部门,
            sg.`bonus`, #年终奖 人力总成本的一部分
            sg.`total_income`, -- 人力总成本的一部分
            sg.`social` -- 人力总成本的一部分
        FROM
            `backyard_pro`.`salary_gongzi` sg
            LEFT JOIN `backyard_pro`.`hr_staff_info` hsi on hsi.`staff_info_id` = sg.`staff_info_id`
            LEFT JOIN dwm.`dwd_hr_organizational_structure_detail` sd on sd.`id` = hsi.`node_department_id`
        WHERE
            sg.excel_month >= '2023-01'
            AND sg.`company_id` = 2
            AND sd.一级部门 = 'Thailand Fulfillment'
    ) gz
    GROUP BY
        1,
        2,
        3,
        4
)
where 1=1
    and 月份 in ('2024-08', '2024-07')
    and 二级部门 in(
        'AGV Warehouse',
        'Bangphli Livestream Warehouse',
        'BPL2-Bangphli Return Warehouse',
        'BST-Bang Sao Thong Warehouse',
        'LAS-Lasalle Material Warehouse'
    )
group by
    1,
    2,
    3,
    4
union
-- 付款报销
select 
    仓库
    ,left(费用开始日期,7) 月份
    ,'付款报销'
    ,付款项
    ,sum(金额) 金额
    /* ,审核状态
    ,支付状态
    ,备注  */
    from
(
    SELECT 
        *
    FROM 
    (
        -- 普通付款
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
                when op.`apply_store_name` in ('BPL2-Bangphli Return Warehouse') then 'BPL-return'
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
            ,opd.cost_start_date 费用开始日期
            ,opd.cost_end_date 费用结束日期
        FROM `oa_production`.`ordinary_payment` op 
        LEFT JOIN `oa_production`.`ordinary_payment_detail` opd on opd.`ordinary_payment_id`=op.`id`
        LEFT JOIN `oa_production`.`budget_object` bo on opd.`budget_id`=bo.`id`
        WHERE 1=1
        -- op.`approval_status`=3 and op.`pay_status`=2
            and op.`apply_company_name`='Flash Fullfillment'
            and left(op.`should_pay_date`, 7) >= '2023-11'

        union 

        SELECT 
            '房租' 付款类型
            ,'房租' 付款项
            ,psr.`apply_no` 付款单号
            ,psr.`create_id` 申请人ID
            ,psr.`create_name` 申请人名称
            ,psr.`create_company_name` 申请业务线
            ,psr.`create_node_department_name` 申请部门
            ,psrd.`store_name` 申请网点
            ,case when psrd.`store_name` in ('AGV Warehouse','AGV  Warehouse') then 'AGV'
                when psrd.`store_name` in ('Fulfillment Bang Sao Thong warehouse','BST- Bang Sao Thong warehouse') then 'BST'
                when psrd.`store_name` in ('LAS-Lasalle Material Warehouse') then 'LAS'
                when psrd.`store_name` in ('BPL3-Bangphli Live Stream Warehouse') then 'BPL3'
                when psrd.`store_name` in ('BPL2-Bangphli Return Warehouse') then 'BPL-return'
                when psrd.`store_name` in ('LCP Warehouse') then 'LCP'
                when psrd.`store_name` in ('Head Office','Header Office') then 'Head Office'
                else psrd.`store_name`
                end 仓库
            -- ,case psr.`currency`
            --     when 1 then 'THB'
            --     when 2 then 'USD'
            --     when 3 then 'CHY'
            --     end 货币
            ,case psr.`currency`
                when 1 then psrd.`amount`
                when 2 then psrd.`amount`*32
                when 3 then psrd.`amount`*5
                end 金额
            -- ,psrd.`amount` 金额
            -- ,psrd.`actually_amount` 金额
            -- ,psr.`total_amount`
            ,psr.`created_at` 创建时间
            ,psrd.`due_date` 支付时间
            ,left(psrd.`due_date`, 7) 支付月份  
            ,psr.`remark` 备注
            -- ,psrd.`cost_start_date`
            -- ,psrd.`cost_end_date`
            ,case psr.`approval_status`
                when 1 then '待审核'
                when 2 then '已驳回'
                when 3 then '已通过'
                when 4 then '已撤回'
                end 审核状态
            ,case psr.`pay_status`
                when 1 then '待支付'
                when 2 then '已支付'
                when 3 then '未支付'
                end 支付状态
            ,psrd.cost_start_date 费用开始日期
            ,psrd.cost_end_date 费用结束日期
        FROM `oa_production`.`payment_store_renting` psr
        LEFT JOIN `oa_production`.`payment_store_renting_detail` psrd on psr.id=psrd.store_renting_id  
        WHERE psr.`create_company_name`='Flash Fullfillment'
            -- and psr.`approval_status`=3 and psr.`pay_status`=2
            and left(psrd.`due_date`, 7) >='2023-11'

        union

        SELECT 
        '报销' 付款类型
        ,bo.`name_cn` 付款项
        ,rs.`no` 付款单号
        ,rs.`apply_id` 申请人ID
        ,rs.`apply_name` 申请人名称
        ,rs.`apply_company_name` 申请业务线
        ,rs.`apply_department_name` 申请部门
        ,rs.`apply_store_name` 申请网点
        ,case when rs.`apply_store_name` in ('AGV Warehouse','AGV  Warehouse') then 'AGV'
            when rs.`apply_store_name` in ('Fulfillment Bang Sao Thong warehouse','BST- Bang Sao Thong warehouse') then 'BST'
            when rs.`apply_store_name` in ('LAS-Lasalle Material Warehouse') then 'LAS'
            when rs.`apply_store_name` in ('BPL3-Bangphli Live Stream Warehouse') then 'BPL3'
            when rs.`apply_store_name` in ('BPL2-Bangphli Return Warehouse') then 'BPL-return'
            when rs.`apply_store_name` in ('LCP Warehouse') then 'LCP'
            when rs.`apply_store_name` in ('Head Office','Header Office') then 'Head Office'
            else rs.`apply_store_name`
            end 仓库
        -- ,case rs.`currency`
        --     when 1 then 'THB'
        --     when 2 then 'USD'
        --     when 3 then 'CHY'
        --     end 货币
        ,case rs.`currency`
            when 1 then rsd.`payable_amount`/1000.0
            when 2 then rsd.`payable_amount`*32/1000.0
            when 3 then rsd.`payable_amount`*5/1000.0
            end 金额
        -- ,rsd.`payable_amount`/1000.0 金额
        ,rs.`created_at` 创建时间
        ,rs.`pay_at` 支付时间
        ,left(rs.`pay_at`, 7) 支付月份
        ,rs.`remark` 备注
        -- ,rs.`start_at` 
        -- ,rs.`end_at`
        -- ,rs.`amount`
        ,case rs.`status`
            when 1 then '待审核'
            when 2 then '已驳回'
            when 3 then '已通过'
            when 4 then '已撤回'
            end 审核状态
        ,case rs.`pay_status`
            when 1 then '待支付'
            when 2 then '已支付'
            when 3 then '未支付'
            end 支付状态
        ,rsd.`start_at` '费用发生开始时间'
        ,rsd.`end_at` '费用发生结束时间'
        FROM `oa_production`.`reimbursement` rs 
        LEFT JOIN `oa_production`.`reimbursement_detail` rsd on rsd.`re_id`=rs.`id`
        LEFT JOIN `oa_production`.`budget_object` bo on rsd.`budget_id`=bo.`id`
        -- LEFT JOIN `oa_production`.`payment_store_renting_detail` psrd on psr.id=psrd.store_renting_id  
        WHERE 1=1
            -- and rs.`status`=3 and rs.`pay_status`=2
            and rs.`apply_company_name`='Flash Fullfillment'
            and left(rs.`pay_at`, 7) >='2023-11'
    ) a
)
where 1=1
    -- and 付款项 like'%劳务%' 
    and left(费用开始日期,7)>='2024-07' 
    and left(费用开始日期,7)<='2024-08' 
    and 申请业务线='Flash Fullfillment'
    and 审核状态 not in ('已撤回', '已驳回')
    and 仓库 in ('AGV', 'BPL-return', 'BPL3', 'BST', 'LAS')
    /* and 备注 like'%August%' -- 这个是房租的时候用 */
group by 1,2,3,4

order by 仓库 

union 

-- erp 收入
select
'BPL3'
,left(账单结束日期, 7) 月份
,'收入' 大项
,分类 小项
,sum(金额) 金额
from
(
    select 
        bd.`billing_id` 
        ,w.name '仓库'
        ,bd.分类
        ,b.`end_date` '账单结束日期'
        ,bd.'账单金额（不含税）' 金额
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

union
-- wms
# wms各个国家通用
select
     仓库
    ,left(账单结束日期, 7) 月份
    ,'收入' 收入
    ,计费项
    ,SUM(结算金额)收入
from
(
    -- FFM 仓储账单明细
    -- 按计费项查看收入，用于填写仓储分析表
    SELECT b.billing_sn '应收单号'
    ,s.name '货主'
    ,case when w.name='BKK-WH-LAS2电商仓' then 'LAS'
          when w.name='BPL-Return Warehouse' then 'BPL-Return'
          when w.name='AutoWarehouse' then 'AGV'
          when w.name in ('BangsaoThong', 'BangsaoThong - Flash Home') then 'BST'
          else w.name
          end as '仓库'
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
GROUP BY 1,2,3,4