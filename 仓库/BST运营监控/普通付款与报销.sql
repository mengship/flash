SELECT 
    付款类型
    ,付款项
    ,费用类型
    ,仓库
    ,金额
    ,支付时间
    ,支付月份
FROM 
    (
    -- 普通付款
    SELECT 
        '普通付款' 付款类型
        ,bo.`name_cn` 付款项
        ,bop.name_cn    费用类型
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
    left join `oa_production`.`budget_object_product` bop on opd.`product_id`=bop.`id`
    WHERE 1=1
        and op.`approval_status`=3 -- 3-已通过
        -- and op.`pay_status`=2 -- 2-已支付
        and op.`apply_company_name`='Flash Fullfillment'
        and left(op.`should_pay_date`, 7) >= '2024-01'

    UNION 
    SELECT 
        '报销' 付款类型
        ,bo.`name_cn` 付款项
        ,bop.name_cn    费用类型
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
            when rs.`apply_store_name` in ('BPL2-Bangphli Return Warehouse') then 'BPL_return'
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
    FROM `oa_production`.`reimbursement` rs 
    LEFT JOIN `oa_production`.`reimbursement_detail` rsd on rsd.`re_id`=rs.`id`
    LEFT JOIN `oa_production`.`budget_object` bo on rsd.`budget_id`=bo.`id`
    left join `oa_production`.`budget_object_product` bop on rsd.`product_id`=bop.`id`
    -- LEFT JOIN `oa_production`.`payment_store_renting_detail` psrd on psr.id=psrd.store_renting_id  
    WHERE 1=1
        and rs.`status`=3 -- 3同意
        -- and rs.`pay_status`=2 -- 2已支付
        and rs.`apply_company_name`='Flash Fullfillment'
        and left(rs.`pay_at`, 7) >='2024-01'
    ) a
    ;

-- 明细
SELECT 
    *
FROM 
(
    -- 普通付款
    SELECT 
        '普通付款' 付款类型
        ,bo.`name_cn` 付款项
        ,bop.name_cn    费用类型
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
    left join `oa_production`.`budget_object_product` bop on opd.`product_id`=bop.`id`
    WHERE 1=1
        and op.`approval_status`=3 -- 3-已通过
        -- and op.`pay_status`=2 -- 2-已支付
        and op.`apply_company_name`='Flash Fullfillment'
        and left(op.`should_pay_date`, 7) >= '2024-01'

    UNION 
    SELECT 
        '报销' 付款类型
        ,bo.`name_cn` 付款项
        ,bop.name_cn    费用类型
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
            when rs.`apply_store_name` in ('BPL2-Bangphli Return Warehouse') then 'BPL_return'
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
    FROM `oa_production`.`reimbursement` rs 
    LEFT JOIN `oa_production`.`reimbursement_detail` rsd on rsd.`re_id`=rs.`id`
    LEFT JOIN `oa_production`.`budget_object` bo on rsd.`budget_id`=bo.`id`
    left join `oa_production`.`budget_object_product` bop on rsd.`product_id`=bop.`id`
    -- LEFT JOIN `oa_production`.`payment_store_renting_detail` psrd on psr.id=psrd.store_renting_id  
    WHERE 1=1
        and rs.`status`=3 -- 3同意
        -- and rs.`pay_status`=2 -- 2已支付
        and rs.`apply_company_name`='Flash Fullfillment'
        and left(rs.`pay_at`, 7) >='2024-01'
) a
where 付款项='车辆租赁费'