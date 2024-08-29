-- 刘凡老师的需求，计算两个日期之间的工作日天数
select 
    *
    ,tt.审批时长day-tt.周末天数-tt.节假日天数 去除周末和节假日天
from 
(
    select 
        `id` ,
        `biz_value` ,
        单据号,
        类型,
        创建日期,
        结束日期,
        审批流名称,
        审批流节点名称,
        工号,
        姓名,
        职位,
        部门,
        type,
        时间,
        前一个节点时间,
        审批时长day,
        数量
        ,sum(if(dt_type='周末', 周末节假日计数, 0)) 周末天数
        ,sum(if(dt_type='节假日', 周末节假日计数, 0)) 节假日天数
    from 
    (
        SELECT  
            wr.`id` ,
            wr.`biz_value` ,
            wr.name 单据号,
            case wr.`biz_type` 
                when 1   then '其他合同'
                when 8   then '借款申请'
                when 9   then '采购申请'
                when 10  then '采购订单'
                when 11  then '采购付款申请单'
                when 12  then '采购验收单'
                when 13  then '报销申请'
                when 14  then '网点租房合同申请'
                when 15  then '薪酬审批流程'
                when 20  then '销售标准主合同定结'
                when 21  then '销售标准主合同现结'
                when 22  then '销售非标准主合同'
                when 23  then '销售附属合同授权内'
                when 24  then '销售附属合同授权外'
                when 29  then '网点租房付款'
                when 30  then '普通付款'
                when 43  then '薪资发放审批'
                when 50  then '取数需求工单审批'
                when 51  then '网点备用金申请审批'
                when 52  then '网点备用金归还'
                when 53  then '报价审批'
                when 58  then '借款归还'
                when 59  then '供应商信息审批'
                when 60  then '财务预算审批流'
                when 76  then '支付模块审批'
                when 91  then '预算调整'
                when 92  then '样品确认审批流'
                when 93  then '押金归还'
                when 94  then '资产申请（领用）以及审批'
                when 96  then '支票购买申请'
                when 97  then '供应商分级管理'
                when 98  then '网点租房合同-作废'
                when 99  then '网点租房合同-终止'
                when 100 then '网点租房合同续签'
                when 101 then 'GPMD平台集团合同'
                when 102 then '电子合同商务审核'
            end 类型,
            date(wr.`created_at`) 创建日期,
            COALESCE (date(wr.`approved_at`),date(wr.`rejected_at`)) 结束日期,
            log.审批流名称,
            log.审批流节点名称,
            log.工号,
            log.姓名,
            log.职位,
            log.部门,
            log.type,
            log.时间,
            lag(log.时间,1) over(partition by wr.`id` order by log.时间) as 前一个节点时间,
            round(TIMESTAMPDIFF(minute,lag(log.时间,1) over(partition by wr.`id` order by log.时间),log.时间)/60/24,2) 审批时长day
            -- , COUNT(DISTINCT rd.id)
            -- , COUNT(pop.poid)
            -- ,CONCAT(COUNT(DISTINCT rd.id),",",COUNT(pop.id))
            ,case wr.`biz_type` 
                when 13 then COUNT(DISTINCT rd.id)
                when 10 then COUNT(distinct pop.id)
                when 30 then COUNT(DISTINCT opd.id)
                when 29 then COUNT(DISTINCT psrd.id)
                else 1
            end 数量
        FROM
            `oa_production`.`workflow_request` wr
        left join
        (
            select
                case  wa.`audit_action`
                        when 0 then '申请'
                        when 1 then '同意'
                        when 2 then '驳回'
                        when 6 then '撤销'
                        when 8 then '自动同意'
                end as type,
                wa.`request_id`,
                wf.name 审批流名称,
                wn.`name` 审批流节点名称,
                wa.`staff_id` 工号,
                wa.`staff_name` 姓名,
                wa.`staff_job_title` 职位,
                wa.`staff_department` 部门,
                wa.`audit_at` 时间
            FROM oa_production.`workflow_audit_log` wa
            LEFT JOIN oa_production.workflow wf on wf.`id` = wa.`flow_id`
            LEFT JOIN `oa_production`.`workflow_node` wn on wn.`id` = wa.`flow_node_id`
            where 1=1
            -- and wa.`audit_action` not in (6,8) -- 不包括撤回和自动通过
                and wa.`audit_action` not in (8)

            union all

            SELECT
                case fyr.action_type
                    when 1 then '征询'
                    when 2 then '回复征询'
                end 'type',
                fyr.`request_id`,
                wf.name 审批流名称,
                wn.`name` 审批流节点名称,
                fyr.`staff_id` 工号,
                fyr.`staff_name` 姓名,
                fyr.`staff_job_title` 职位 ,
                fyr.`staff_department` 部门,
                fyr.`created_at` 时间
            FROM `oa_production`.`workflow_request_node_fyr` fyr
            LEFT JOIN oa_production.workflow wf on wf.`id` = fyr.`flow_id`
            LEFT JOIN `oa_production`.`workflow_node` wn on wn.`id` = fyr.`flow_node_id`

            union all

            select 
                case py. `pay_status` 
                    when 1 then '待支付'
                    when 2 then '已支付'
                    when 3 then '未支付'
                    when 4 then '支付中'
                    when 5 then '支付失败'
                    when 6 then '银行支付中'
                    when 7 then 'pay支付中'
                    when 8 then 'pay支付失败'
                end 'type',
                wr.id request_id,
                '支付模块审批' 审批流名称,
                '支付模块-支付人' 审批流节点名称,
                py.payer_id 工号,
                ss.name 姓名,
                jt.job_name 职位,
                de.name 部门,
                convert_tz(py.payer_date,'+00:00','+08:00') 时间
            from `oa_production`.`workflow_request` wr
            join `oa_production`.payment py on py.id=wr.biz_value and wr.biz_type = 76
            join bi_pro.hr_staff_info ss on ss.staff_info_id =py.payer_id
            join bi_pro.sys_department de on de.id=ss.node_department_id
            join bi_pro.hr_job_title jt on jt.id=ss.job_title
            -- where py.no = 'FK202312200002'
        ) log on log.request_id=wr.`id`
        left join oa_production.reimbursement_detail rd on rd.re_id=wr.`biz_value`
        left join oa_production.purchase_order_product pop on pop.poid=wr.`biz_value`
        left join oa_production.ordinary_payment_detail opd on opd.ordinary_payment_id=wr.`biz_value`
        left join oa_production.payment_store_renting_detail psrd on psrd.store_renting_id=wr.`biz_value`
        where 1=1
            -- and  (wr.name like 'BX202401260009%' or wr.name like 'PO202401160003%' or wr.name like "PTFK202401290051%" or wr.name like "FK202401170005%")
            -- and wr.`state` in (2,3) -- 驳回和通过状态
            -- and wr.`created_at` >='2023-10-01'
            --  and  wr.`created_at` <'2024-1-19'
            -- and  log.时间 <= COALESCE (wr.`approved_at`,wr.`rejected_at`) 
            
        group by 1,2，14
        order by 1,2,3,12
    ) t 
    left join
    (
        select
            '日期与节假日'
            ,date
            ,day_of_week
            ,off_date
            ,dt_type
            ,if(dt_type in ('周末', '节假日'), 1, 0) 周末节假日计数
        from
        (
            select
                '日期与节假日'
                ,dt.date
                ,dt.day_of_week
                ,ho.off_date
                ,case when dt.day_of_week in (6, 7) then '周末'
                    when ho.off_date is not null then '节假日'
                    else '工作日'
                    end as dt_type
            from
            (
                select
                    date
                    ,day_of_week
                from 
                tmpale.ods_th_dim_date
                where 1=1
                    and date>='2024-01-01'
                    and date<='2024-12-31'
            ) dt
            left join
            (
                SELECT 
                    sh.`off_date`
                FROM fle_staging.`sys_holiday` sh
                WHERE sh.`off_date` >= '2024-01-01'
                    AND sh.`off_date` <= '2024-12-31'
                    AND sh.`deleted` = 0
                GROUP BY sh.`off_date`
                -- ORDER BY sh.`off_date`
            ) ho on dt.date = ho.off_date
        ) dt1
    )dt2 on dt2.date >= date(t.前一个节点时间) and dt2.date <= date(t.时间)
    where t.部门 in ('Finance and Accounting','Payment (Finance Operations)')
            -- t.工号 in ('141392' ,'133357' , '119778' , '136021', '130958','143855','144293','144342'）
            -- and t.时间 >='2023-09-01'
            -- and  t.时间 <'2024-1-29'
            -- and date(t.时间)= DATE_ADD(CURDATE(),INTERVAL 1 day))
            and date(t.时间) >= DATE_SUB(CURDATE(),INTERVAL 90 day)
    group by `id` ,
        `biz_value` ,
        单据号,
        类型,
        创建日期,
        结束日期,
        审批流名称,
        审批流节点名称,
        工号,
        姓名,
        职位,
        部门,
        type,
        时间,
        前一个节点时间,
        审批时长day
) tt
;
