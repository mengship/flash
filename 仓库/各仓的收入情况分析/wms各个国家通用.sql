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
    where b.billing_end >= '2024-01-01'
    and b.billing_end <= '2024-05-31'
    and b.status not in (0,50)
    and bd.settlement_amount <> 0
    and b.type=1
    # and b.data_audit_time is null
    # and b.status = 11
    group by 1,2,3,7
    order by b.type,w.name,b.billing_start,s.name
)
GROUP BY 1,2,3,4,5
