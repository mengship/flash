select
    'VN' country,
    sum(入库费) 入库费,
    sum(存储费) 存储费,
    sum(操作费) 操作费,
    sum(包材费) 包材费,
    sum(其他)  其他费用,
    sum(发货量) 发货量
from
(
    select 
        coalesce(date(b.`data_audit_time` ),date(b.rejection_time)) as 审核时间,
        s.name as 货主,
        b.billing_sn 账单号 ,
        count(distinct do.`delivery_sn`) as 发货量,
        count(distinct rw.`return_warehouse_sn`) as  出库量,
        '仓储费' as 账单类型,
        if(b.business_auditor_id =0,'未审核','已审核'） as 商务审核状态,
        case 
        when b.data_auditor_id =0 and b.rejection_time is null then '未审核'
        when b.data_auditor_id =0 and b.rejection_time is not null then '已审核'
        when b.data_auditor_id >0 then '已审核'
        else '未审核'
        end as 数据审核状态,
        'VND' as 币种,
        #b.business_audit_time as 商务审核时间,
        b.accounts_receivable/100 as 账单金额,
        bd.包材费 ,
        bdr.入库费,
        bds.存储费,
        bdp.操作费,
        bdo.其他,
        #if( b.type=1,"仓储费","快递费") as 账单类型 ,
        #b.`created`+interval -1 hour as 创建时间,
        b.data_auditor_id,
        w.`name` as 仓库,
        b.rejection_reason
    from wms_production.billing  as b
    left join `wms_production`.`seller` s on s.`id` =b.`seller_id` 
    left join (select distinct `billing_id` , `warehouse_id`  from `wms_production`.`billing_detail`) bw on bw.billing_id=b.`id`
    left join 
    #包材费
    (
        select 
            bdd.`billing_id`,
            bdd.`seller_id` ,
            bdd.`warehouse_id` ,
            sum(bdd.`settlement_amount`/100) as 包材费
        from `wms_production`.`billing_detail` bdd 
        left join `wms_production`.`billing_projects` bp on bp.`id` =bdd.`billing_projects_id`
        left join `wms_production`.`seller` s on s.`id` =bdd.`seller_id` 
        where bp.id=10 
        group by 1,2
    ) bd on bd.`billing_id` =b.`id` and bd.seller_id=b.`seller_id` 
    left join 
    #入库费
    (
        select 
            bdd.`billing_id`,
            bdd.`seller_id` ,
            bdd.`warehouse_id` ,
            sum(bdd.`settlement_amount`/100) as 入库费
        from `wms_production`.`billing_detail` bdd 
        left join `wms_production`.`billing_projects` bp on bp.`id` =bdd.`billing_projects_id`
        left join `wms_production`.`seller` s on s.`id` =bdd.`seller_id` 
        where bp.id=6 
        group by 1,2
    ) bdr on bdr.`billing_id` =b.`id` and bdr.seller_id=b.`seller_id` 
    left join 
    #存储费
    (
        select 
            bdd.`billing_id`,
            bdd.`seller_id` ,
            bdd.`warehouse_id` ,
            sum(bdd.`settlement_amount`/100) as 存储费
        from `wms_production`.`billing_detail` bdd 
        left join `wms_production`.`billing_projects` bp on bp.`id` =bdd.`billing_projects_id`
        left join `wms_production`.`seller` s on s.`id` =bdd.`seller_id` 
        where bp.id=1 
        group by 1,2
    ) bds on bds.`billing_id` =b.`id` and bds.seller_id=b.`seller_id` 
    left join 
    #操作费
    (
        select 
            bdd.`billing_id`,
            bdd.`seller_id` ,
            bdd.`warehouse_id` ,
            sum(bdd.`settlement_amount`/100) as 操作费
        from `wms_production`.`billing_detail` bdd 
        left join `wms_production`.`billing_projects` bp on bp.`id` =bdd.`billing_projects_id`
        left join `wms_production`.`seller` s on s.`id` =bdd.`seller_id` 
        where bp.id=2 
        group by 1,2
    ) bdp on bdp.`billing_id` =b.`id` and bdp.seller_id=b.`seller_id` 
    left join 
    #其他
    (
        select 
            bdd.`billing_id`,
            bdd.`seller_id` ,
            bdd.`warehouse_id` ,
            sum(bdd.`settlement_amount`/100) as 其他
        from `wms_production`.`billing_detail` bdd 
        left join `wms_production`.`billing_projects` bp on bp.`id` =bdd.`billing_projects_id`
        left join `wms_production`.`seller` s on s.`id` =bdd.`seller_id` 
        where bp.id not in (1,2,6,10)
        group by 1,2
    ) bdo on bdo.`billing_id` =b.`id` and bdo.seller_id=b.`seller_id`
    left join `wms_production`. `warehouse` w on w.id=bw.`warehouse_id` 
    left join `wms_production`.`delivery_order` do on do.`seller_id` =b.`seller_id` and date(do.`delivery_time`)>=b.`billing_start` and date(do.`delivery_time`)<=b.`billing_end` 
    left join `wms_production`.`return_warehouse` rw on rw.`seller_id` =b.`seller_id` and date(rw.`out_warehouse_time`) >=b.`billing_start`
    and date(rw.`out_warehouse_time`)<=b.`billing_end` 
    where 1=1
    #and b.`billing_sn` ='AR2402013397' 
    #and s.`name` ='PLAN B New Media'
        and date(b.`created`+interval -1 hour) >='2024-07-01'
        and date(b.`created`+interval -1 hour) <='2024-07-31'
    #and bp.id=10
        and b.accounts_receivable>0
        and b.type=1
    group by 1,2,3
    -- order by 1
    
)
group by 1
;

#  货主总数
select * from wms_production.seller where `disabled`=0;

