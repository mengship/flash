select
    日期
    ,week(日期+ interval 1 day) 周
    ,仓库名称
    ,收入类型1
    ,收入类型2
    ,amount
from
(
    -- 操作费
    -- wms
    select 
        left(bld.business_date,10) 日期,
        case when  w.name in ('AutoWarehouse', 'AutoWarehouse-人工仓') then 'AGV'
            when w.name='BPL-Return Warehouse' then 'BPL-Return'
            when w.name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
            when w.name='BangsaoThong' then 'BST'
            when w.name IN ('BKK-WH-LAS2电商仓') then 'LAS'
            when w.name='LCP Warehouse' then 'LCP'
            else w.name
        end 仓库名称,
        blp.billing_name_zh 收入类型1,
        blp.billing_name_zh 收入类型2,
        sum(bld.settlement_amount)/100 amount -- 结算金额
    from wms_production.billing_detail bld
    left join wms_production.billing_projects blp on bld.billing_projects_id= blp.id
    left join wms_production.warehouse w on bld.warehouse_id=w.id
    where 1=1
        and bld.business_date >= date(now() - interval 100 day) -- 看昨天的数据
        and bld.business_date < date(now()) -- 看昨天的数据
        and LEFT(blp.billing_name_zh,2) = '操作' -- 看操作费
    group by 1,2,3,4
    having 仓库名称 is not null

    union
    -- erp
    SELECT 
        jf_date
        ,'BPL3' warehouse_name
        ,分类
        ,分类
        ,SUM(amount) 收入
    FROM
    (
        SELECT 
            '操作费' AS 分类
            ,oobd.seller_id
            ,oobd.warehouse_id
            ,oobd.sn
            ,date(oobd.ji_fei_time) jf_date
            ,SUM(oobd.amount) amount
        FROM erp_wms_prod.order_operation_billing_detail oobd
        LEFT JOIN `erp_wms_prod`.`seller` sl on oobd.`seller_id`=sl.`id`
        LEFT JOIN `erp_wms_prod`.`warehouse` w on oobd.`warehouse_id`=w.`id`
        where 1=1
            and oobd.ji_fei_time >= date(now() - interval 100 day) -- 看昨天的数据
            and oobd.ji_fei_time < date(now()) -- 看昨天的数据
            and w.name='BPL3-LIVESTREAM'
        GROUP BY 2,3,4,5
    ) t0
    GROUP BY 1,2,3,4

    union -- 人力成本 薪资
    select 
        统计日期
        ,仓库
        ,type
        ,title
        ,sum(salary_sum)
    from
    dwm.dwm_th_ffm_staffsalaryOT_day
    where 1=1
        and title='人力成本'
        and 统计日期 >= date(now() - interval 100 day) 
        and 统计日期 <= date(now())
    group by 1,2,3,4

    union -- 人力成本 OT
    select 
        统计日期
        ,仓库
        ,type
        ,title
        ,sum(OTcost_sum)
    from
    dwm.dwm_th_ffm_staffsalaryOT_day
    where 1=1
        and title='人力成本'
        and 统计日期 >= date(now() - interval 100 day) 
        and 统计日期 <= date(now())
    group by 1,2,3,4

    union -- 人力成本 提成
    select 
        统计日期
        ,仓库
        ,type
        ,title
        ,sum(commission_sum)
    from
    dwm.dwm_th_ffm_staffsalaryOT_day
    where 1=1
        and title='人力成本'
        and 统计日期 >= date(now() - interval 100 day) 
        and 统计日期 <= date(now())
    group by 1,2,3,4

    union -- 人力成本 提成
    select 
        统计日期
        ,仓库
        ,type
        ,title
        ,sum(salary_sum)
    from
    dwm.dwm_th_ffm_staffsalaryOT_day
    where 1=1
        and title='管理成本'
        and 统计日期 >= date(now() - interval 100 day) 
        and 统计日期 <= date(now())
    group by 1,2,3,4

    union -- 成本 外协
    select 
        dt
        ,warehouse
        ,'人力'
        ,'外协成本'
        ,sum(num_people*550) + sum(num_ot*1.5*550) fee_peo_ot
    from dwm.th_ffm_tempworker_input
    where 1=1
        and dt >= date_sub(date(now() + interval -1 hour),interval 100 day)
    group by 1,2,3,4
    /* order by 
        warehouse
        ,dt */

)
where 1=1
and 日期>='${dt_start}'
and 日期<date_add('${dt_end}', interval 1 day)
${if(len(warehouse_name) == 0,"","and 仓库名称 in ('" + warehouse_name + "')")}