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



    union
     -- 成本 工资模块
    select
        统计日期
        ,仓库
        ,'人力'
        ,'人力成本'
        ,sum(salary_sum) salary_sum
    from
    (
        select
            统计日期
            ,仓库
            ,人员信息
            ,case when fsd.job_title_grade_v2=0 and 出勤>0 then round(出勤*(ifnull(gs.daysalary,0) + ifnull(gs.housesubsidy,0) + ifnull(gs.mealsubsidy,0) + ifnull(gs.socialsecurity,0)/26 + ifnull(subsidy,0)/26) ,0)
                when fsd.job_title_grade_v2=0 and 出勤=0 then 0
                else round((ifnull(gs.salary/26,0) + ifnull(gs.housesubsidy,0) + ifnull(gs.mealsubsidy,0) + ifnull(gs.socialsecurity,0)/26 + ifnull(subsidy,0)/26)*应出勤成本 ,0)
            end as salary_sum
            ,gs.daysalary
            ,gs.housesubsidy	
            ,gs.mealsubsidy
            ,gs.socialsecurity
            ,fjs.subsidy
            ,fsd.职位
            ,fsd.job_title_grade_v2
        from
        dwm.dwd_th_ffm_staff_dayV2 fsd 
        left join tmpale.tmp_th_ffm_jobsubsidy fjs on fsd.职位 = fjs.job
        left join tmpale.tmp_th_ffm_gradesalary gs on concat('F',fsd.job_title_grade_v2) = gs.grade
        where fsd.在职=1
        and 统计日期 >= date(now() - interval 100 day)
        and 人员信息 not in ('606321' -- 剔除部分员工
            ,'661237'
            ,'32310'
            ,'672950'
            ,'78178'
            ,'636585')
        -- and 仓库='BPL3'
    ) t0
    where 仓库 in ('AGV', 'BPL-Return', 'BPL3', 'BST', 'LAS')
    group by 1,2,3,4

    union
    -- 成本 OT
    select
        申请日期
        ,仓库
        ,'人力'
        ,'OT成本' OT类型
        ,sum(salary_sum_hour)
    from
    (
        select
            申请日期
            ,仓库
            ,'人力'
            ,'OT成本'
            ,OT类型
            ,OT类型*加班时长*salary_sum_hour  salary_sum_hour
        from
        (
            SELECT 
                ho.date_at 申请日期
                ,ho.staff_id 员工ID
                ,hsi.name 员工姓名
                ,CASE hsi.state 
                        when 1 then '在职'
                        when 2 then '离职'
                        when 3 then '停职'
                        else ht.state 
                end 在职状态
                ,case when 二级部门 in ('AutoWarehouse', 'AutoWarehouse-人工仓') then 'AGV'
                    when 二级部门='BPL2-Bangphli Return Warehouse' then 'BPL-Return'
                    when 二级部门 in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
                    when 二级部门='BST-Bang Sao Thong Warehouse' then 'BST'
                    when 二级部门='LAS-Lasalle Material Warehouse' then 'LAS'
                    when 二级部门='LCP Warehouse' then 'LCP'
                end 仓库
                ,case 
                    when left(三级部门,4)='Pack' then 'Packing'
                    when left(三级部门,4)='Pick' then 'Picking'
                    when left(三级部门,3)='Out' then 'Outbound'
                    when left(三级部门,3)='Inb' then 'Inbound'
                    when left(三级部门,3)='B2B' then 'B2B'
                        else 'HO'
                end 职位类别
                ,sub_type
                ,hjt.job_name 职位
                ,sd2.name 部门
                ,sd.一级部门
                ,sd.二级部门
                ,sd.三级部门
                ,sd.四级部门
                ,if(hsi.sys_store_id ='-1','Head Office',ss.name) 网点
                ,CASE ho.`type` 
                        when 1 then 1.5
                        when 2 then 3
                        when 4 then 1
                        ELSE 0 
                end OT类型
                ,ho.start_time 开始时间
                ,ho.end_time 结束时间
                ,ho.duration 加班时长
                ,o.day_of_week 周几
                ,o.week_begin_date 周最早日期
                ,o.week_end_date 周最晚日期
                ,CASE when ho.`type` =4 and adv.times1 >0 then '是'
                        when ho.`type` =1 and adv.times1_5 >0 then '是'
                        when ho.`type` =2 and adv.times3 >0 then '是'
                        ELSE '否'
                END  是否给加班费
                ,hsi2.job_title_grade_v2
                ,round((ifnull(gs.daysalary,0) + ifnull(gs.housesubsidy,0) + ifnull(gs.mealsubsidy,0) + ifnull(gs.socialsecurity,0)/30 + ifnull(subsidy,0)/30)/8, 0) salary_sum_hour
            FROM backyard_pro.hr_overtime ho 
            LEFT JOIN bi_pro.hr_staff_info hsi on ho.staff_id =hsi.staff_info_id 
            LEFT JOIN bi_pro.hr_staff_transfer ht on ho.staff_id =ht.staff_info_id and ho.date_at =ht.stat_date 
            left join fle_staging.sys_department sd2 on sd2.id =hsi.node_department_id 
            left join fle_staging.sys_store ss on ss.id =hsi.sys_store_id 
            left join dwm.dwd_hr_organizational_structure_detail sd  on sd.id =hsi.node_department_id 
            left join bi_pro.hr_job_title hjt on hjt.id =hsi.job_title 
            left join tmpale.ods_th_dim_date o on o.`date` =ho.date_at 
            left join bi_pro.attendance_data_v2 adv on adv.stat_date =ho.date_at and adv.staff_info_id =ho.staff_id
            left join backyard_pro.hr_staff_info hsi2 on hsi2.staff_info_id = ho.staff_id
            left join tmpale.tmp_th_ffm_jobsubsidy fjs on hjt.job_name = fjs.job
            left join tmpale.tmp_th_ffm_gradesalary gs on concat('F',hsi2.job_title_grade_v2) = gs.grade
            WHERE ho.state =2
                and sd.一级部门='Thailand Fulfillment'
                and ho.date_at >= date_sub(date(now() + interval -1 hour),interval 100 day)
        ) t0
        where 仓库 is not null
    ) t1
    group by 1,2,3,4

    union
    -- 成本 提成
    select
        统计日期
        ,仓库
        ,'人力'
        ,'提成成本'
        ,cnt_people*commission
    from
    (
        select
            统计日期
            ,仓库
            ,''
            ,''
            ,count(人员信息) cnt_people
        from 
        dwm.dwd_th_ffm_staff_dayV2 
    where 仓库 in ('AGV', 'BPL-Return', 'BPL3', 'BST', 'LAS')
        and 统计日期 >= date_sub(date(now() + interval -1 hour),interval 100 day)
    group by 统计日期
            ,仓库
    ) fsd
    left join tmpale.tmp_th_ffm_commission_0910 fc on fsd.仓库 = fc.warehouse_name
    /* order by 统计日期 desc */

    union
    -- 成本 外协
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