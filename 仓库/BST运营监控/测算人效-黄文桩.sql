
select
    '出库'
    ,warehouse_name
    ,date(delivery_time) delivery_date
    ,count(delivery_sn) delivery_cnt
    ,sum(goods_num) goods_num
from
dwm.dwd_th_ffm_outbound_dayv2
where 1=1
    and TYPE='B2C'
    and warehouse_name in ('AGV','BPL-Return','BPL3','BST','LAS')
    and date(delivery_time)>='2024-05-01'
group by
    date(delivery_time)
    ,warehouse_name
order by warehouse_name
    ,date(delivery_time)
;


select
    '人力'
    ,仓库
    ,统计日期
    ,sum(在职) 在职人数
    ,sum(出勤) 出勤人数
from
dwm.dwd_th_ffm_staff_dayV2
where 1=1
    and 在职=1
    and 统计日期>='2024-05-01'
    and 仓库 in ('AGV','BPL-Return','BPL3','BST','LAS')
group by '人力'
    ,仓库
    ,统计日期
order by 仓库
    ,统计日期
;

select 
    left(申请日期,10)
    ,仓库
    ,'OT工时' type
    ,nvl(sum(加班时长),0) 
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
        ,case when 二级部门='AGV Warehouse' then 'AGV'
            when 二级部门='BPL2-Bangphli Return Warehouse' then 'BPL-Return'
            when 二级部门='Bangphli Livestream Warehouse' then 'BPL3'
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
    FROM backyard_pro.hr_overtime ho 
    LEFT JOIN bi_pro.hr_staff_info hsi on ho.staff_id =hsi.staff_info_id 
    LEFT JOIN bi_pro.hr_staff_transfer ht on ho.staff_id =ht.staff_info_id and ho.date_at =ht.stat_date 
    left join fle_staging.sys_department sd2 on sd2.id =hsi.node_department_id 
    left join fle_staging.sys_store ss on ss.id =hsi.sys_store_id 
    left join dwm.dwd_hr_organizational_structure_detail sd  on sd.id =hsi.node_department_id 
    left join bi_pro.hr_job_title hjt on hjt.id =hsi.job_title 
    left join tmpale.ods_th_dim_date o on o.`date` =ho.date_at 
    left join bi_pro.attendance_data_v2 adv on adv.stat_date =ho.date_at and adv.staff_info_id =ho.staff_id 
    WHERE ho.state =2
    and sd.一级部门='Thailand Fulfillment'
    and ho.date_at >= '2024-01-01'
) OT
WHERE left(申请日期,10)>='2024-05-01'
    and 仓库 in ('AGV','BPL-Return','BPL3','BST','LAS')
GROUP BY 1,2,3
ORDER BY 1,2,3
;