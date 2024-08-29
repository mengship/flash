with a as 
(
    SELECT 
        人员信息
        ,统计日期
        ,一级部门
        ,部门
        ,仓库
        ,在职
        ,职位类别
        ,职位
        ,上班打卡时间
        ,班次开始
        ,下班打卡时间
        ,班次结束
        ,公休日
        ,休息日
        ,if(出勤=1,1,应出勤) 应出勤
        ,出勤
    --    ,应出勤-请假-旷工 出勤
        -- ,未出勤
        ,请假
        -- ,请假时段
        ,if(迟到>0,floor(迟到),0) 迟到
        ,旷工
        ,年假
        ,事假
        ,病假
        ,产假
        ,丧假
        ,婚假
        ,公司培训假
        ,跨国探亲假
        ,旷工最晚时间
        ,now() + interval -1 hour update_time
    FROM
    (
        SELECT
            人员信息
            ,统计日期
            ,一级部门
            ,部门
            ,仓库     
            ,职位类别
            ,职位
            ,在职
            ,上班打卡时间
            ,班次开始
            ,下班打卡时间
            ,班次结束
            ,公休日
            ,休息日
            ,应出勤
            ,出勤
            -- ,未出勤
            ,请假
            ,请假时段
            ,case when 应出勤=1 AND (上班打卡时间 is null or 下班打卡时间 is null) AND 请假=0 then 0 
                when 应出勤=1 AND (上班打卡时间 is null or 下班打卡时间 is null) AND 请假=0.5 then 0
                when 应出勤=1 AND 请假=1 then 0 
                when 应出勤=1 AND 请假=0.5 AND 请假时段='上午半天假' AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(旷工最晚时间))/60<=120 then (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(旷工最晚时间))/60 
                when 应出勤=1 AND 请假=0.5 AND 请假时段='上午半天假' AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(旷工最晚时间))/60>120 then 0
                when 应出勤=1 AND 请假=0.5 AND 请假时段='下午半天假' AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60<=120 then (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60 
                when 应出勤=1 AND 请假=0.5 AND 请假时段='下午半天假' AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60>120 then 0
                when 应出勤=1 AND 请假=0 AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60<=120 then (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60
                when 应出勤=1 AND 请假=0 AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60>120 then 0
                when 应出勤=1 AND 请假=0 AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(旷工最晚时间))/60>0 then 0
                else 0
                end 迟到
            -- 当天没有上班卡或者下班卡为全天旷工；迟到120分钟内，算作迟到，5泰铢/分钟罚款；迟到超过120分钟，算半天旷工；迟到超过13:00或19:00，算全天旷工
            ,case when 应出勤=1 AND (上班打卡时间 is null or 下班打卡时间 is null) AND 请假=0 then 1 
                when 应出勤=1 AND (上班打卡时间 is null or 下班打卡时间 is null) AND 请假=0.5 then 0.5
                when 应出勤=1 AND 请假=1 then 0 
                when 应出勤=1 AND 请假=0.5 AND 请假时段='上午半天假' AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(旷工最晚时间))/60<=120 then 0 
                when 应出勤=1 AND 请假=0.5 AND 请假时段='上午半天假' AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(旷工最晚时间))/60>120 then 0.5
                when 应出勤=1 AND 请假=0.5 AND 请假时段='下午半天假' AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60<=120 then 0 
                when 应出勤=1 AND 请假=0.5 AND 请假时段='下午半天假' AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60>120 then 0.5
                when 应出勤=1 AND 请假=0 AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60<=120 then 0
                when 应出勤=1 AND 请假=0 AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60>120 then 0.5
                when 应出勤=1 AND 请假=0 AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(旷工最晚时间))/60>0 then 1
                else 0
                end 旷工
            ,年假
            ,事假
            ,病假
            ,产假
            ,丧假
            ,婚假
            ,公司培训假
            ,跨国探亲假
            ,旷工最晚时间
        FROM
        (
            SELECT 
                人员信息
                ,统计日期
                ,一级部门
                ,部门
                ,仓库
                ,职位类别
                ,职位
                ,在职
                ,上班打卡时间
                ,班次开始
                ,下班打卡时间
                ,班次结束
                ,公休日
                ,休息日
                ,应出勤
                ,出勤
                ,未出勤
                ,请假
                ,请假时段
                ,年假
                ,事假
                ,病假
                ,产假
                ,丧假
                ,婚假
                ,公司培训假
                ,跨国探亲假
                ,concat(统计日期,' ',旷工最晚时间,':','00' ) 旷工最晚时间
            FROM 
            (
                SELECT 
                    人员信息
                    ,统计日期
                    ,一级部门
                    ,部门
                    ,仓库
                    ,职位类别
                    ,职位
                    ,在职
                    ,上班打卡时间
                    ,班次开始
                    ,下班打卡时间
                    ,班次结束
                    ,公休日
                    ,休息日
                    ,if(应出勤>0 and 病假<=0 and 请假>0, 0, 应出勤) 应出勤
                    ,出勤
                    ,未出勤
                    ,请假
                    ,请假时段
                    ,年假
                    ,事假
                    ,病假
                    ,产假
                    ,丧假
                    ,婚假
                    ,公司培训假
                    ,跨国探亲假
                    ,if(shift_start<'12:00','13:00','19:00') 旷工最晚时间
                FROM 
                (
                    SELECT 
                        ad.`staff_info_id` 人员信息
                        ,ad.`stat_date` 统计日期
                        ,sd.`一级部门` 一级部门
                        ,sd.`二级部门` 二级部门
                        ,sd.`name` 部门
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
                        ,hjt.`job_name` 职位
                        ,case when ad.state='1' then 1 else 0 end  在职   
                        ,ad.`shift_start`
                        ,ad.`shift_end`
                        ,ad.`attendance_started_at` as 上班打卡时间
                        ,if(ad.`shift_start`<>'',concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ),null) as 班次开始
                        ,ad.`attendance_end_at` as 下班打卡时间
                        ,if(ad.`shift_end`<>'',concat(ad.`stat_date`,' ',ad.`shift_end`,':','00' ),null) as 班次结束
                        -- ,if
                        -- ,UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP((ad.`shift_start`<>'',concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ),null) )
                        -- ,ad.leave_type
                        ,case ad.leave_time_type when 1 then '上午半天'
                            when 2 then '下午半天'
                            when 3 then '全天'
                            end 请假时段
                        ,ad.attendance_time
                        -- ,ad.`AB`
                        ,if(ad.PH!=0,1,0) 公休日
                        ,if(ad.OFF!=0,1,0) 休息日
                        ,if(ad.PH=0 AND ad.OFF=0,1,0) 应出勤
                        ,CASE WHEN  ad.`attendance_started_at` IS NOT NULL THEN 1 ELSE 0 END 出勤
                        -- ,if(ad.PH=0 AND ad.OFF=0 AND ad.attendance_time=0,1,0) 未出勤天数
                        ,case when ad.PH=0 AND ad.OFF=0 AND ad.attendance_time=0 then 1 
                            when ad.PH=0 AND ad.OFF=0 AND ad.attendance_time=5 then 0.5 
                            when ad.PH=0 AND ad.OFF=0 AND ad.attendance_time=10 then 0
                            end 未出勤
                        ,ad.`AB`
                        -- ,UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' )) 迟到分钟
                        -- ,case when ad.PH=0 AND ad.OFF=0 
                        --      AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))>0
                        --      AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))<=30 then UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))
                        --      else 0
                        --      end 迟到分钟
                        -- ,UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_end`,':','00' ))-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' )) 班次时长
                        -- 当天没有上班卡或者下班卡为全天旷工；迟到30分钟内，算作迟到，5泰铢/分钟罚款；迟到超过30分钟，算半天旷工；迟到超过4小时，算全天旷工
                        ,case when ad.PH=0 AND ad.OFF=0 
                                AND (ad.`attendance_started_at` is null or ad.`attendance_end_at` is null) 
                                AND ad.leave_type not in (1,2,12,3,18,4,5,17,7,10,16,19) then 1 
                            when ad.PH=0 AND ad.OFF=0 
                                AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))>0
                                AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))<=30  then 0
                            -- when ad.PH=0 AND ad.OFF=0 
                            --  AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))>30
                            --  AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))<=240  then 0.5
                            when ad.PH=0 AND ad.OFF=0 
                                AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))>30
                                AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))<=(UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_end`,':','00' ))-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' )))/2  then 0.5
                            -- when ad.PH=0 AND ad.OFF=0 
                            --  AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))>240 then 1
                            when ad.PH=0 AND ad.OFF=0 
                                AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))>(UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_end`,':','00' ))-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' )))/2 then 1
                            else 0
                            end 旷工
                        ,case when ad.leave_type in (1,2,12,3,18,4,5,17,7,10,16,19) AND ad.leave_time_type=1 then 0.5
                            when ad.leave_type in (1,2,12,3,18,4,5,17,7,10,16,19) AND ad.leave_time_type=2 then 0.5
                            when ad.leave_type in (1,2,12,3,18,4,5,17,7,10,16,19) AND ad.leave_time_type=3 then 1 
                            else 0 
                            end 请假
                        ,case 
                            when ad.leave_type=1 and ad.leave_time_type=1 then 0.5
                            when ad.leave_type=1 and ad.leave_time_type=2 then 0.5
                            when ad.leave_type=1 and ad.leave_time_type=3 then 1
                            else 0
                            end 年假
                        ,case 
                            -- 带薪,不带薪事假
                            when ad.leave_type in (2,12) and ad.leave_time_type=1 then 0.5
                            when ad.leave_type in (2,12) and ad.leave_time_type=2 then 0.5
                            when ad.leave_type in (2,12) and ad.leave_time_type=3 then 1
                            else 0
                            end 事假
                        ,case 
                            -- 带薪,不带薪病假
                            when ad.leave_type in (3,18) and ad.leave_time_type=1 then 0.5
                            when ad.leave_type in (3,18) and ad.leave_time_type=2 then 0.5
                            when ad.leave_type in (3,18) and ad.leave_time_type=3 then 1
                            else 0
                            end 病假
                        ,case 
                            -- 产假,陪产假,产检
                            when ad.leave_type in (4,5,17) and ad.leave_time_type=1 then 0.5
                            when ad.leave_type in (4,5,17) and ad.leave_time_type=2 then 0.5
                            when ad.leave_type in (4,5,17) and ad.leave_time_type=3 then 1
                            else 0
                            end 产假
                        ,case 
                            when ad.leave_type=7 and ad.leave_time_type=1 then 0.5
                            when ad.leave_type=7 and ad.leave_time_type=2 then 0.5
                            when ad.leave_type=7 and ad.leave_time_type=3 then 1
                            else 0
                            end 丧假
                        ,case 
                            when ad.leave_type=10 and ad.leave_time_type=1 then 0.5
                            when ad.leave_type=10 and ad.leave_time_type=2 then 0.5
                            when ad.leave_type=10 and ad.leave_time_type=3 then 1
                            else 0
                            end 婚假
                        ,case 
                            when ad.leave_type=16 and ad.leave_time_type=1 then 0.5
                            when ad.leave_type=16 and ad.leave_time_type=2 then 0.5
                            when ad.leave_type=16 and ad.leave_time_type=3 then 1
                            else 0
                            end 公司培训假
                        ,case 
                            when ad.leave_type=19 and ad.leave_time_type=1 then 0.5
                            when ad.leave_type=19 and ad.leave_time_type=2 then 0.5
                            when ad.leave_type=19 and ad.leave_time_type=3 then 1
                            else 0
                            end 跨国探亲假
                    FROM `bi_pro`.`attendance_data_v2` ad
                    LEFT JOIN `fle_staging`.`staff_info` si on si.`id` =ad.`staff_info_id` 
                    LEFT JOIN `fle_staging`.`sys_store` ss on ss.`id` =si.`organization_id`
                    LEFT JOIN `bi_pro`.`hr_job_title` hjt on hjt.`id` =si.`job_title` 
                    -- LEFT JOIN `fle_staging`.`sys_department` sd on sd.`id` =si.`department_id` 
                    LEFT JOIN `dwm`.`dwd_hr_organizational_structure_detail` sd ON sd.`id`=si.`department_id`
                    WHERE sd.`一级部门`='Thailand Fulfillment'
                        AND ad.`stat_date`>= date_sub(date(now() + interval -1 hour),interval 30 day)
                ) ad 
            ) ad 
        ) ad 
    ) ad 
)
,
OT as
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
    and ho.date_at >= date_sub(date(now() + interval -1 hour),interval 30 day)
)
select 
    left(统计日期,10)日期
    ,仓库
    ,'在职人数' type
    ,sum(在职)num 
from 
dwm.dwd_th_ffm_staff_dayV2
WHERE left(统计日期,10)>=left(NOW() - interval 7 day,10)
    and 仓库 is not null
GROUP BY 1,2,3

UNION
select 
    left(统计日期,10)
    ,仓库
    ,'应出勤' type
    ,sum(应出勤) 
from 
dwm.dwd_th_ffm_staff_dayV2
WHERE left(统计日期,10)>=left(NOW() - interval 7 day,10)
    and 仓库 is not null
    and 在职=1
GROUP BY 1,2,3

union 
select 
    left(统计日期,10)
    ,仓库
    ,'实际出勤' type
    ,sum(出勤) 
from 
dwm.dwd_th_ffm_staff_dayV2
WHERE left(统计日期,10)>=left(NOW() - interval 7 day,10)
    and 仓库 is not null
    and 在职=1
GROUP BY 1,2,3
 
UNION 
select 
    left(统计日期,10)
    ,仓库
    ,'出勤率(出勤/在职)' type
    ,sum(出勤)/sum(在职) 
from 
dwm.dwd_th_ffm_staff_dayV2
WHERE left(统计日期,10)>=left(NOW() - interval 7 day,10)
    and 仓库 is not null
    and 在职=1
GROUP BY 1,2,3

UNION 
select 
    left(统计日期,10)
    ,仓库
    ,'出勤率(出勤/应出勤)' type
    ,sum(出勤)/sum(应出勤) 
from 
dwm.dwd_th_ffm_staff_dayV2
WHERE left(统计日期,10)>=left(NOW() - interval 7 day,10)
and 仓库 is not null
and 在职=1
GROUP BY 1,2,3

union 
select 
        left(dt,10) 日期
        ,warehouse 仓库
        ,'临时工工时' type
        ,sum(num_people*8+num_ot) t_临时工时
    FROM dwm.th_ffm_tempworker_input WHERE left(dt,10)>=left(NOW() - interval 7 day,10)
        and warehouse is not null
    GROUP BY 1,2,3

union 
select 
    left(申请日期,10)
    ,仓库
    ,'OT工时' type
    ,nvl(sum(加班时长),0) 
from  
OT
WHERE left(申请日期,10)>=left(NOW() - interval 7 day,10)
    and 仓库 is not null
GROUP BY 1,2,3
ORDER BY 1,2,3
;

########################################################################################### 修改一版 ###########################################################################################

with a as 
(
    SELECT 
        人员信息
        ,统计日期
        ,一级部门
        ,部门
        ,仓库
        ,在职
        ,职位类别
        ,职位
        ,上班打卡时间
        ,班次开始
        ,下班打卡时间
        ,班次结束
        ,公休日
        ,休息日
        ,if(出勤=1,1,应出勤) 应出勤
        ,出勤
    --    ,应出勤-请假-旷工 出勤
        -- ,未出勤
        ,请假
        -- ,请假时段
        ,if(迟到>0,floor(迟到),0) 迟到
        ,旷工
        ,年假
        ,事假
        ,病假
        ,产假
        ,丧假
        ,婚假
        ,公司培训假
        ,跨国探亲假
        ,旷工最晚时间
        ,now() + interval -1 hour update_time
    FROM
    (
        SELECT
            人员信息
            ,统计日期
            ,一级部门
            ,部门
            ,仓库     
            ,职位类别
            ,职位
            ,在职
            ,上班打卡时间
            ,班次开始
            ,下班打卡时间
            ,班次结束
            ,公休日
            ,休息日
            ,应出勤
            ,出勤
            -- ,未出勤
            ,请假
            ,请假时段
            ,case when 应出勤=1 AND (上班打卡时间 is null or 下班打卡时间 is null) AND 请假=0 then 0 
                when 应出勤=1 AND (上班打卡时间 is null or 下班打卡时间 is null) AND 请假=0.5 then 0
                when 应出勤=1 AND 请假=1 then 0 
                when 应出勤=1 AND 请假=0.5 AND 请假时段='上午半天假' AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(旷工最晚时间))/60<=120 then (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(旷工最晚时间))/60 
                when 应出勤=1 AND 请假=0.5 AND 请假时段='上午半天假' AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(旷工最晚时间))/60>120 then 0
                when 应出勤=1 AND 请假=0.5 AND 请假时段='下午半天假' AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60<=120 then (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60 
                when 应出勤=1 AND 请假=0.5 AND 请假时段='下午半天假' AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60>120 then 0
                when 应出勤=1 AND 请假=0 AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60<=120 then (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60
                when 应出勤=1 AND 请假=0 AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60>120 then 0
                when 应出勤=1 AND 请假=0 AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(旷工最晚时间))/60>0 then 0
                else 0
                end 迟到
            -- 当天没有上班卡或者下班卡为全天旷工；迟到120分钟内，算作迟到，5泰铢/分钟罚款；迟到超过120分钟，算半天旷工；迟到超过13:00或19:00，算全天旷工
            ,case when 应出勤=1 AND (上班打卡时间 is null or 下班打卡时间 is null) AND 请假=0 then 1 
                when 应出勤=1 AND (上班打卡时间 is null or 下班打卡时间 is null) AND 请假=0.5 then 0.5
                when 应出勤=1 AND 请假=1 then 0 
                when 应出勤=1 AND 请假=0.5 AND 请假时段='上午半天假' AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(旷工最晚时间))/60<=120 then 0 
                when 应出勤=1 AND 请假=0.5 AND 请假时段='上午半天假' AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(旷工最晚时间))/60>120 then 0.5
                when 应出勤=1 AND 请假=0.5 AND 请假时段='下午半天假' AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60<=120 then 0 
                when 应出勤=1 AND 请假=0.5 AND 请假时段='下午半天假' AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60>120 then 0.5
                when 应出勤=1 AND 请假=0 AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60<=120 then 0
                when 应出勤=1 AND 请假=0 AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60>120 then 0.5
                when 应出勤=1 AND 请假=0 AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(旷工最晚时间))/60>0 then 1
                else 0
                end 旷工
            ,年假
            ,事假
            ,病假
            ,产假
            ,丧假
            ,婚假
            ,公司培训假
            ,跨国探亲假
            ,旷工最晚时间
        FROM
        (
            SELECT 
                人员信息
                ,统计日期
                ,一级部门
                ,部门
                ,仓库
                ,职位类别
                ,职位
                ,在职
                ,上班打卡时间
                ,班次开始
                ,下班打卡时间
                ,班次结束
                ,公休日
                ,休息日
                ,应出勤
                ,出勤
                ,未出勤
                ,请假
                ,请假时段
                ,年假
                ,事假
                ,病假
                ,产假
                ,丧假
                ,婚假
                ,公司培训假
                ,跨国探亲假
                ,concat(统计日期,' ',旷工最晚时间,':','00' ) 旷工最晚时间
            FROM 
            (
                SELECT 
                    人员信息
                    ,统计日期
                    ,一级部门
                    ,部门
                    ,仓库
                    ,职位类别
                    ,职位
                    ,在职
                    ,上班打卡时间
                    ,班次开始
                    ,下班打卡时间
                    ,班次结束
                    ,公休日
                    ,休息日
                    ,应出勤
                    ,出勤
                    ,未出勤
                    ,请假
                    ,请假时段
                    ,年假
                    ,事假
                    ,病假
                    ,产假
                    ,丧假
                    ,婚假
                    ,公司培训假
                    ,跨国探亲假
                    ,if(shift_start<'12:00','13:00','19:00') 旷工最晚时间
                FROM 
                (
                    SELECT 
                        ad.`staff_info_id` 人员信息
                        ,ad.`stat_date` 统计日期
                        ,sd.`一级部门` 一级部门
                        ,sd.`二级部门` 二级部门
                        ,sd.`name` 部门
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
                        ,hjt.`job_name` 职位
                        ,case when ad.state='1' then 1 else 0 end  在职   
                        ,ad.`shift_start`
                        ,ad.`shift_end`
                        ,ad.`attendance_started_at` as 上班打卡时间
                        ,if(ad.`shift_start`<>'',concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ),null) as 班次开始
                        ,ad.`attendance_end_at` as 下班打卡时间
                        ,if(ad.`shift_end`<>'',concat(ad.`stat_date`,' ',ad.`shift_end`,':','00' ),null) as 班次结束
                        -- ,if
                        -- ,UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP((ad.`shift_start`<>'',concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ),null) )
                        -- ,ad.leave_type
                        ,case ad.leave_time_type when 1 then '上午半天'
                            when 2 then '下午半天'
                            when 3 then '全天'
                            end 请假时段
                        ,ad.attendance_time
                        -- ,ad.`AB`
                        ,if(ad.PH!=0,1,0) 公休日
                        ,if(ad.OFF!=0,1,0) 休息日
                        ,if(ad.PH=0 AND ad.OFF=0,1,0) 应出勤
                        ,CASE WHEN  ad.`attendance_started_at` IS NOT NULL THEN 1 ELSE 0 END 出勤
                        -- ,if(ad.PH=0 AND ad.OFF=0 AND ad.attendance_time=0,1,0) 未出勤天数
                        ,case when ad.PH=0 AND ad.OFF=0 AND ad.attendance_time=0 then 1 
                            when ad.PH=0 AND ad.OFF=0 AND ad.attendance_time=5 then 0.5 
                            when ad.PH=0 AND ad.OFF=0 AND ad.attendance_time=10 then 0
                            end 未出勤
                        ,ad.`AB`
                        -- ,UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' )) 迟到分钟
                        -- ,case when ad.PH=0 AND ad.OFF=0 
                        --      AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))>0
                        --      AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))<=30 then UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))
                        --      else 0
                        --      end 迟到分钟
                        -- ,UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_end`,':','00' ))-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' )) 班次时长
                        -- 当天没有上班卡或者下班卡为全天旷工；迟到30分钟内，算作迟到，5泰铢/分钟罚款；迟到超过30分钟，算半天旷工；迟到超过4小时，算全天旷工
                        ,case when ad.PH=0 AND ad.OFF=0 
                                AND (ad.`attendance_started_at` is null or ad.`attendance_end_at` is null) 
                                AND ad.leave_type not in (1,2,12,3,18,4,5,17,7,10,16,19) then 1 
                            when ad.PH=0 AND ad.OFF=0 
                                AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))>0
                                AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))<=30  then 0
                            -- when ad.PH=0 AND ad.OFF=0 
                            --  AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))>30
                            --  AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))<=240  then 0.5
                            when ad.PH=0 AND ad.OFF=0 
                                AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))>30
                                AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))<=(UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_end`,':','00' ))-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' )))/2  then 0.5
                            -- when ad.PH=0 AND ad.OFF=0 
                            --  AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))>240 then 1
                            when ad.PH=0 AND ad.OFF=0 
                                AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))>(UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_end`,':','00' ))-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' )))/2 then 1
                            else 0
                            end 旷工
                        ,case when ad.leave_type in (1,2,12,3,18,4,5,17,7,10,16,19) AND ad.leave_time_type=1 then 0.5
                            when ad.leave_type in (1,2,12,3,18,4,5,17,7,10,16,19) AND ad.leave_time_type=2 then 0.5
                            when ad.leave_type in (1,2,12,3,18,4,5,17,7,10,16,19) AND ad.leave_time_type=3 then 1 
                            else 0 
                            end 请假
                        ,case 
                            when ad.leave_type=1 and ad.leave_time_type=1 then 0.5
                            when ad.leave_type=1 and ad.leave_time_type=2 then 0.5
                            when ad.leave_type=1 and ad.leave_time_type=3 then 1
                            else 0
                            end 年假
                        ,case 
                            -- 带薪,不带薪事假
                            when ad.leave_type in (2,12) and ad.leave_time_type=1 then 0.5
                            when ad.leave_type in (2,12) and ad.leave_time_type=2 then 0.5
                            when ad.leave_type in (2,12) and ad.leave_time_type=3 then 1
                            else 0
                            end 事假
                        ,case 
                            -- 带薪,不带薪病假
                            when ad.leave_type in (3,18) and ad.leave_time_type=1 then 0.5
                            when ad.leave_type in (3,18) and ad.leave_time_type=2 then 0.5
                            when ad.leave_type in (3,18) and ad.leave_time_type=3 then 1
                            else 0
                            end 病假
                        ,case 
                            -- 产假,陪产假,产检
                            when ad.leave_type in (4,5,17) and ad.leave_time_type=1 then 0.5
                            when ad.leave_type in (4,5,17) and ad.leave_time_type=2 then 0.5
                            when ad.leave_type in (4,5,17) and ad.leave_time_type=3 then 1
                            else 0
                            end 产假
                        ,case 
                            when ad.leave_type=7 and ad.leave_time_type=1 then 0.5
                            when ad.leave_type=7 and ad.leave_time_type=2 then 0.5
                            when ad.leave_type=7 and ad.leave_time_type=3 then 1
                            else 0
                            end 丧假
                        ,case 
                            when ad.leave_type=10 and ad.leave_time_type=1 then 0.5
                            when ad.leave_type=10 and ad.leave_time_type=2 then 0.5
                            when ad.leave_type=10 and ad.leave_time_type=3 then 1
                            else 0
                            end 婚假
                        ,case 
                            when ad.leave_type=16 and ad.leave_time_type=1 then 0.5
                            when ad.leave_type=16 and ad.leave_time_type=2 then 0.5
                            when ad.leave_type=16 and ad.leave_time_type=3 then 1
                            else 0
                            end 公司培训假
                        ,case 
                            when ad.leave_type=19 and ad.leave_time_type=1 then 0.5
                            when ad.leave_type=19 and ad.leave_time_type=2 then 0.5
                            when ad.leave_type=19 and ad.leave_time_type=3 then 1
                            else 0
                            end 跨国探亲假
                    FROM `bi_pro`.`attendance_data_v2` ad
                    LEFT JOIN `fle_staging`.`staff_info` si on si.`id` =ad.`staff_info_id` 
                    LEFT JOIN `fle_staging`.`sys_store` ss on ss.`id` =si.`organization_id`
                    LEFT JOIN `bi_pro`.`hr_job_title` hjt on hjt.`id` =si.`job_title` 
                    -- LEFT JOIN `fle_staging`.`sys_department` sd on sd.`id` =si.`department_id` 
                    LEFT JOIN `dwm`.`dwd_hr_organizational_structure_detail` sd ON sd.`id`=si.`department_id`
                    WHERE sd.`一级部门`='Thailand Fulfillment'
                        AND ad.`stat_date`>= date_sub(date(now() + interval -1 hour),interval 30 day)
                ) ad 
                where 仓库 is not null -- 仓库名称不为空
            ) ad 
        ) ad 
    ) ad 
)
,
OT as
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
    and ho.date_at >= date_sub(date(now() + interval -1 hour),interval 30 day)
)
select
    '人力' date_source
    ,t0.日期
    ,t0.仓库
    ,t0.在职人数
    ,t1.应出勤
    ,t2.实际出勤
    ,round(t2.实际出勤/t0.在职人数, 4) 实际在职出勤率
    ,round(t2.实际出勤/t1.应出勤,4)  实际应出勤率
    ,t5.临时工工时
    ,t6.OT工时
from
(
    select 
        left(统计日期,10)日期
        ,仓库
        ,'在职人数' type
        ,sum(在职) 在职人数
    from 
    A
    WHERE 1=1
        and left(统计日期,10)>=left(NOW() - interval 7 day,10) -- 拉取近七天的数据
    GROUP BY 1,2,3
) t0
left join
(
    select 
        left(统计日期,10) 日期
        ,仓库
        ,'应出勤' type
        ,sum(应出勤) 应出勤
    from 
    A
    WHERE left(统计日期,10)>=left(NOW() - interval 7 day,10)
        and 在职=1
    GROUP BY 1,2,3
) t1 on t0.日期 = t1.日期 and t0.仓库 = t1.仓库
left join
(
    select 
        left(统计日期,10) 日期
        ,仓库
        ,'实际出勤' type
        ,sum(出勤)  实际出勤
    from 
    A
    WHERE left(统计日期,10)>=left(NOW() - interval 7 day,10)
        and 仓库 is not null
        and 在职=1
    GROUP BY 1,2,3
) t2 on t0.日期 = t2.日期 and t0.仓库 = t2.仓库
-- left join
-- (
--     select 
--         left(统计日期,10) 日期
--         ,仓库
--         ,'出勤率(出勤/在职)' type
--         ,sum(出勤)/sum(在职) 
--     from 
--     A
--     WHERE left(统计日期,10)>=left(NOW() - interval 7 day,10)
--         and 仓库 is not null
--         and 在职=1
--     GROUP BY 1,2,3
-- ) t3 on t0.日期 = t3.日期 and t0.仓库 = t3.仓库
-- left join
-- (
--     select 
--         left(统计日期,10) 日期
--         ,仓库
--         ,'出勤率(出勤/应出勤)' type
--         ,sum(出勤)/sum(应出勤) 
--     from 
--     A
--     WHERE left(统计日期,10)>=left(NOW() - interval 7 day,10)
--     and 仓库 is not null
--     and 在职=1
--     GROUP BY 1,2,3
-- ) t4 on t0.日期 = t4.日期 and t0.仓库 = t4.仓库
left join
(
    select 
        left(dt,10) 日期
        ,warehouse 仓库
        ,'临时工工时' type
        ,sum(num_people*8+num_ot) 临时工工时
    FROM dwm.th_ffm_tempworker_input WHERE left(dt,10)>=left(NOW() - interval 7 day,10)
        and warehouse is not null
    GROUP BY 1,2,3
) t5 on t0.日期 = t5.日期 and t0.仓库 = t5.仓库
left join
(
    select 
        left(申请日期,10) 日期
        ,仓库
        ,'OT工时' type
        ,nvl(sum(加班时长),0) OT工时
    from  
    OT
    WHERE left(申请日期,10)>=left(NOW() - interval 7 day,10)
        and 仓库 is not null
    GROUP BY 1,2,3
) t6 on t0.日期 = t6.日期 and t0.仓库 = t6.仓库
ORDER BY 1,2,3
;

########################################################################################### 帆软report ###########################################################################################

with a as 
(
    SELECT 
        人员信息
        ,统计日期
        ,一级部门
        ,部门
        ,仓库
        ,在职
        ,职位类别
        ,职位
        ,上班打卡时间
        ,班次开始
        ,下班打卡时间
        ,班次结束
        ,公休日
        ,休息日
        ,应出勤
        ,出勤
    --    ,应出勤-请假-旷工 出勤
        -- ,未出勤
        ,请假
        -- ,请假时段
        ,if(迟到>0,floor(迟到),0) 迟到
        ,旷工
        ,年假
        ,事假
        ,病假
        ,产假
        ,丧假
        ,婚假
        ,公司培训假
        ,跨国探亲假
        ,旷工最晚时间
        ,now() + interval -1 hour update_time
    FROM
    (
        SELECT
            人员信息
            ,统计日期
            ,一级部门
            ,部门
            ,仓库     
            ,职位类别
            ,职位
            ,在职
            ,上班打卡时间
            ,班次开始
            ,下班打卡时间
            ,班次结束
            ,公休日
            ,休息日
            ,应出勤
            ,出勤
            -- ,未出勤
            ,请假
            ,请假时段
            ,case when 应出勤=1 AND (上班打卡时间 is null or 下班打卡时间 is null) AND 请假=0 then 0 
                when 应出勤=1 AND (上班打卡时间 is null or 下班打卡时间 is null) AND 请假=0.5 then 0
                when 应出勤=1 AND 请假=1 then 0 
                when 应出勤=1 AND 请假=0.5 AND 请假时段='上午半天假' AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(旷工最晚时间))/60<=120 then (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(旷工最晚时间))/60 
                when 应出勤=1 AND 请假=0.5 AND 请假时段='上午半天假' AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(旷工最晚时间))/60>120 then 0
                when 应出勤=1 AND 请假=0.5 AND 请假时段='下午半天假' AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60<=120 then (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60 
                when 应出勤=1 AND 请假=0.5 AND 请假时段='下午半天假' AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60>120 then 0
                when 应出勤=1 AND 请假=0 AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60<=120 then (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60
                when 应出勤=1 AND 请假=0 AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60>120 then 0
                when 应出勤=1 AND 请假=0 AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(旷工最晚时间))/60>0 then 0
                else 0
                end 迟到
            -- 当天没有上班卡或者下班卡为全天旷工；迟到120分钟内，算作迟到，5泰铢/分钟罚款；迟到超过120分钟，算半天旷工；迟到超过13:00或19:00，算全天旷工
            ,case when 应出勤=1 AND (上班打卡时间 is null or 下班打卡时间 is null) AND 请假=0 then 1 
                when 应出勤=1 AND (上班打卡时间 is null or 下班打卡时间 is null) AND 请假=0.5 then 0.5
                when 应出勤=1 AND 请假=1 then 0 
                when 应出勤=1 AND 请假=0.5 AND 请假时段='上午半天假' AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(旷工最晚时间))/60<=120 then 0 
                when 应出勤=1 AND 请假=0.5 AND 请假时段='上午半天假' AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(旷工最晚时间))/60>120 then 0.5
                when 应出勤=1 AND 请假=0.5 AND 请假时段='下午半天假' AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60<=120 then 0 
                when 应出勤=1 AND 请假=0.5 AND 请假时段='下午半天假' AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60>120 then 0.5
                when 应出勤=1 AND 请假=0 AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60<=120 then 0
                when 应出勤=1 AND 请假=0 AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60>120 then 0.5
                when 应出勤=1 AND 请假=0 AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(旷工最晚时间))/60>0 then 1
                else 0
                end 旷工
            ,年假
            ,事假
            ,病假
            ,产假
            ,丧假
            ,婚假
            ,公司培训假
            ,跨国探亲假
            ,旷工最晚时间
        FROM
        (
            SELECT 
                人员信息
                ,统计日期
                ,一级部门
                ,部门
                ,仓库
                ,职位类别
                ,职位
                ,在职
                ,上班打卡时间
                ,班次开始
                ,下班打卡时间
                ,班次结束
                ,公休日
                ,休息日
                ,应出勤
                ,出勤
                ,未出勤
                ,请假
                ,请假时段
                ,年假
                ,事假
                ,病假
                ,产假
                ,丧假
                ,婚假
                ,公司培训假
                ,跨国探亲假
                ,concat(统计日期,' ',旷工最晚时间,':','00' ) 旷工最晚时间
            FROM 
            (
                SELECT 
                    人员信息
                    ,统计日期
                    ,一级部门
                    ,部门
                    ,仓库
                    ,职位类别
                    ,职位
                    ,在职
                    ,上班打卡时间
                    ,班次开始
                    ,下班打卡时间
                    ,班次结束
                    ,公休日
                    ,休息日
                    ,应出勤
                    ,出勤
                    ,未出勤
                    ,请假
                    ,请假时段
                    ,年假
                    ,事假
                    ,病假
                    ,产假
                    ,丧假
                    ,婚假
                    ,公司培训假
                    ,跨国探亲假
                    ,if(shift_start<'12:00','13:00','19:00') 旷工最晚时间
                FROM 
                (
                    SELECT 
                        ad.`staff_info_id` 人员信息
                        ,ad.`stat_date` 统计日期
                        ,sd.`一级部门` 一级部门
                        ,sd.`二级部门` 二级部门
                        ,sd.`name` 部门
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
                        ,hjt.`job_name` 职位
                        ,case when ad.state='1' then 1 else 0 end  在职   
                        ,ad.`shift_start`
                        ,ad.`shift_end`
                        ,ad.`attendance_started_at` as 上班打卡时间
                        ,if(ad.`shift_start`<>'',concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ),null) as 班次开始
                        ,ad.`attendance_end_at` as 下班打卡时间
                        ,if(ad.`shift_end`<>'',concat(ad.`stat_date`,' ',ad.`shift_end`,':','00' ),null) as 班次结束
                        -- ,if
                        -- ,UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP((ad.`shift_start`<>'',concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ),null) )
                        -- ,ad.leave_type
                        ,case ad.leave_time_type when 1 then '上午半天'
                            when 2 then '下午半天'
                            when 3 then '全天'
                            end 请假时段
                        ,ad.attendance_time
                        -- ,ad.`AB`
                        ,if(ad.PH!=0,1,0) 公休日
                        ,if(ad.OFF!=0,1,0) 休息日
                        ,if(ad.PH=0 AND ad.OFF=0,1,0) 应出勤
                        ,CASE WHEN  ad.`attendance_started_at` IS NOT NULL THEN 1 ELSE 0 END 出勤
                        -- ,if(ad.PH=0 AND ad.OFF=0 AND ad.attendance_time=0,1,0) 未出勤天数
                        ,case when ad.PH=0 AND ad.OFF=0 AND ad.attendance_time=0 then 1 
                            when ad.PH=0 AND ad.OFF=0 AND ad.attendance_time=5 then 0.5 
                            when ad.PH=0 AND ad.OFF=0 AND ad.attendance_time=10 then 0
                            end 未出勤
                        ,ad.`AB`
                        -- ,UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' )) 迟到分钟
                        -- ,case when ad.PH=0 AND ad.OFF=0 
                        --      AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))>0
                        --      AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))<=30 then UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))
                        --      else 0
                        --      end 迟到分钟
                        -- ,UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_end`,':','00' ))-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' )) 班次时长
                        -- 当天没有上班卡或者下班卡为全天旷工；迟到30分钟内，算作迟到，5泰铢/分钟罚款；迟到超过30分钟，算半天旷工；迟到超过4小时，算全天旷工
                        ,case when ad.PH=0 AND ad.OFF=0 
                                AND (ad.`attendance_started_at` is null or ad.`attendance_end_at` is null) 
                                AND ad.leave_type not in (1,2,12,3,18,4,5,17,7,10,16,19) then 1 
                            when ad.PH=0 AND ad.OFF=0 
                                AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))>0
                                AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))<=30  then 0
                            -- when ad.PH=0 AND ad.OFF=0 
                            --  AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))>30
                            --  AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))<=240  then 0.5
                            when ad.PH=0 AND ad.OFF=0 
                                AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))>30
                                AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))<=(UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_end`,':','00' ))-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' )))/2  then 0.5
                            -- when ad.PH=0 AND ad.OFF=0 
                            --  AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))>240 then 1
                            when ad.PH=0 AND ad.OFF=0 
                                AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))>(UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_end`,':','00' ))-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' )))/2 then 1
                            else 0
                            end 旷工
                        ,case when ad.leave_type in (1,2,12,3,18,4,5,17,7,10,16,19) AND ad.leave_time_type=1 then 0.5
                            when ad.leave_type in (1,2,12,3,18,4,5,17,7,10,16,19) AND ad.leave_time_type=2 then 0.5
                            when ad.leave_type in (1,2,12,3,18,4,5,17,7,10,16,19) AND ad.leave_time_type=3 then 1 
                            else 0 
                            end 请假
                        ,case 
                            when ad.leave_type=1 and ad.leave_time_type=1 then 0.5
                            when ad.leave_type=1 and ad.leave_time_type=2 then 0.5
                            when ad.leave_type=1 and ad.leave_time_type=3 then 1
                            else 0
                            end 年假
                        ,case 
                            -- 带薪,不带薪事假
                            when ad.leave_type in (2,12) and ad.leave_time_type=1 then 0.5
                            when ad.leave_type in (2,12) and ad.leave_time_type=2 then 0.5
                            when ad.leave_type in (2,12) and ad.leave_time_type=3 then 1
                            else 0
                            end 事假
                        ,case 
                            -- 带薪,不带薪病假
                            when ad.leave_type in (3,18) and ad.leave_time_type=1 then 0.5
                            when ad.leave_type in (3,18) and ad.leave_time_type=2 then 0.5
                            when ad.leave_type in (3,18) and ad.leave_time_type=3 then 1
                            else 0
                            end 病假
                        ,case 
                            -- 产假,陪产假,产检
                            when ad.leave_type in (4,5,17) and ad.leave_time_type=1 then 0.5
                            when ad.leave_type in (4,5,17) and ad.leave_time_type=2 then 0.5
                            when ad.leave_type in (4,5,17) and ad.leave_time_type=3 then 1
                            else 0
                            end 产假
                        ,case 
                            when ad.leave_type=7 and ad.leave_time_type=1 then 0.5
                            when ad.leave_type=7 and ad.leave_time_type=2 then 0.5
                            when ad.leave_type=7 and ad.leave_time_type=3 then 1
                            else 0
                            end 丧假
                        ,case 
                            when ad.leave_type=10 and ad.leave_time_type=1 then 0.5
                            when ad.leave_type=10 and ad.leave_time_type=2 then 0.5
                            when ad.leave_type=10 and ad.leave_time_type=3 then 1
                            else 0
                            end 婚假
                        ,case 
                            when ad.leave_type=16 and ad.leave_time_type=1 then 0.5
                            when ad.leave_type=16 and ad.leave_time_type=2 then 0.5
                            when ad.leave_type=16 and ad.leave_time_type=3 then 1
                            else 0
                            end 公司培训假
                        ,case 
                            when ad.leave_type=19 and ad.leave_time_type=1 then 0.5
                            when ad.leave_type=19 and ad.leave_time_type=2 then 0.5
                            when ad.leave_type=19 and ad.leave_time_type=3 then 1
                            else 0
                            end 跨国探亲假
                    FROM `bi_pro`.`attendance_data_v2` ad
                    LEFT JOIN `fle_staging`.`staff_info` si on si.`id` =ad.`staff_info_id` 
                    LEFT JOIN `fle_staging`.`sys_store` ss on ss.`id` =si.`organization_id`
                    LEFT JOIN `bi_pro`.`hr_job_title` hjt on hjt.`id` =si.`job_title` 
                    -- LEFT JOIN `fle_staging`.`sys_department` sd on sd.`id` =si.`department_id` 
                    LEFT JOIN `dwm`.`dwd_hr_organizational_structure_detail` sd ON sd.`id`=si.`department_id`
                    WHERE sd.`一级部门`='Thailand Fulfillment'
                        AND ad.`stat_date`>= date_sub(date(now() + interval -1 hour),interval 30 day)
                ) ad 
            ) ad 
        ) ad 
    ) ad 
)
,
OT as
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
        and 二级部门='AGV Warehouse'
        and ho.date_at >= date_sub(date(now() + interval -1 hour),interval 30 day)
    )
select
    t0.日期
    ,t0.仓库
    ,t0.t_在职人数
    ,t1.t_应出勤
    ,t2.t_实际出勤
    ,t3.t_出勤率在职
    ,t4.t_出勤率应出勤
    ,t5.t_临时工时
    ,t6.t_OT工时
from
(
    select 
        left(统计日期,10)日期
        ,仓库
        ,'在职人数' 在职人数
        ,sum(在职) t_在职人数
    from 
    A
    WHERE left(统计日期,10)>=left(NOW() - interval 7 day,10)
        and 仓库 is not null
    GROUP BY 1,2,3
) t0
left join
(
    select 
        left(统计日期,10) 日期
        ,仓库
        ,'应出勤' 应出勤
        ,sum(应出勤) t_应出勤
    from 
    A
    WHERE left(统计日期,10)>=left(NOW() - interval 7 day,10)
        and 仓库 is not null
    GROUP BY 1,2,3
) t1 on t0.日期 = t1.日期 and t0.仓库 = t1.仓库
left join 
(
    select 
        left(统计日期,10) 日期
        ,仓库
        ,'实际出勤' type
        ,sum(出勤) t_实际出勤
    from 
    A
    WHERE left(统计日期,10)>=left(NOW() - interval 7 day,10)
        and 仓库 is not null
    GROUP BY 1,2,3
) t2 on t0.日期 = t2.日期 and t0.仓库 = t2.仓库
left join
(
    select 
        left(统计日期,10) 日期
        ,仓库
        ,'出勤率(出勤/在职)' type
        ,sum(出勤)/sum(在职) t_出勤率在职
    from 
    A
    WHERE left(统计日期,10)>=left(NOW() - interval 7 day,10)
        and 仓库 is not null
    GROUP BY 1,2,3
) t3 on t0.日期 = t3.日期 and t0.仓库 = t3.仓库
left join
(
    select 
        left(统计日期,10) 日期
        ,仓库
        ,'出勤率(出勤/应出勤)' type
        ,sum(出勤)/sum(应出勤) t_出勤率应出勤
    from 
    A
    WHERE left(统计日期,10)>=left(NOW() - interval 7 day,10)
    and 仓库 is not null
    GROUP BY 1,2,3
) t4 on t0.日期 = t4.日期 and t0.仓库 = t4.仓库
left join
(
    select 
        left(dt,10) 日期
        ,warehouse 仓库
        ,'临时工工时' type
        ,sum(num_people*8+num_ot) t_临时工时
    FROM dwm.th_ffm_tempworker_input WHERE left(dt,10)>=left(NOW() - interval 7 day,10)
        and warehouse is not null
    GROUP BY 1,2,3
) t5 on t0.日期 = t5.日期 and t0.仓库 = t5.仓库
left join
(
    select 
        left(申请日期,10) 日期
        ,仓库
        ,'OT工时' type
        ,nvl(sum(加班时长),0) t_OT工时
    from  
    OT
    WHERE left(申请日期,10)>=left(NOW() - interval 7 day,10)
        and 仓库 is not null
    GROUP BY 1,2,3
    ORDER BY 1,2,3
) t6 on t0.日期 = t5.日期 and t0.仓库 = t5.仓库
where t0.日期='${dt}'