/*=====================================================================+
表名称：  dwm_th_ffm_staffsalaryOT_day
功能描述： 人力成本 薪资+OT+提成 天粒度汇总
                        
需求来源：
编写人员: 王昱棋
设计日期：2024/10/28
        修改日期: 
        修改人员:            
        修改原因: 
-----------------------------------------------------------------------
---存在问题：
-----------------------------------------------------------------------
+=====================================================================*/ 
      

-- 人力信息表
-- drop table if exists dwm.dwm_th_ffm_staffsalaryOT_day;
-- create table dwm.dwm_th_ffm_staffsalaryOT_day as
delete from dwm.dwm_th_ffm_staffsalaryOT_day where 统计日期 >= date_sub(date(now() + interval -1 hour),interval 10 day); -- 先删除数据
insert into dwm.dwm_th_ffm_staffsalaryOT_day -- 再插入数据
-- 成本
select
    t0.统计日期
    ,t0.仓库
    ,'人力' type
    ,if(t0.人员信息 in ('606321' ,'661237','32310','672950','78178','636585'), '管理成本','人力成本') title
    ,sum(t0.salary_sum) salary_sum
    ,sum(t0.OT类型*t0.加班时长*t0.salary_sum_hour) OTcost_sum
    ,count(t0.人员信息)*fc.commission commission_sum
from
(
    select
        统计日期
        ,仓库
        ,人员信息
        ,case when fsd.job_title_grade_v2=0 and 出勤>0 then round(出勤*(ifnull(gs.daysalary,0) + ifnull(gs.housesubsidy,0) + ifnull(gs.mealsubsidy,0) + ifnull(gs.socialsecurity,0)/26 + ifnull(subsidy,0)/26) ,0)
            when fsd.job_title_grade_v2=0 and 出勤=0 then 0
            when 人员信息 in ('606321' ,'661237','32310','672950','78178','636585') then round(ifnull(gs.cnsalary/26,0)*应出勤成本, 0)
            else round((ifnull(gs.salary/26,0) + ifnull(gs.housesubsidy,0) + ifnull(gs.mealsubsidy,0) + ifnull(gs.socialsecurity,0)/26 + ifnull(subsidy,0)/26)*应出勤成本 ,0)
        end as salary_sum
        ,gs.daysalary
        ,gs.housesubsidy	
        ,gs.mealsubsidy
        ,gs.socialsecurity
        ,fjs.subsidy
        ,fsd.职位
        ,fsd.job_title_grade_v2
        ,round((ifnull(gs.daysalary,0) + ifnull(gs.housesubsidy,0) + ifnull(gs.mealsubsidy,0) + ifnull(gs.socialsecurity,0)/30 + ifnull(subsidy,0)/30)/8, 0) salary_sum_hour
        ,fsd.加班时长
        ,fsd.OT类型
    from
    dwm.dwd_th_ffm_staff_dayV2 fsd 
    left join tmpale.tmp_th_ffm_jobsubsidy fjs on fsd.职位 = fjs.job
    left join tmpale.tmp_th_ffm_gradesalary gs on concat('F',fsd.job_title_grade_v2) = gs.grade
    where fsd.在职=1
        and 统计日期 >= '2023-12-01'
    	and 统计日期 >= date_sub(date(now() + interval -1 hour),interval 10 day)
) t0
left join
tmpale.tmp_th_ffm_commission_1030 fc on t0.仓库 = fc.warehouse_name
where 仓库 in ('AGV', 'BPL-Return', 'BPL3', 'BST', 'LAS')
group by 1,2,3,4