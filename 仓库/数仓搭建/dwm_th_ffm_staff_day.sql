/*=====================================================================+
表名称：  dwm_th_ffm_staff_day
功能描述： 人力信息表 天粒度汇总
                        
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
drop table if exists dwm.dwm_th_ffm_staff_day;
create table dwm.dwm_th_ffm_staff_day as
-- delete from dwm.dwm_th_ffm_staff_day where 日期 >= date_sub(date(now() + interval -1 hour),interval 90 day); -- 先删除数据
-- insert into dwm.dwm_th_ffm_staff_day -- 再插入数据
select
    left(stat_date,10) 日期
    ,warehouse_name
    ,'在职人数' type
    ,sum(is_on_job) 在职人数
    ,sum(required_attend_flag) 应出勤
    ,sum(attendance_days) 实际出勤
    ,sum(if(2Bor2C='B2B',is_on_job,0)) as B2B在职人数
    ,sum(if(2Bor2C='B2B',is_on_job,0)) as B2B实际出勤人数
    ,sum(ot_duration) as 加班时长
from
    dwm.dwd_th_ffm_staff_dayV3
where 1=1
    and warehouse_name in ('AGV', 'BPL-Return', 'BPL3', 'BST', 'LAS')
    and is_on_job = 1
    -- and left(stat_date,10) >= date_sub(date(now() + interval -1 hour),interval 90 day)
group by 1,2