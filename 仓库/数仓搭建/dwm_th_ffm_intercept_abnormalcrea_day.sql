/*=====================================================================+
表名称：  dwm_th_ffm_intercept_abnormalcrea_day
功能描述： 异常单/拦截单生成表（天粒度汇总）
                        
需求来源：
编写人员: 王昱棋
设计日期：2024/10/29
        修改日期: 
        修改人员:            
        修改原因: 
-----------------------------------------------------------------------
---存在问题：
-----------------------------------------------------------------------
+=====================================================================*/ 
      

-- 人力信息表
-- drop table if exists dwm.dwm_th_ffm_intercept_abnormalcrea_day;
-- create table dwm.dwm_th_ffm_intercept_abnormalcrea_day as
delete from dwm.dwm_th_ffm_intercept_abnormalcrea_day where created >= date_sub(date(now() + interval -1 hour),interval 90 day); -- 先删除数据
insert into dwm.dwm_th_ffm_intercept_abnormalcrea_day -- 再插入数据


select 
    LEFT(created_time, 10) created
    ,warehouse_name
    ,source 单据
    ,'拦截单' 指标
    ,count(intercept_sn) num 
from dwm.dwd_th_ffm_intercept_abnormal_day
where 1=1
    and SUBSTRING(source, -3)='拦截单'
    and created_time >= date_sub(date(now() + interval -1 hour),interval 90 day) -- 先删除数据
GROUP BY 1,2,3,4

union all

select 
    LEFT(created_time, 10) created
    ,warehouse_name
    ,'异常单' 单据
    ,'生成异常单量' 指标
    ,count(intercept_sn) num 
from dwm.dwd_th_ffm_intercept_abnormal_day
where 1=1
    and SUBSTRING(source, -3)='异常单'
    and created_time >= date_sub(date(now() + interval -1 hour),interval 90 day) -- 先删除数据
GROUP BY 1,2,3,4