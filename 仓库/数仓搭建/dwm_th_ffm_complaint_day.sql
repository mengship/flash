/*=====================================================================+
表名称：  dwm_th_ffm_complaint_day
功能描述： 客诉生成表（天粒度汇总）
                        
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

drop table if exists dwm.dwm_th_ffm_complaint_day;
create table dwm.dwm_th_ffm_complaint_day as
-- delete from dwm.dwm_th_ffm_complaint_day where 创建日期 >= date_sub(date(now() + interval -1 hour),interval 90 day); -- 先删除数据
-- insert into dwm.dwm_th_ffm_complaint_day -- 再插入数据
SELECT 
    创建日期
    ,仓库名称
    ,count(if(仓责判断='有责投诉', id, null)) 有责客诉量 
    ,count(if(仓责判断='无责投诉', id, null)) 无责客诉量 
    ,count(if(timely_type='及时', id, null)) 及时关闭工单
FROM 
    dwm.dwd_th_ffm_complaint_day 
WHERE 1=1
    and 仓库名称 IS NOT null
    -- and 创建日期 >= date_sub(date(now() + interval -1 hour),interval 90 day)
GROUP BY 1,2