/*=====================================================================+
表名称：  dwm_th_ffm_intercept_abnormaltimely_day
功能描述： 异常单/拦截单时效表（天粒度汇总）
                        
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

drop table if exists dwm.dwm_th_ffm_intercept_abnormaltimely_day;
create table dwm.dwm_th_ffm_intercept_abnormaltimely_day as
-- delete from dwm.dwm_th_ffm_intercept_abnormaltimely_day where shelf_on_end_date >= date_sub(date(now() + interval -1 hour),interval 90 day); -- 先删除数据
-- insert into dwm.dwm_th_ffm_intercept_abnormaltimely_day -- 再插入数据

select 
    LEFT(shelf_on_end_time, 10) shelf_on_end_date
    ,warehouse_name
    ,'拦截单' 单据
    ,'完成拦截单量' 指标
    ,count(if(shelf_on_end_time is not null, intercept_sn, null)) complete_num
    ,count(if(timelytype='及时', intercept_sn, null)) timely_num
    ,count(if(timelytype='不及时', intercept_sn, null)) notimely_num
    ,count(if(timelytype in ('及时', '不及时'), intercept_sn, null)) should_num
from dwm.dwd_th_ffm_intercept_abnormal_day
where 1=1
    and SUBSTRING(source, -3)='拦截单'
    -- and shelf_on_end_time >= date_sub(date(now() + interval -1 hour),interval 90 day)
GROUP BY 1,2,3,4

UNION ALL
    select 
    LEFT(shelf_on_end_time, 10) created
    ,warehouse_name
    ,'异常单' 单据
    ,'完成异常单量' 指标
    ,count(if(shelf_on_end_time is not null, intercept_sn, null)) complete_num
    ,count(if(timelytype='及时', intercept_sn, null)) timely_num
    ,count(if(timelytype='不及时', intercept_sn, null)) notimely_num
    ,count(if(timelytype in ('及时', '不及时'), intercept_sn, null)) should_num
from dwm.dwd_th_ffm_intercept_abnormal_day
where 1=1
    and SUBSTRING(source, -3)='异常单'
    -- and shelf_on_end_time >= date_sub(date(now() + interval -1 hour),interval 90 day)
GROUP BY 1,2,3,4