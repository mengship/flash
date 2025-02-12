/*=====================================================================+
表名称：  dwd_th_ffm_complaint_day
功能描述： 客诉明细表
                        
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

drop table if exists dwm.dwd_th_ffm_complaint_day;
create table dwm.dwd_th_ffm_complaint_day as
-- delete from dwm.dwd_th_ffm_complaint_day where 创建日期 >= date_sub(date(now() + interval -1 hour),interval 90 day); -- 先删除数据
-- insert into dwm.dwd_th_ffm_complaint_day -- 再插入数据

select  
    wo.id id
    ,case when w.name in ('AutoWarehouse', 'AutoWarehouse-人工仓') then 'AGV'
        when w.name='BPL-Return Warehouse' then 'BPL-Return'
        when w.name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
        when w.name='BangsaoThong' then 'BST'
        when w.name IN ('BKK-WH-LAS2电商仓','PMD-WH','BKK-WH-Ecommerce','BKK-WH-LAS物料仓') then 'LAS'
        when w.name='LCP Warehouse' then 'LCP' 
        else w.name
        end 仓库名称
    ,LEFT(wo.created - interval 1 hour,10) 创建日期
    ,week(wo.created + interval 23 hour) 周
    ,case when RESULT =2 then '有责投诉' else '无责投诉' end 仓责判断
    ,complete_time
    ,wo.created
    ,IF(complete_time <= wo.created + INTERVAL 23 HOUR,1,0) 是否及时
    ,case when complete_time is not null and complete_time <= wo.created + INTERVAL 23 HOUR then '及时'
        when complete_time is null and date_add(now(), interval -60 minute) <= wo.created + INTERVAL 23 HOUR then '未到考核时间'
        when (complete_time is not null and complete_time > wo.created + INTERVAL 23 HOUR) or (complete_time is null and date_add(now(), interval -60 minute) > wo.created + INTERVAL 23 HOUR) then '不及时'
        end as timely_type
from  wms_production.work_order wo 
left join wms_production.warehouse w on w.id=WO.warehouse_id
LEFT JOIN wms_production.seller sl on wo.seller_id =sl.id 
left join wms_production.judgement jm on wo.id = jm.work_order_id 
where 1=1
and wo.created >= convert_tz('2023-12-01', '+07:00', '+08:00')
-- and wo.created >= convert_tz(date_sub(date(now() + interval -1 hour),interval 90 day), '+07:00', '+08:00')


