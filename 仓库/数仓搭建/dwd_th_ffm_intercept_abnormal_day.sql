/*=====================================================================+
表名称：  dwd_th_ffm_intercept_abnormal_day
功能描述： 异常单 拦截单 明细数据
                        
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
-- drop table if exists dwm.dwd_th_ffm_intercept_abnormal_day;
-- create table dwm.dwd_th_ffm_intercept_abnormal_day as
delete from dwm.dwd_th_ffm_intercept_abnormal_day where created_time >= date_sub(date(now() + interval -1 hour),interval 90 day); -- 先删除数据
insert into dwm.dwd_th_ffm_intercept_abnormal_day -- 再插入数据

-- 异常单与拦截单
select
    source
    ,warehouse_name
    ,intercept_sn
    ,created_time
    ,shelf_on_end_time
    ,shelf_on_end_deadline_time
    ,shelf_on_end_deadline_date
    ,timelytype
from
(
    select
        source
        ,warehouse_name
        ,intercept_sn
        ,created_time
        ,shelf_on_end_time
        ,shelf_on_end_deadline_time
        ,date(shelf_on_end_deadline_time) shelf_on_end_deadline_date
        ,case when shelf_on_end_time is not null and shelf_on_end_time <= shelf_on_end_deadline_time then '及时'
            when shelf_on_end_time is null and date_add(now(), interval -60 minute) <= shelf_on_end_deadline_time then '未到考核时间'
            when (shelf_on_end_time is null and date_add(now(), interval -60 minute) > shelf_on_end_deadline_time) or (shelf_on_end_time is not null and shelf_on_end_time > shelf_on_end_deadline_time) then '不及时'
            end as timelytype
        /* ,if(shelf_on_end_time is not null and (shelf_on_end_time<=shelf_on_end_deadline_time),'及时', '不及时') 及时上架
        ,if(created_time is not null and (shelf_on_end_time is not null OR shelf_on_end_deadline_time < date_add(now(), interval -60 minute)),1,0) 应上架 */
    from
    (
        select
            source
            ,warehouse_name
            ,intercept_sn
            ,created_date
            ,created_time
            ,shelf_on_end_date
            ,shelf_on_end_time
            -- ,concat(date1,substr(reg_time_mod,11,9)) shelf_on_end_deadline_time
            ,created
            ,if_day_off
            ,created_mod
            ,date1
            ,date2
            ,date3
            ,date4
            ,reg_time_mod
            -- ,if(shelf_on_end_time is not null and (shelf_on_end_time<=concat(date1,substr(reg_time_mod,11,9))),1,0) 及时上架
            -- ,if(created_time is not null and (shelf_on_end_time is not null OR concat(date1,substr(reg_time_mod,11,9))< date_add(now(), interval -60 minute)),1,0) 应上架
            ,CASE when substr(reg_time_mod,12,2)<16 THEN concat(LEFT(reg_time_mod,10),' 23:59:59')
                WHEN substr(reg_time_mod,12,2)>=16 THEN concat(date1,' 23:59:59')
                ELSE concat(date1,substr(reg_time_mod,11,9)) END shelf_on_end_deadline_time
        from
        (
            select
                do.source
                ,do.warehouse_name
                ,do.intercept_sn
                ,do.created_date
                ,do.created_time
                ,do.shelf_on_end_date
                ,do.shelf_on_end_time
                ,calendar.created
                ,calendar.if_day_off
                ,calendar.created_mod
                ,calendar.date1
                ,calendar.date2
                ,calendar.date3
                ,calendar.date4
                ,case when calendar.if_day_off='是' then concat(calendar.created_mod,' 00:00:00') else concat(calendar.created_mod,substr(do.created_time,11,9))end reg_time_mod
            from
            (
                -- wms 拦截单
                select
                    'wms拦截单' source
                    ,case when w.name in ('AutoWarehouse', 'AutoWarehouse-人工仓') then 'AGV'
                        when w.name='BPL-Return Warehouse' then 'BPL-Return'
                        when w.name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
                        when w.name='BangsaoThong' then 'BST'
                        when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS'
                        when w.name='LCP Warehouse' then 'LCP' end warehouse_name
                    ,ip.intercept_sn
                    ,LEFT(ip.created - INTERVAL 1 HOUR,10) created_date
                    ,ip.created - INTERVAL 1 HOUR created_time
                    ,LEFT(ip.shelf_on_end_time,10)  shelf_on_end_date
                    ,ip.shelf_on_end_time
                    from wms_production.intercept_place ip
                    LEFT JOIN wms_production.warehouse w ON ip.warehouse_id=w.id
                where 1=1
                    and ip.status <>'1000'
                    and ip.created >= convert_tz(date_sub(date(now() + interval -1 hour),interval 90 day), '+07:00', '+08:00')
                    and ip.created >= convert_tz('2023-12-01', '+07:00', '+08:00')
                
                union
                select 
                    'wms异常单' source
                    ,case when w.name in ('AutoWarehouse', 'AutoWarehouse-人工仓') then 'AGV'
                        when w.name='BPL-Return Warehouse' then 'BPL-Return'
                        when w.name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
                        when w.name='BangsaoThong' then 'BST'
                        when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS'
                        when w.name='LCP Warehouse' then 'LCP' end warehouse_name
                    ,ao.sn
                    ,LEFT(ao.create_time - INTERVAL 1 HOUR,10) created_date
                    ,ao.create_time - INTERVAL 1 HOUR created_time
                    ,date(ao.finish_time) finish_date
                    ,ao.finish_time
                from wms_production.abnormal_order ao
                LEFT JOIN wms_production.warehouse w ON ao.warehouse_id=w.id
                where 1=1
                    and ao.create_time >= convert_tz(date_sub(date(now() + interval -1 hour),interval 90 day), '+07:00', '+08:00')
                    and ao.create_time >= convert_tz('2023-12-01', '+07:00', '+08:00')
                
                union
                select
                    'erp拦截单' source
                    ,case when w.name in ('AutoWarehouse', 'AutoWarehouse-人工仓') then 'AGV'
                        when w.name='BPL-Return Warehouse' then 'BPL-Return'
                        when w.name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
                        when w.name='BangsaoThong' then 'BST'
                        when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS'
                        when w.name='LCP Warehouse' then 'LCP' end warehouse_name
                    ,io.intercept_sn
                    ,LEFT(io.created - INTERVAL 1 HOUR,10) created_date
                    ,io.created - INTERVAL 1 HOUR created_time
                    ,date(io.complete_time) complete_date
                    ,io.complete_time
                from erp_wms_prod.intercept_order io
                LEFT JOIN erp_wms_prod.warehouse w ON io.warehouse_id=w.id
                where io.status <>'1000'
                    and io.created >= convert_tz(date_sub(date(now() + interval -1 hour),interval 90 day), '+07:00', '+08:00')
                    and io.created >= convert_tz('2023-12-01', '+07:00', '+08:00')
            ) do
            left join 
            -- 日历调整// created_mod是节假日顺延后首日,date1是节假日顺延后第二天
            dwm.dim_th_default_timeV2 calendar on calendar.created=do.created_date
            
        ) t0
    ) t1
) t1
where 1=1