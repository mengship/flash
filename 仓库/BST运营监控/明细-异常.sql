with err as
(
    select
        source
        ,warehouse_name
        ,intercept_sn
        ,created_time
        ,shelf_on_end_time
        ,shelf_on_end_deadline_time
        ,if(shelf_on_end_time is not null and (shelf_on_end_time<=shelf_on_end_deadline_time),1,0) 及时上架
        ,if(created_time is not null and (shelf_on_end_time is not null OR shelf_on_end_deadline_time < date_add(now(), interval -60 minute)),1,0) 应上架
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
                    ,case when w.name='AutoWarehouse' then 'AGV'
                        when w.name='BPL-Return Warehouse' then 'BPL-Return'
                        when w.name='BPL3-LIVESTREAM' then 'BPL3'
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
                    and ip.created >= convert_tz(date_sub(date(now() + interval -1 hour),interval 7 day), '+07:00', '+08:00')
                    and ip.created <= convert_tz(date_sub(date(now() + interval -1 hour),interval 0 day), '+07:00', '+08:00')

                union
                select 
                    'wms异常单' source
                    ,case when w.name='AutoWarehouse' then 'AGV'
                        when w.name='BPL-Return Warehouse' then 'BPL-Return'
                        when w.name='BPL3-LIVESTREAM' then 'BPL3'
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
                    and ao.create_time >= convert_tz(date_sub(date(now() + interval -1 hour),interval 7 day), '+07:00', '+08:00')
                    and ao.create_time <= convert_tz(date_sub(date(now() + interval -1 hour),interval 0 day), '+07:00', '+08:00')

                union
                select
                    'erp拦截单' source
                    ,case when w.name='AutoWarehouse' then 'AGV'
                        when w.name='BPL-Return Warehouse' then 'BPL-Return'
                        when w.name='BPL3-LIVESTREAM' then 'BPL3'
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
                    and io.created >= convert_tz(date_sub(date(now() + interval -1 hour),interval 7 day), '+07:00', '+08:00')
                    and io.created <= convert_tz(date_sub(date(now() + interval -1 hour),interval 0 day), '+07:00', '+08:00')
            ) do
            left join 
            -- 日历调整// created_mod是节假日顺延后首日,date1是节假日顺延后第二天
            dwm.dim_th_default_timeV2 calendar on calendar.created=do.created_date
            
        ) t0
    ) t1
)

select 
    LEFT(created_time, 10) created
    ,warehouse_name
    ,'拦截单' 单据
    ,'生成拦截单量' 指标
    ,count(intercept_sn) num 
from err
where 1=1
    and SUBSTRING(source, -3)='拦截单'
GROUP BY 1,2,3,4

UNION
    select 
    LEFT(shelf_on_end_time, 10) created
    ,warehouse_name
    ,'拦截单' 单据
    ,'完成拦截单量' 指标
    ,count(intercept_sn) num 
from err
where 1=1
    and SUBSTRING(source, -3)='拦截单'
    and shelf_on_end_time is not null
GROUP BY 1,2,3,4

UNION
    select 
    LEFT(shelf_on_end_deadline_time, 10) created
    ,warehouse_name
    ,'拦截单' 单据
    ,'及时上架' 指标
    ,count(intercept_sn) num 
from err
where 1=1
    and SUBSTRING(source, -3)='拦截单'
    and 应上架=1
    and 及时上架=1
GROUP BY 1,2,3,4

UNION
    select 
    LEFT(shelf_on_end_deadline_time, 10) created
    ,warehouse_name
    ,'拦截单' 单据
    ,'应上架' 指标
    ,count(intercept_sn) num 
from err
where 1=1
    and SUBSTRING(source, -3)='拦截单'
    and 应上架=1
    /* and 及时上架=1 */
GROUP BY 1,2,3,4

UNION
    select 
    LEFT(shelf_on_end_deadline_time, 10) created
    ,warehouse_name
    ,'拦截单' 单据
    ,'未及时完成' 指标
    ,count(intercept_sn) num 
from err
where 1=1
    and SUBSTRING(source, -3)='拦截单'
    and 应上架=1
    and 及时上架=0
GROUP BY 1,2,3,4

-- 异常单
union
select 
    LEFT(created_time, 10) created
    ,warehouse_name
    ,'异常单' 单据
    ,'生成异常单量' 指标
    ,count(intercept_sn) num 
from err
where 1=1
    and SUBSTRING(source, -3)='异常单'
GROUP BY 1,2,3,4

UNION
    select 
    LEFT(shelf_on_end_time, 10) created
    ,warehouse_name
    ,'异常单' 单据
    ,'完成异常单量' 指标
    ,count(intercept_sn) num 
from err
where 1=1
    and SUBSTRING(source, -3)='异常单'
    and shelf_on_end_time is not null
GROUP BY 1,2,3,4

UNION
    select 
    LEFT(shelf_on_end_deadline_time, 10) created
    ,warehouse_name
    ,'异常单' 单据
    ,'及时上架' 指标
    ,count(intercept_sn) num 
from err
where 1=1
    and SUBSTRING(source, -3)='异常单'
    and 应上架=1
    and 及时上架=1
GROUP BY 1,2,3,4

UNION
    select 
    LEFT(shelf_on_end_deadline_time, 10) created
    ,warehouse_name
    ,'异常单' 单据
    ,'应上架' 指标
    ,count(intercept_sn) num 
from err
where 1=1
    and SUBSTRING(source, -3)='异常单'
    and 应上架=1
    /* and 及时上架=1 */
GROUP BY 1,2,3,4

UNION
    select 
    LEFT(shelf_on_end_deadline_time, 10) created
    ,warehouse_name
    ,'异常单' 单据
    ,'未及时完成' 指标
    ,count(intercept_sn) num 
from err
where 1=1
    and SUBSTRING(source, -3)='异常单'
    and 应上架=1
    and 及时上架=0
GROUP BY 1,2,3,4

########################################################## 备份 ########################################################## 
SELECT 
    T1.created 时间
    ,case when w.name='AutoWarehouse' then 'AGV'
            when w.name='BPL-Return Warehouse' then 'BPL-Return'
            when w.name='BPL3-LIVESTREAM' then 'BPL3'
            when w.name='BangsaoThong' then 'BST'
            when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS'
            when w.name='LCP Warehouse' then 'LCP' end 仓库
    ,单据
    ,指标
    ,sum(num)数值
from
(
    -- 直播仓
    select 
        LEFT(created - INTERVAL 1 HOUR,10) created
        ,warehouse_id
        ,'拦截单' 单据
        ,'生成拦截单量' 指标 
        ,count(id)num 
    from erp_wms_prod.intercept_order 
    where status <>'1000'
        and created >= convert_tz(date_sub(date(now() + interval -1 hour),interval 30 day), '+07:00', '+08:00')
    GROUP BY 1,2,3,4

    UNION
    select 
        LEFT(complete_time,10) completed
        ,warehouse_id
        ,'拦截单' 单据
        ,'完成拦截单量' TYPE 
        ,count(id)num -- 及时上架
    from erp_wms_prod.intercept_order 
    where status <>'1000'
        and complete_time >= convert_tz(date_sub(date(now() + interval -1 hour),interval 30 day), '+07:00', '+08:00')
    GROUP BY 1,2,3,4

    union
    select 
        LEFT(created + INTERVAL 23 HOUR,10) deadline
        ,warehouse_id
        ,'拦截单' 单据
        ,'及时上架' TYPE 
        ,sum(IF(complete_time<(created + INTERVAL 23 HOUR ),1,0))num -- 及时上架
    from erp_wms_prod.intercept_order 
    where status <>'1000'
        and created >= convert_tz(date_sub(date(now() + interval -1 hour),interval 30 day), '+07:00', '+08:00')
    GROUP BY 1,2,3,4

    UNION
    select 
        LEFT(created + INTERVAL 23 HOUR,10) deadline
        ,warehouse_id
        ,'拦截单' 单据
        ,'应上架' TYPE 
        ,sum(if((complete_time is not null OR (created + INTERVAL 23 HOUR)< date_add(now(), interval -60 minute)),1,0)) -- 应上架
    from erp_wms_prod.intercept_order 
    where status <>'1000'
        and created >= convert_tz(date_sub(date(now() + interval -1 hour),interval 30 day), '+07:00', '+08:00')
    GROUP BY 1,2,3,4

    UNION
    select 
        LEFT(now() - INTERVAL 1 day,10) date
        ,warehouse_id
        ,'拦截单' 单据
        ,'超时未完结' TYPE 
        ,count(id)
    from erp_wms_prod.intercept_order 
    where status <>'1000'
        AND complete_time IS null
        and (created + INTERVAL 23 HOUR)< now() - INTERVAL 1 hour
    GROUP BY 1,2,3,4
) T1 
LEFT JOIN erp_wms_prod.warehouse w ON T1.warehouse_id=w.id
group by 1,2,3,4

union
select
    T1.created 时间
    ,case when w.name='AutoWarehouse' then 'AGV'
            when w.name='BPL-Return Warehouse' then 'BPL-Return'
            when w.name='BPL3-LIVESTREAM' then 'BPL3'
            when w.name='BangsaoThong' then 'BST'
            when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS'
            when w.name='LCP Warehouse' then 'LCP' end 仓库
    ,单据
    ,指标
    ,sum(num)数值
from
(
    -- wms 拦截单
    select 
        LEFT(created - INTERVAL 1 HOUR,10) created
        ,warehouse_id
        ,'拦截单' 单据
        ,'生成拦截单量' 指标 
        ,count(id)num 
    from wms_production.intercept_place 
    where status <>'1000'
        and LEFT(created - INTERVAL 1 HOUR,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now() - INTERVAL 1 day,10)
    GROUP BY 1,2,3,4

    UNION
    select 
        LEFT(shelf_on_end_time,10) completed
        ,warehouse_id
        ,'拦截单' 单据
        ,'完成拦截单量' TYPE 
        ,count(id)num -- 及时上架
    from wms_production.intercept_place 
    where status <>'1000'
        and LEFT(shelf_on_end_time,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now() - INTERVAL 1 day,10)
    GROUP BY 1,2,3,4

    union
    select 
        LEFT(created + INTERVAL 23 HOUR,10) deadline
        ,warehouse_id
        ,'拦截单' 单据
        ,'及时上架' TYPE 
        ,sum(IF(shelf_on_end_time<(created + INTERVAL 23 HOUR ),1,0))num -- 及时上架
    from wms_production.intercept_place 
    where status <>'1000'
        and LEFT(created + INTERVAL 23 HOUR,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now() - INTERVAL 1 day,10)
    GROUP BY 1,2,3,4

    UNION
    select 
        LEFT(created + INTERVAL 23 HOUR,10) deadline
        ,warehouse_id
        ,'拦截单' 单据
        ,'应上架' TYPE 
        ,sum(if((shelf_on_end_time is not null OR (created + INTERVAL 23 HOUR)< date_add(now(), interval -60 minute)),1,0)) -- 应上架
    from wms_production.intercept_place 
    where status <>'1000'
        and LEFT(created + INTERVAL 23 HOUR,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now() - INTERVAL 1 day,10) -- deadline 近7天
    GROUP BY 1,2,3,4


    UNION
    select 
        LEFT(now() - INTERVAL 1 day,10) date
        ,warehouse_id
        ,'拦截单' 单据
        ,'超时未完结' TYPE 
        ,count(id)
    from wms_production.intercept_place 
    where status <>'1000'
        AND shelf_on_end_time IS null
        and (created + INTERVAL 23 HOUR)< now() - INTERVAL 1 hour
    GROUP BY 1,2,3,4

    -- 异常单
    UNION
    select 
        LEFT(create_time - INTERVAL 1 HOUR,10) created
        ,warehouse_id
        ,'异常单' 单据
        ,'生成异常单量' TYPE 
        ,count(id)num 
    from wms_production.abnormal_order  
    where 1=1
        and LEFT(create_time - INTERVAL 1 HOUR,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now() - INTERVAL 1 day,10)
    GROUP BY 1,2,3,4

    UNION
    select 
        LEFT(finish_time,10) completed
        ,warehouse_id
        ,'异常单' 单据
        ,'完成异常单量' TYPE 
        ,count(id)num -- 及时上架
    from wms_production.abnormal_order 
    where 1=1
        and LEFT(finish_time ,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now() - INTERVAL 1 day,10)
    GROUP BY 1,2,3,4

    union
    select 
        LEFT(create_time + INTERVAL 23 HOUR,10) deadline
        ,warehouse_id
        ,'异常单' 单据
        ,'及时完成' TYPE 
        ,sum(IF(finish_time<(create_time + INTERVAL 23 HOUR ),1,0))num -- 及时上架
    from wms_production.abnormal_order 
    where 1=1
        and LEFT(create_time + INTERVAL 23 HOUR,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now() - INTERVAL 1 day,10)
    GROUP BY 1,2,3,4

    UNION
    select 
        LEFT(create_time + INTERVAL 23 HOUR,10) deadline
        ,warehouse_id
        ,'异常单' 单据
        ,'应完成' TYPE 
        ,sum(if((finish_time is not null OR (create_time + INTERVAL 23 HOUR)< date_add(now(), interval -60 minute)),1,0)) -- 应上架
    from wms_production.abnormal_order 
    where 1=1
        and LEFT(create_time + INTERVAL 23 HOUR,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now() - INTERVAL 1 day,10) -- deadline 近7天
    GROUP BY 1,2,3,4

    UNION
    select 
        LEFT(now() - INTERVAL 1 day,10) date
        ,warehouse_id
        ,'异常单' 单据
        ,'超时未完结' TYPE 
        ,count(id)
    from wms_production.abnormal_order 
    where 1=1
        AND finish_time IS null
        and (create_time + INTERVAL 23 HOUR)< now() - INTERVAL 1 hour
    GROUP BY 1,2,3,4

)T1 
LEFT JOIN wms_production.warehouse w ON T1.warehouse_id=w.id
group by 1,2,3,4


