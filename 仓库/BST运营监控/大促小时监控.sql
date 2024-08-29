with EF as
(
    SELECT 
        *,
        IF(finish_delivery<deadline,1,0)及时发货
        ,if(audit_time is not null and (finish_delivery is not null OR deadline< date_add(now(), interval -60 minute)),1,0) 应发货
    FROM 
        (
            SELECT 
                TYPE,
                delivery_sn
                ,goods_num
                ,warehouse_id,
                case when warehouse_name='AutoWarehouse' then 'AGV'
                    when warehouse_name='BPL-Return Warehouse' then 'BPL-Return'
                    when warehouse_name='BPL3-LIVESTREAM' then 'BPL3'
                    when warehouse_name='BangsaoThong' then 'BST'
                    when warehouse_name IN ('BKK-WH-LAS2电商仓','PMD-WH','BKK-WH-Ecommerce','BKK-WH-LAS物料仓') then 'LAS'
                    when warehouse_name ='LCP Warehouse' then 'LCP' end warehouse_name
                ,CASE 
                    WHEN platform_source in('Shopee','Tik Tok','LAZADA')THEN platform_source 
                    ELSE 'Other' END platform_source
                ,seller_id
                ,seller_name
                ,created_time Created_Time
                ,created_date
                ,audit_time
                ,audit_date
                ,pack_time
                ,succ_pick
                ,start_receipt
                ,delivery_time
                ,finish_delivery
                ,created_time_mod
                ,CASE WHEN platform_source='Shopee' AND substr(created_time_mod,12,2)<16 THEN concat(LEFT(created_time_mod,10),' 20:00:00')
                WHEN platform_source='Shopee' AND substr(created_time_mod,12,2)>=16 THEN concat(date1,' 20:00:00')
                WHEN platform_source='Tik Tok' AND substr(created_time_mod,12,2)<18 THEN concat(LEFT(created_time_mod,10),' 20:00:00')
                WHEN platform_source='Tik Tok' AND substr(created_time_mod,12,2)>=18 THEN concat(date1,' 20:00:00')
                WHEN TYPE ='B2B' THEN concat(date2,substr(created_time_mod,11,9))
                ELSE concat(date1,substr(created_time_mod,11,9)) END deadline
                ,date_add(now(),interval -60 minute) ETL   
            FROM 
                (
                    SELECT 
                         do.TYPE
                        ,do.delivery_sn
                        ,do.goods_num 
                        ,do.warehouse_id
                        ,do.warehouse_name
                        ,do.platform_source
                        ,do.seller_name
                        ,do.seller_id
                        ,do.created_time
                        ,do.created_date
                        ,do.audit_time
                        ,do.audit_date
                        ,do.finish_delivery
                        ,do.delivery_time
                        ,do.start_receipt
                        ,do.pack_time
                        ,do.succ_pick
                        ,calendar.created
                        ,calendar.if_day_off
                        ,calendar.created_mod
                        ,calendar.date1
                        ,calendar.date2
                        ,calendar.date3
                        ,calendar.date4
                        ,case when calendar.if_day_off='是' then concat(calendar.created_mod,' 00:00:00') else concat(calendar.created_mod,substr(do.created_time,11,9))end created_time_mod 
                    FROM
                    (
                        SELECT 
                            'B2C' TYPE
                            ,do.`delivery_sn`
                            ,goods_num 
                            ,warehouse_id
                            ,w.name warehouse_name
                            ,ps.`name` platform_source
                            ,sl.`name` seller_name
                            ,do.seller_id
                            ,date_add(do.`created`, interval -60 minute) created_time
                            ,left(date_add(do.`created`, interval -60 minute), 10) created_date
                            ,date_add(do.`audit_time`, interval -60 minute) audit_time
                            ,left(date_add(do.`audit_time`, interval -60 minute), 10) audit_date
                            ,do.`start_receipt ` finish_delivery
                            ,delivery_time - INTERVAL 1 HOUR delivery_time
                            ,start_receipt
                            ,pack_time
                            ,succ_pick
                        FROM `wms_production`.`delivery_order` do 
                        LEFT JOIN `wms_production`.`seller_platform_source` sps on do.`platform_source_id`=sps.`id`
                        LEFT JOIN `wms_production`.`platform_source` ps on sps.`platform_source_id`=ps.`id` 
                        LEFT JOIN `wms_production`.`seller` sl on do.`seller_id`=sl.`id`
                        LEFT JOIN wms_production.warehouse w ON do.warehouse_id=w.id
                        WHERE date_add(do.`created`, interval -60 minute)>=DATE_ADD(CURRENT_DATE(),-10) 
                            AND do.`status` NOT IN ('1000','1010') -- 取消单
                            AND do.`platform_status`!=9
                            AND do.prompt NOT in (1,2,3,4)-- 剔除拦截

                        UNION
                        SELECT 
                            'B2C' TYPE
                            ,do.`delivery_sn`
                            ,goods_num 
                            ,warehouse_id
                            ,w.name warehouse_name
                            ,ps.`name` platform_source
                            ,sl.`name` seller_name
                            ,do.`seller_id`
                            ,date_add(do.`created`, interval -60 minute) created_time
                            ,left(date_add(do.`created`, interval -60 minute), 10) created_date
                            ,date_add(do.`created`, interval -60 minute) audit_time
                            ,left(date_add(do.`created`, interval -60 minute), 10) audit_date
                            ,do.`delivery_time `- INTERVAL 1 HOUR finish_delivery
                            ,delivery_time - INTERVAL 1 HOUR delivery_time
                            ,delivery_time - INTERVAL 1 HOUR  start_receipt
                            ,pack_time + INTERVAL 7 HOUR pack_time
                            ,succ_pick + INTERVAL 7 HOUR succ_pick
                        FROM `erp_wms_prod`.`delivery_order` do 
                        LEFT JOIN `erp_wms_prod`.`platform_source` ps on do.`platform_source_id`=ps.`id` 
                        LEFT JOIN `erp_wms_prod`.`seller` sl on do.`seller_id`=sl.`id`
                        LEFT JOIN erp_wms_prod.warehouse w ON do.warehouse_id=w.id
                        WHERE date_add(do.`created`, interval -60 minute)>=DATE_ADD(CURRENT_DATE(),-10) 
                        AND warehouse_id='312' 
                        and do.status <> 3030
                            -- AND do.`status` NOT IN ('1000','1010') -- 取消单
                        -- AND do.`platform_status`!=9
                            -- AND do.prompt NOT in (1,2,3,4)-- 剔除拦截

                    ) do 
                    
                    left join 
                    -- 日历调整// created_mod是节假日顺延后首日,date1是节假日顺延后第二天
                    (
                        select 
                            created
                            ,if_day_off
                            ,case when if_day_off ='是' then date else date0 end created_mod
                            ,case when if_day_off ='是' then date1 else date end date1
                            ,case when if_day_off ='是' then date2 else date1 end date2
                            ,case when if_day_off ='是' then date3 else date2 end date3
                            ,case when if_day_off ='是' then date4 else date3 end date4
                        from 
                            (
                                select 
                                    calendar.date created
                                    ,case when off_date is not null then '是' else '否' end if_day_off 
                                    ,date0
                                    ,workdate.date date
                                    ,date1
                                    ,date2
                                    ,date3
                                    ,date4
                                from 
                                -- 日历
                                    (select date from tmpale.ods_th_dim_date where date between '2023-01-01' and date_add(curdate(), interval 21 day))calendar 
                                left join
                                -- 假日表
                                    (select off_date from fle_staging.sys_holiday where deleted = 0 and company_category='2' and off_date>='2023-01-01' group by off_date)offdate on calendar.date=off_date
                                left join
                                -- 仓库工作日表（date为工作日，date0上一个工作日，date1为下一个工作日，date2为下下一个工作日...）
                                    (
                                        select 
                                            lag(date,1)over(order by date)date0
                                            ,date
                                            ,lead(date,1)over(order by date)date1
                                            ,lead(date,2)over(order by date)date2
                                            ,lead(date,3)over(order by date)date3
                                            ,lead(date,4)over(order by date)date4 
                                        from 
                                            (select date from tmpale.ods_th_dim_date where date between '2022-12-31' and date_add(curdate(), interval 28 day) ) d0
                                        left join 
                                            (select off_date from fle_staging.sys_holiday where deleted = 0 and company_category='2' and off_date>='2022-12-31' group by off_date) on date=off_date 
                                        where off_date is null
                                    )workdate on calendar.date>=workdate.date0 and calendar.date<workdate.date
                                where date0 is not null order by 1 desc
                            )
                    )   calendar on calendar.created=do.created_date
                )do
        )
)

SELECT warehouse_name,LEFT(created_time,10),HOUR(created_time),'流入' 指标,count(delivery_sn),sum(goods_num) FROM EF  
WHERE LEFT(created_time,10)=curdate() GROUP BY 1,2,3,4

union
SELECT warehouse_name,LEFT(created_time,10),HOUR(created_time),'未审核' 指标,count(delivery_sn),sum(goods_num) FROM EF  
WHERE LEFT(created_time,10)=curdate() AND audit_time IS NULL GROUP BY 1,2,3,4 

union
SELECT warehouse_name,LEFT(succ_pick,10),HOUR(succ_pick),'拣货完成' 指标,count(delivery_sn),sum(goods_num) FROM EF  
WHERE LEFT(succ_pick,10)=curdate() GROUP BY 1,2,3,4 

union
SELECT warehouse_name,LEFT(pack_time,10),HOUR(pack_time),'打包完成' 指标,count(delivery_sn),sum(goods_num) FROM EF  
WHERE LEFT(pack_time,10)=curdate() GROUP BY 1,2,3,4 

union
SELECT warehouse_name,LEFT(start_receipt,10),HOUR(start_receipt),'开始交接' 指标,count(delivery_sn),sum(goods_num) FROM EF  
WHERE LEFT(start_receipt,10)=curdate() GROUP BY 1,2,3,4 

union
SELECT warehouse_name,LEFT(delivery_time,10),HOUR(delivery_time),'出库' 指标,count(delivery_sn),sum(goods_num) FROM EF  
WHERE LEFT(delivery_time,10)=curdate() GROUP BY 1,2,3,4 

union
SELECT warehouse_name,curdate(),HOUR(now()),'积压（未打包完成）' 指标,count(delivery_sn),sum(goods_num) FROM EF  
WHERE  audit_time IS NOT NULL AND pack_time is NULL GROUP BY 1,2,3,4 

union
SELECT warehouse_name,curdate(),HOUR(now()),'昨日积压' 指标,count(delivery_sn),sum(goods_num) FROM EF  
WHERE  audit_time IS NOT NULL AND pack_time is NULL  AND LEFT(Created_Time,10)<left(now(),10) GROUP BY 1,2,3,4 

union
SELECT 
    case when w.name='AutoWarehouse' then 'AGV'
            when w.name='BPL-Return Warehouse' then 'BPL-Return'
            when w.name='BPL3-LIVESTREAM' then 'BPL3'
            when w.name='BangsaoThong' then 'BST'
            when w.name IN ('BKK-WH-LAS2电商仓','PMD-WH','BKK-WH-Ecommerce','BKK-WH-LAS物料仓')         then 'LAS'
            when w.name='LCP Warehouse' then 'LCP' end 仓库
    ,curdate() 日期
    ,created_hour
    -- ,单据
    ,指标
    ,sum(num) 数值
    ,null 默认
from
(
    select 
    hour(created - INTERVAL 1 HOUR) created_hour
    ,warehouse_id
    ,'拦截单' 单据
    ,'生成拦截单量' 指标 
    ,count(id)num 
    from wms_production.intercept_place 
    where status <>'1000'
    and LEFT(created - INTERVAL 1 HOUR,10) = date(now())
    GROUP BY 1,2,3,4

UNION
    select 
    hour(shelf_on_end_time) completed_hour
    ,warehouse_id
    ,'拦截单' 单据
    ,'完成拦截单量' TYPE 
    ,count(id)num -- 及时上架
    from wms_production.intercept_place 
    where status <>'1000'
    and LEFT(shelf_on_end_time,10) = date(now())
    GROUP BY 1,2,3,4

-- union
--     select 
--     LEFT(created + INTERVAL 23 HOUR,10) deadline
--     ,warehouse_id
--     ,'拦截单' 单据
--     ,'及时上架' TYPE 
--     ,sum(IF(shelf_on_end_time<(created + INTERVAL 23 HOUR ),1,0))num -- 及时上架
--     from wms_production.intercept_place 
--     where status <>'1000'
--     and LEFT(created + INTERVAL 23 HOUR,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now() - INTERVAL 1 day,10)
--     GROUP BY 1,2,3,4

-- UNION
--     select 
--     LEFT(created + INTERVAL 23 HOUR,10) deadline
--     ,warehouse_id
--     ,'拦截单' 单据
--     ,'应上架' TYPE 
--     ,sum(if((shelf_on_end_time is not null OR (created + INTERVAL 23 HOUR)< date_add(now(), interval -60 minute)),1,0)) -- 应上架
--     from wms_production.intercept_place 
--     where status <>'1000'
--     and LEFT(created + INTERVAL 23 HOUR,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now() - INTERVAL 1 day,10) -- deadline 近7天
--     GROUP BY 1,2,3,4

UNION
    select 
    hour(now()) date
    ,warehouse_id
    ,'拦截单' 单据
    ,'未完结拦截单量' TYPE 
    ,count(id)
    from wms_production.intercept_place 
    where status <>'1000'
    AND shelf_on_end_time IS null
    -- and (created + INTERVAL 23 HOUR)< now() - INTERVAL 1 hour
    GROUP BY 1,2,3,4

UNION
    select 
    hour(create_time - INTERVAL 1 HOUR) created_hour
    ,warehouse_id
    ,'异常单' 单据
    ,'生成异常单量' TYPE 
    ,count(id)num 
    from wms_production.abnormal_order  
    where 1=1
    and LEFT(create_time - INTERVAL 1 HOUR,10) = date(now())
    GROUP BY 1,2,3,4

UNION
    select 
    hour(finish_time) completed_hour
    ,warehouse_id
    ,'异常单' 单据
    ,'完成异常单量' TYPE 
    ,count(id)num -- 及时上架
    from wms_production.abnormal_order 
    where 1=1
    and LEFT(finish_time ,10) =date(now())
    GROUP BY 1,2,3,4

-- union
--     select 
--     LEFT(create_time + INTERVAL 23 HOUR,10) deadline
--     ,warehouse_id
--     ,'异常单' 单据
--     ,'及时完成' TYPE 
--     ,sum(IF(finish_time<(create_time + INTERVAL 23 HOUR ),1,0))num -- 及时上架
--     from wms_production.abnormal_order 
--     where 1=1
--     and LEFT(create_time + INTERVAL 23 HOUR,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now() - INTERVAL 1 day,10)
--     GROUP BY 1,2,3,4

-- UNION
--     select 
--     LEFT(create_time + INTERVAL 23 HOUR,10) deadline
--     ,warehouse_id
--     ,'异常单' 单据
--     ,'应完成' TYPE 
--     ,sum(if((finish_time is not null OR (create_time + INTERVAL 23 HOUR)< date_add(now(), interval -60 minute)),1,0)) -- 应上架
--     from wms_production.abnormal_order 
--     where 1=1
--     and LEFT(create_time + INTERVAL 23 HOUR,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now() - INTERVAL 1 day,10) -- deadline 近7天
--     GROUP BY 1,2,3,4

UNION
    select 
    hour(now()) date
    ,warehouse_id
    ,'异常单' 单据
    ,'未完结异常单量' TYPE 
    ,count(id)
    from wms_production.abnormal_order 
    where 1=1
    AND finish_time IS null
    -- and (create_time + INTERVAL 23 HOUR)< now() - INTERVAL 1 hour
    GROUP BY 1,2,3,4
)T1 
LEFT JOIN wms_production.warehouse w ON T1.warehouse_id=w.id
group by 1,2,3,4


