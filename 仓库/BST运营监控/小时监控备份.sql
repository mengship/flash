with EF as
(SELECT *,
IF(finish_delivery<deadline,1,0)及时发货
,if(audit_time is not null and (finish_delivery is not null OR deadline< date_add(now(), interval -60 minute)),1,0) 应发货
FROM 
(    SELECT TYPE,
        delivery_sn,goods_num,warehouse_id,
        case when warehouse_name='AutoWarehouse' then 'AGV'
        when warehouse_name='BPL-Return Warehouse' then 'BPL-Return'
        when warehouse_name='BPL3-LIVESTREAM' then 'BPL3'
        when warehouse_name='BangsaoThong' then 'BST'
        when warehouse_name IN ('BKK-WH-LAS2电商仓','PMD-WH','BKK-WH-Ecommerce','BKK-WH-LAS物料仓')         then 'LAS'
        when warehouse_name ='LCP Warehouse' then 'LCP' end warehouse_name
        ,CASE WHEN platform_source in('Shopee','Tik Tok','LAZADA')THEN platform_source ELSE 'Other' END platform_source
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
#     ,if(finish_delivery<=concat(date2,substr(created_time_mod,11,9)),1,0)及时发货
#     ,if(audit_time is not null and (finish_delivery is not null OR concat(date2,substr(created_time_mod,11,9))< date_add(now(), interval -60 minute)),1,0) 应发货
  ,CASE WHEN platform_source='Shopee' AND substr(created_time_mod,12,2)<16 THEN concat(LEFT(created_time_mod,10),' 20:00:00')
   WHEN platform_source='Shopee' AND substr(created_time_mod,12,2)>=16 THEN concat(date1,' 20:00:00')
   WHEN platform_source='Tik Tok' AND substr(created_time_mod,12,2)<18 THEN concat(LEFT(created_time_mod,10),' 20:00:00')
   WHEN platform_source='Tik Tok' AND substr(created_time_mod,12,2)>=18 THEN concat(date1,' 20:00:00')
   WHEN TYPE ='B2B' THEN concat(date2,substr(created_time_mod,11,9))
   ELSE concat(date1,substr(created_time_mod,11,9)) END deadline
  ,date_add(now(),interval -60 minute) ETL   
    FROM 
        (SELECT *,case when if_day_off='是' then concat(created_mod,' 00:00:00') else concat(created_mod,substr(created_time,11,9))end created_time_mod FROM(
        SELECT 'B2C' TYPE
           ,do.`delivery_sn`,goods_num 
          ,warehouse_id
          ,w.name warehouse_name
            ,ps.`name` platform_source
            ,sl.`name` seller_name
            ,do.`seller_id`
            ,date_add(do.`created`, interval -60 minute) created_time
            ,left(date_add(do.`created`, interval -60 minute), 10) created_date
            ,date_add(do.`audit_time`, interval -60 minute) audit_time
            ,left(date_add(do.`audit_time`, interval -60 minute), 10) audit_date
            ,do.`start_receipt ` finish_delivery
            ,delivery_time - INTERVAL 1 HOUR delivery_time
            ,start_receipt
            ,pack_time
            ,succ_pick
            -- ,date_add(do.`delivery_time`, interval -60 minute) finish_delivery
            
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
        SELECT 'B2C' TYPE
           ,do.`delivery_sn`,goods_num 
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
            -- ,date_add(do.`delivery_time`, interval -60 minute) finish_delivery
            
        FROM `erp_wms_prod`.`delivery_order` do 
        LEFT JOIN `erp_wms_prod`.`platform_source` ps on do.`platform_source_id`=ps.`id` 
        LEFT JOIN `erp_wms_prod`.`seller` sl on do.`seller_id`=sl.`id`
        LEFT JOIN erp_wms_prod.warehouse w ON do.warehouse_id=w.id
        WHERE date_add(do.`created`, interval -60 minute)>=DATE_ADD(CURRENT_DATE(),-10) AND warehouse_id='312' 
            -- AND do.`status` NOT IN ('1000','1010') -- 取消单
           -- AND do.`platform_status`!=9
            -- AND do.prompt NOT in (1,2,3,4)-- 剔除拦截

        ) do 
        
        left join 
        -- 日历调整// created_mod是节假日顺延后首日,date1是节假日顺延后第二天
  (select created,if_day_off
  ,case when if_day_off ='是' then date else date0 end created_mod
  ,case when if_day_off ='是' then date1 else date end date1
  ,case when if_day_off ='是' then date2 else date1 end date2
  ,case when if_day_off ='是' then date3 else date2 end date3
  ,case when if_day_off ='是' then date4 else date3 end date4
  from 
  (select calendar.date created,case when off_date is not null then '是' else '否' end if_day_off, 
  date0,workdate.date date,date1,date2,date3,date4
  from 
  -- 日历
  (select date from tmpale.ods_th_dim_date where date between '2023-01-01' and date_add(curdate(), interval 21 day))calendar 
  left join
  -- 假日表
  (select off_date from fle_staging.sys_holiday where deleted = 0 and company_category='2' and off_date>='2023-01-01' group by off_date)offdate
  on calendar.date=off_date
  left join
  -- 仓库工作日表（date为工作日，date0上一个工作日，date1为下一个工作日，date2为下下一个工作日...）
  (select lag(date,1)over(order by date)date0,date,lead(date,1)over(order by date)date1,lead(date,2)over(order by date)date2,lead(date,3)over(order by date)date3,lead(date,4)over(order by date)date4 from 
  (select date from tmpale.ods_th_dim_date where date between '2022-12-31' and date_add(curdate(), interval 28 day) ) d0
  left join 
  (select off_date from fle_staging.sys_holiday where deleted = 0 and company_category='2' and off_date>='2022-12-31' group by off_date)
  on date=off_date 
  where off_date is null)workdate
  on calendar.date>=workdate.date0 and calendar.date<workdate.date
  where date0 is not null order by 1 desc))calendar on calendar.created=do.created_date
 )do))

 
SELECT warehouse_name,LEFT(created_time,10),HOUR(created_time),'流入' 指标,count(delivery_sn),sum(goods_num) FROM EF  
WHERE LEFT(created_time,10)='2024-08-08' GROUP BY 1,2,3,4

union
SELECT warehouse_name,LEFT(created_time,10),HOUR(created_time),'未审核' 指标,count(delivery_sn),sum(goods_num) FROM EF  
WHERE LEFT(created_time,10)='2024-08-08' AND audit_time IS NULL GROUP BY 1,2,3,4 

union
SELECT warehouse_name,LEFT(succ_pick,10),HOUR(succ_pick),'拣货完成' 指标,count(delivery_sn),sum(goods_num) FROM EF  
WHERE LEFT(succ_pick,10)='2024-08-08' GROUP BY 1,2,3,4 

union
SELECT warehouse_name,LEFT(pack_time,10),HOUR(pack_time),'打包完成' 指标,count(delivery_sn),sum(goods_num) FROM EF  
WHERE LEFT(pack_time,10)='2024-08-08' GROUP BY 1,2,3,4 

union
SELECT warehouse_name,LEFT(start_receipt,10),HOUR(start_receipt),'开始交接' 指标,count(delivery_sn),sum(goods_num) FROM EF  
WHERE LEFT(start_receipt,10)='2024-08-08' GROUP BY 1,2,3,4 

union
SELECT warehouse_name,LEFT(delivery_time,10),HOUR(delivery_time),'出库' 指标,count(delivery_sn),sum(goods_num) FROM EF  
WHERE LEFT(delivery_time,10)='2024-08-08' GROUP BY 1,2,3,4 

union
SELECT warehouse_name,'2024-08-08',HOUR(now()),'积压（未打包完成）' 指标,count(delivery_sn),sum(goods_num) FROM EF  
WHERE  audit_time IS NOT NULL AND pack_time is NULL GROUP BY 1,2,3,4 

union
SELECT warehouse_name,'2024-08-08',HOUR(now()),'昨日积压' 指标,count(delivery_sn),sum(goods_num) FROM EF  
WHERE  audit_time IS NOT NULL AND pack_time is NULL  AND LEFT(Created_Time,10)<left(now(),10) GROUP BY 1,2,3,4 
