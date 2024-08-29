WITH CK AS
(SELECT *,
IF(finish_delivery<deadline,1,0)及时发货
,if(audit_time is not null and (finish_delivery is not null OR deadline< date_add(now(), interval -60 minute)),1,0) 应发货
FROM 
(    SELECT 
        delivery_sn,warehouse_id
        ,case when warehouse_id='18' then 'BST'
        WHEN  warehouse_id='33' then 'BPL-return'
        WHEN warehouse_id='36' then 'AGV'
        when warehouse_id in ('19','39')then 'LAS' 
        END warehouse_name
        ,CASE WHEN platform_source in('Shopee','Tik Tok','LAZADA')THEN platform_source ELSE 'Other' END platform_source
        ,seller_id
        ,seller_name
        ,seller_group Seller_Group
        ,created_time Created_Time
        ,created_date
        ,audit_time
        ,audit_date
        ,start_pick
        ,finish_pick
        ,finish_pack
        ,finish_handover
        ,finish_delivery
        ,created_time_mod
#     ,if(finish_delivery<=concat(date2,substr(created_time_mod,11,9)),1,0)及时发货
#     ,if(audit_time is not null and (finish_delivery is not null OR concat(date2,substr(created_time_mod,11,9))< date_add(now(), interval -60 minute)),1,0) 应发货
  ,case when finish_delivery is not null then 'Delivered'
     when finish_pack is not null then 'Not Delivered'
     when finish_pick is not null then 'Not packed'
     when audit_time is not null then 'Not Picked'
     when audit_time is null then 'Not Audited' end status
#  ,case when finish_pack is null and concat(date1,substr(created_time_mod,11,9))< date_add(now(), interval -60 minute) then 'overdue'
#     when finish_delivery is null and concat(date2,substr(created_time_mod,11,9))< date_add(now(), interval -60 minute) then 'overdue'
#     end warning
  ,CASE WHEN platform_source='Shopee' AND substr(created_time_mod,12,2)<12 THEN concat(LEFT(created_time_mod,10),' 21:00:00')
   WHEN platform_source='Shopee' AND substr(created_time_mod,12,2)>=12 THEN concat(date1,' 21:00:00')
   WHEN platform_source='Tik Tok' AND substr(created_time_mod,12,2)<12 THEN concat(date_add(LEFT(created_time_mod,10),1),' 00:00:00')
   WHEN platform_source='Tik Tok' AND substr(created_time_mod,12,2)>=12 THEN concat(date_add(date1,1),' 00:00:00')
   ELSE concat(date1,substr(created_time_mod,11,9)) END deadline
  ,date_add(now(),interval -60 minute) ETL   
    FROM 
        (SELECT *,case when if_day_off='是' then concat(created_mod,' 00:00:00') else concat(created_mod,substr(created_time,11,9))end created_time_mod FROM(
        SELECT
            do.`delivery_sn`
          ,warehouse_id
            ,ps.`name` platform_source
            ,sl.`name` seller_name
            ,do.`seller_id`
            -- ,do.`buy_time`
            ,date_add(do.`created`, interval -60 minute) created_time
            ,left(date_add(do.`created`, interval -60 minute), 10) created_date
            ,date_add(do.`audit_time`, interval -60 minute) audit_time
            ,left(date_add(do.`audit_time`, interval -60 minute), 10) audit_date
            ,do.`start_pick` start_pick
            ,do.`succ_pick` finish_pick
            ,do.`pack_time` finish_pack
            ,do.`start_receipt ` finish_handover
            ,date_add(do.`delivery_time`, interval -60 minute) finish_delivery
            ,case when sl.`name` REGEXP '^Intrepid.*$'=1 then 'Intrepid'
                when sl.`name` REGEXP '^SCI.*$'=1 then 'SCI'
                    when sl.`name`='CREA' then 'CREA'
                    when sl.`name`='Ondemand' then 'Ondemand'
                    else 'Other'
                    end seller_group 
            -- ,date_add(date_add(do.`created`, interval -60 minute), interval 24*60 minute) pack_deadline
            -- ,date_add(date_add(do.`created`, interval -60 minute), interval 2*24*60 minute) delivery_deadline

        FROM `wms_production`.`delivery_order` do 
        LEFT JOIN `wms_production`.`seller_platform_source` sps on do.`platform_source_id`=sps.`id`
        LEFT JOIN `wms_production`.`platform_source` ps on sps.`platform_source_id`=ps.`id` 
        LEFT JOIN `wms_production`.`seller` sl on do.`seller_id`=sl.`id`
        WHERE 1=1
            AND do.`status` NOT IN ('1000','1010') -- 取消单
            AND do.`platform_status`!=9
            AND do.prompt NOT in (1,2,3,4)-- 剔除拦截
            AND audit_time IS NOT null
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

,RK as
(SELECT warehouse_id
   ,notice_number
   ,reg_time -- 时间签收时间
  ,reg_time_mod -- 节假日调整签收时间
   ,complete_time
   ,shelf_complete_time
        ,concat(date1,substr(reg_time_mod,11,9)) receive_deadline_24h -- 节假日顺延24hdd
        ,concat(date2,substr(reg_time_mod,11,9)) putaway_deadline_48h -- 节假日顺延48hdd
  ,if(complete_time is not null and (complete_time<=concat(date1,substr(reg_time_mod,11,9))),1,0) 及时入库
  ,if(reg_time is not null and (complete_time is not null OR concat(date1,substr(reg_time_mod,11,9))< date_add(now(), interval -60 minute)),1,0) 应入库
        ,if(shelf_complete_time is not null and (shelf_complete_time<=concat(date2,substr(reg_time_mod,11,9))),1,0) 及时上架
     ,if(reg_time is not null and (shelf_complete_time is not null OR concat(date2,substr(reg_time_mod,11,9))< date_add(now(), interval -60 minute)),1,0) 应上架
from
-- 节假日签收认为是节后首个工作日00：00：00签收的
(SELECT *,case when if_day_off='是' then concat(created_mod,' 00:00:00') else concat(created_mod,substr(reg_time,11,9))end reg_time_mod  from 
(SELECT 
notice_number
,warehouse_id
,reg_time - interval 1 hour reg_time
,left(reg_time - interval 1 hour,10) reg_date
,start_receiving_time
,complete_time
,shelf_complete_time
 FROM wms_production.arrival_notice )an
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
  where date0 is not null order by 1 desc))calendar on calendar.created=an.reg_date))
,XT AS 
(SELECT warehouse_id
   ,back_sn
   ,arrival_time -- 时间签收时间
  ,arrival_time_mod -- 节假日调整签收时间
   ,complete_time
   ,shelf_end_time
        ,concat(date1,substr(arrival_time_mod,11,9)) receive_deadline_24h -- 节假日顺延24hdd
        ,concat(date2,substr(arrival_time_mod,11,9)) putaway_deadline_48h -- 节假日顺延48hdd
  ,if(complete_time is not null and (complete_time<=concat(date1,substr(arrival_time_mod,11,9))),1,0) 及时入库
  ,if(arrival_time is not null and (complete_time is not null OR concat(date1,substr(arrival_time_mod,11,9))< date_add(now(), interval -60 minute)),1,0) 应入库
        ,if(shelf_end_time is not null and (shelf_end_time<=concat(date2,substr(arrival_time_mod,11,9))),1,0) 及时上架
     ,if(arrival_time is not null and (shelf_end_time is not null OR concat(date2,substr(arrival_time_mod,11,9))< date_add(now(), interval -60 minute)),1,0) 应上架
from
-- 节假日签收认为是节后首个工作日00：00：00签收的
(SELECT *,case when if_day_off='是' then concat(created_mod,' 00:00:00') else concat(created_mod,substr(arrival_time,11,9))end arrival_time_mod  from 
(SELECT 
back_sn
,warehouse_id
,arrival_time arrival_time
,left(arrival_time ,10) arrival_date
,complete_time
,shelf_end_time
 FROM wms_production.delivery_rollback_order )an
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
  where date0 is not null order by 1 desc))calendar on calendar.created=an.arrival_date))



SELECT 类型
,case when w.name='AutoWarehouse' then 'AGV'
        when w.name='BPL-Return Warehouse' then 'BPL-Return'
        when w.name='BPL3-LIVESTREAM' then 'BPL3'
        when w.name='BangsaoThong' then 'BST'
        when w.name IN ('BKK-WH-LAS2电商仓','PMD-WH','BKK-WH-Ecommerce','BKK-WH-LAS物料仓')         then 'LAS'
        when w.name='LCP Warehouse' then 'LCP' end 仓库名称,
to_date(日期)日期,week(日期 + interval 1 day) 周,及时完成单量,应完成单量,创建单量,及时率
from 
(SELECT '发货及时率' 类型,warehouse_id,created_date 日期,sum(及时发货)及时完成单量,sum(应发货)应完成单量
,count(delivery_sn) 创建单量
,sum(及时发货)/sum(应发货)及时率
FROM CK
where created_date>=left(now()- INTERVAL 70 day,10)
group by 1,2,3
 
UNION  
SELECT 
'24H入库及时率' TYPE,warehouse_id,to_date(LEFT(reg_time,10))日期
,sum(及时入库),sum(应入库),count(notice_number)
,sum(及时入库)/sum(应入库)及时率

FROM RK
WHERE LEFT(reg_time,10)>=left(now()- INTERVAL 70 day,10)
GROUP BY 1,2,3


UNION 
SELECT 
'48H上架及时率' type,warehouse_id,to_date(LEFT(reg_time,10))日期
,sum(及时上架),sum(应上架),count(notice_number)
,sum(及时上架)/sum(应上架)
FROM RK
WHERE LEFT(reg_time,10)>=left(now()- INTERVAL 70 day,10)
GROUP BY 1,2,3
 
union
SELECT 
'48H销退上架及时率' type,warehouse_id,to_date(LEFT(arrival_time,10))日期
,sum(及时上架),sum(应上架),count(back_sn)
,sum(及时上架)/sum(应上架)
FROM XT
WHERE LEFT(arrival_time,10)>=left(now()- INTERVAL 70 day,10)
GROUP BY 1,2,3
) T1
LEFT JOIN wms_production.warehouse w on warehouse_id=w.id
where w.name in('AutoWarehouse','BPL-Return Warehouse','BPL3-LIVESTREAM','BangsaoThong','BKK-WH-LAS2电商仓','PMD-WH','BKK-WH-Ecommerce','BKK-WH-LAS物料仓')
order by 1,2,3
 
