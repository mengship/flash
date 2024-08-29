WITH T1 as
(
    SELECT 
        warehouse_id
        ,w.name warehouse_name
        ,单据
        ,shelf_status
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
        (
            SELECT 
                *
                ,case when if_day_off='是' then concat(created_mod,' 00:00:00') else concat(created_mod,substr(reg_time,11,9))end reg_time_mod
            from 
            (
                SELECT 
                    notice_number
                    ,"采购订单" 单据
                    ,warehouse_id
                    ,'' shelf_status
                    ,reg_time - interval 1 hour reg_time
                    ,left(reg_time - interval 1 hour,10) reg_date
                    ,complete_time
                    ,shelf_complete_time
                FROM wms_production.arrival_notice 
                    WHERE reg_time IS NOT NULL 
                    AND status>='30' 
                    AND reg_time >= convert_tz(date_sub(date(now() + interval -1 hour),interval 30 day), '+07:00', '+08:00')

                UNION 
                SELECT 
                    back_sn
                    ,"销退订单" 单据
                    ,warehouse_id
                    ,shelf_status
                    ,arrival_time arrival_time
                    ,left(arrival_time ,10) arrival_date
                    ,complete_time
                    ,shelf_end_time
                FROM wms_production.delivery_rollback_order 
                WHERE arrival_time IS NOT NULL AND status >= '1045' AND STATUS<>'9000'
                AND back_express_status NOT IN('20','30')
                AND arrival_time >= convert_tz(date_sub(date(now() + interval -1 hour),interval 30 day), '+07:00', '+07:00')

                UNION
                select
                    an.notice_number
                    ,"采购订单" 单据
                    ,warehouse_id
                    ,'' shelf_status
                    ,an.complete_time + interval 7 hour
                    ,date(an.complete_time + interval 7 hour) unload_start_date
                    ,an.complete_time + interval 7 hour complete_time
                    ,os.末次上架结束时间
                from
                erp_wms_prod.arrival_notice an
                left join (
                    select
                        os.from_order_sn 来源入库单号,
                        min(os.shelf_start_time + interval 7 hour) 首次上架开始时间,
                        max(os.shelf_end_time + interval 7 hour) 末次上架结束时间
                    from
                        erp_wms_prod.on_shelf_order os
                    where os.shelf_end_time >= convert_tz(date_sub(date(now() + interval -1 hour),interval 30 day), '+07:00', '+00:00')
                    group by
                        os.from_order_sn
                ) os on an.notice_number = os.来源入库单号
                WHERE an.complete_time IS NOT NULL 
                AND an.status>='30' 
                AND an.complete_time >= convert_tz(date_sub(date(now() + interval -1 hour),interval 30 day), '+07:00', '+00:00')

                UNION
                SELECT rollback_sn
                    ,"销退订单" 单据
                    ,warehouse_id
                    ,status shelf_status
                    ,arrival_time + interval 7 hour arrival_time
                    ,left(arrival_time ,10) arrival_date
                    ,receive_time + interval 7 hour complete_time
                    ,complete_time + interval 7 hour shelf_end_time
                FROM erp_wms_prod.rollback_order  
                where arrival_time IS NOT NULL 
                AND status >= '1045' 
                AND STATUS<>'9000'
                AND arrival_time >= convert_tz(date_sub(date(now() + interval -1 hour),interval 30 day), '+07:00', '+00:00')

            ) an

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
                        (select date from tmpale.ods_th_dim_date where date between date_sub(date(now() + interval -1 hour),interval 180 day) and date_add(curdate(), interval 21 day))calendar 
                        left join
                        -- 假日表
                        (select off_date from fle_staging.sys_holiday where deleted = 0 and company_category='2' and off_date between date_sub(date(now() + interval -1 hour),interval 180 day) and date_add(date(now() + interval -1 hour), interval 21 day)  group by off_date) offdate on calendar.date=offdate.off_date
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
                                (select date from tmpale.ods_th_dim_date where date between date_sub(date(now() + interval -1 hour),interval 180 day) and date_add(curdate(), interval 28 day) ) d0
                            left join 
                                (select off_date from fle_staging.sys_holiday where deleted = 0 and company_category='2' and off_date between date_sub(date(now() + interval -1 hour),interval 180 day) and date_add(date(now() + interval -1 hour), interval 28 day) group by off_date) off on d0.date=off.off_date 
                            where off_date is null
                        )workdate on calendar.date>=workdate.date0 and calendar.date<workdate.date
                        where date0 is not null order by 1 desc
                    )
            )calendar on calendar.created=an.reg_date
        )a LEFT JOIN  wms_production.warehouse w ON a.warehouse_id=w.id
)

SELECT 
    '未及时入库' type
    ,warehouse_name
    ,单据
    ,shelf_status
    ,notice_number
    ,reg_time 签收时间
    ,complete_time 收货时间
    ,shelf_complete_time 上架时间
    ,receive_deadline_24h 最晚收货时间
    ,putaway_deadline_48h 最晚上架时间
    ,及时入库
    ,应入库
    ,及时上架
    ,应上架
FROM T1 
WHERE 1=1
    and 及时入库='0'
    and  应入库='1'
    /* and left(receive_deadline_24h,10) BETWEEN LEFT(now()- INTERVAL 7 DAY ,10) AND LEFT(now()- INTERVAL 1 DAY ,10) */
    and left(receive_deadline_24h,10) = date_sub(date(now() + interval -1 hour),interval 1 day)

union
SELECT 
    '未及时上架' type
    ,warehouse_name
    ,单据
    ,shelf_status
    ,notice_number
    ,reg_time 签收时间
    ,complete_time 收货时间
    ,shelf_complete_time 上架时间
    ,receive_deadline_24h 最晚收货时间
    ,putaway_deadline_48h 最晚上架时间
    ,及时入库
    ,应入库
    ,及时上架
    ,应上架
FROM T1 
WHERE 1=1
    and 及时上架='0'
    and  应上架='1'
    /* and left(putaway_deadline_48h,10) BETWEEN LEFT(now()- INTERVAL 7 DAY ,10) AND LEFT(now()- INTERVAL 1 DAY ,10) */
    and left(putaway_deadline_48h,10) = date_sub(date(now() + interval -1 hour),interval 1 day)