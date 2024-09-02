WITH T1 as
(
    SELECT 
        warehouse_id
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
                ,if(shelf_complete_time)
            FROM wms_production.arrival_notice an
            left join was.inb_receive_bill irb on an.notice_number = irb.receive_external_no
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
    )
)

select 
    日期,
    case when w.name='AutoWarehouse' then 'AGV'
            when w.name='BPL-Return Warehouse' then 'BPL-Return'
            when w.name='BPL3-LIVESTREAM' OR warehouse_id='312' then 'BPL3'
            when w.name='BangsaoThong' then 'BST'
            when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS' # ,'PMD-WH','BKK-WH-Ecommerce','BKK-WH-LAS物料仓'
            when w.name='LCP Warehouse' then 'LCP' end 仓库名称
    ,单据
    ,指标
    ,sum(数量) 
FROM 
    (
        select 
            LEFT(reg_time,10) 日期
            ,warehouse_id
            ,单据
            ,'到货单量' 指标
            ,count(notice_number) 数量
        FROM T1 
        WHERE left(reg_time,10) BETWEEN LEFT(now()- INTERVAL 7 DAY ,10) AND LEFT(now()- INTERVAL 1 DAY ,10)
        group by 1,2,3,4

        UNION 
        select 
            LEFT(complete_time,10) 日期
            ,warehouse_id
            ,单据
            ,'入库单量' 指标
            ,count(notice_number) 数量
        FROM T1 
        WHERE left(complete_time,10) BETWEEN LEFT(now()- INTERVAL 7 DAY ,10) AND LEFT(now()- INTERVAL 1 DAY ,10)
        group by 1,2,3,4

        union
        SELECT 
            LEFT(receive_deadline_24h,10) 日期
            ,warehouse_id
            ,单据
            ,'及时入库' TYPE
            ,sum(及时入库)
        FROM T1 
        WHERE left(receive_deadline_24h,10) BETWEEN LEFT(now()- INTERVAL 7 DAY ,10) AND LEFT(now()- INTERVAL 1 DAY ,10)
        group by 1,2,3,4

        union
        SELECT 
            LEFT(receive_deadline_24h,10) 日期
            ,warehouse_id
            ,单据
            ,'应入库' TYPE
            ,sum(应入库)
        FROM T1 
        WHERE left(receive_deadline_24h,10) BETWEEN LEFT(now()- INTERVAL 7 DAY ,10) AND LEFT(now()- INTERVAL 1 DAY ,10)
        group by 1,2,3,4

        UNION
        SELECT 
            LEFT(receive_deadline_24h,10) 日期
            ,warehouse_id
            ,单据
            ,'未及时入库' type
            ,count(notice_number) num
        FROM T1 
        WHERE 1=1
            and 及时入库='0'
            and  应入库='1'
        group by 1,2,3,4

        union
        SELECT 
            LEFT(putaway_deadline_48h,10) 日期
            ,warehouse_id
            ,单据
            ,'及时上架' TYPE
            ,sum(及时上架)
        FROM T1 
        WHERE left(putaway_deadline_48h,10) BETWEEN LEFT(now()- INTERVAL 7 DAY ,10) AND LEFT(now()- INTERVAL 1 DAY ,10)
        group by 1,2,3,4

        union
        SELECT 
            LEFT(putaway_deadline_48h,10) 日期
            ,warehouse_id
            ,单据
            ,'应上架' TYPE
            ,sum(应上架)
        FROM T1 
        WHERE left(putaway_deadline_48h,10) BETWEEN LEFT(now()- INTERVAL 7 DAY ,10) AND LEFT(now()- INTERVAL 1 DAY ,10)
        group by 1,2,3,4

        UNION
        SELECT 
            /* LEFT(now()- INTERVAL 1 DAY ,10) 日期 */
            LEFT(putaway_deadline_48h,10) 日期
            ,warehouse_id
            ,单据
            ,'未及时上架' type
            ,count(notice_number) num
        FROM T1 
        WHERE 1=1
        and 及时上架='0' 
        and  应上架='1' 
        AND shelf_status <> '1080'
        and left(putaway_deadline_48h,10) BETWEEN LEFT(now()- INTERVAL 7 DAY ,10) AND LEFT(now()- INTERVAL 1 DAY ,10)
        group by 1,2,3,4
    )a
    LEFT JOIN 
    wms_production.warehouse w ON a.warehouse_id=w.id
GROUP BY 1,2,3,4
HAVING 仓库名称 IS NOT null;

###################################################################### report ######################################################################

WITH T1 as
(
    SELECT 
        warehouse_id
        ,仓库名称
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
                ,case when w.name='AutoWarehouse' then 'AGV'
                    when w.name='BPL-Return Warehouse' then 'BPL-Return'
                    when w.name='BPL3-LIVESTREAM' OR warehouse_id='312' then 'BPL3'
                    when w.name='BangsaoThong' then 'BST'
                    when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS' # ,'PMD-WH','BKK-WH-Ecommerce','BKK-WH-LAS物料仓'
                    when w.name='LCP Warehouse' then 'LCP' end 仓库名称
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
                left join 
                (
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
            LEFT JOIN 
            wms_production.warehouse w ON an.warehouse_id=w.id
        )
)
select
calendar.日期
,calendar.仓库名称
,calendar.单据
,t0.到货单量
,t1.入库单量
,t2.及时入库
,t3.应入库
,t2.及时入库 / t3.应入库 入库及时率
,t4.超时未入库
,t5.及时上架
,t6.应上架
,t5.及时上架 / t6.应上架 上架及时率
,t7.超时未上架
from
(
    select
        t0.日期
        ,t1.仓库名称
        ,t2.单据
    from
    (
        select 
        date 日期 
        from 
        tmpale.ods_th_dim_date where date between LEFT(now()- INTERVAL 7 DAY ,10) AND LEFT(now()- INTERVAL 1 DAY ,10)
    )t0 join 
    (
        select 
            'AGV' 仓库名称
        union
        select
            'BPL-Return'
        union
        select
            'BPL3'
        union
        select
            'BST'
        union
        select
            'LAS'
    )t1 on 1=1
    join
    (
        select
        '采购订单' 单据
        union
        select
        '销退订单'
    )t2 on 1=1
    
)calendar 
left join
(
    select 
        LEFT(reg_time,10) 日期
        ,仓库名称
        ,单据
        ,'到货单量' 指标
        ,count(notice_number) 到货单量
    FROM T1 
    WHERE left(reg_time,10) BETWEEN LEFT(now()- INTERVAL 7 DAY ,10) AND LEFT(now()- INTERVAL 1 DAY ,10)
    group by 1,2,3,4
) t0 on calendar.日期 = t0.日期 and calendar.仓库名称 = t0.仓库名称 and calendar.单据 = t0.单据
left join
(
    select 
        LEFT(complete_time,10) 日期
        ,仓库名称
        ,单据
        ,'入库单量' 指标
        ,count(notice_number) 入库单量
    FROM T1 
    WHERE left(complete_time,10) BETWEEN LEFT(now()- INTERVAL 7 DAY ,10) AND LEFT(now()- INTERVAL 1 DAY ,10)
    group by 1,2,3,4
) t1 on calendar.日期 = t1.日期 and calendar.仓库名称 = t1.仓库名称 and calendar.单据 = t1.单据
left join
(
    SELECT 
        LEFT(receive_deadline_24h,10) 日期
        ,仓库名称
        ,单据
        ,'及时入库' TYPE
        ,sum(及时入库) 及时入库
    FROM T1 
    WHERE left(receive_deadline_24h,10) BETWEEN LEFT(now()- INTERVAL 7 DAY ,10) AND LEFT(now()- INTERVAL 1 DAY ,10)
    group by 1,2,3,4
) t2 on calendar.日期 = t2.日期 and calendar.仓库名称 = t2.仓库名称 and calendar.单据 = t2.单据
left join
(
    SELECT 
        LEFT(receive_deadline_24h,10) 日期
        ,仓库名称
        ,单据
        ,'应入库' TYPE
        ,sum(应入库) 应入库
    FROM T1 
    WHERE left(receive_deadline_24h,10) BETWEEN LEFT(now()- INTERVAL 7 DAY ,10) AND LEFT(now()- INTERVAL 1 DAY ,10)
    group by 1,2,3,4
) t3 on calendar.日期 = t3.日期 and calendar.仓库名称 = t3.仓库名称 and calendar.单据 = t3.单据
left join
(
    SELECT 
        LEFT(now()- INTERVAL 1 DAY ,10) 日期
        ,仓库名称
        ,单据
        ,'超时未入库' type
        ,count(notice_number) 超时未入库
    FROM T1 
    WHERE complete_time is  null and  应入库='1' 
    group by 1,2,3,4
) t4 on calendar.日期 = t4.日期 and calendar.仓库名称 = t4.仓库名称 and calendar.单据 = t4.单据
left join
(
    SELECT 
        LEFT(putaway_deadline_48h,10) 日期
        ,仓库名称
        ,单据
        ,'及时上架' TYPE
        ,sum(及时上架) 及时上架
    FROM T1 
    WHERE left(putaway_deadline_48h,10) BETWEEN LEFT(now()- INTERVAL 7 DAY ,10) AND LEFT(now()- INTERVAL 1 DAY ,10)
    group by 1,2,3,4
) t5 on calendar.日期 = t5.日期 and calendar.仓库名称 = t5.仓库名称 and calendar.单据 = t5.单据
left join
(
    SELECT 
        LEFT(putaway_deadline_48h,10) 日期
        ,仓库名称
        ,单据
        ,'应上架' TYPE
        ,sum(应上架) 应上架
    FROM T1 
    WHERE left(putaway_deadline_48h,10) BETWEEN LEFT(now()- INTERVAL 7 DAY ,10) AND LEFT(now()- INTERVAL 1 DAY ,10)
    group by 1,2,3,4
) t6 on calendar.日期 = t6.日期 and calendar.仓库名称 = t6.仓库名称 and calendar.单据 = t6.单据
left join
(
    SELECT 
        LEFT(now()- INTERVAL 1 DAY ,10) 日期
        ,仓库名称
        ,单据
        ,'超时未上架' type
        ,count(notice_number) 超时未上架
    FROM T1 
    WHERE shelf_complete_time is  null and  应上架='1' 
    AND shelf_status <> '1080'
    group by 1,2,3,4
) t7 on calendar.日期 = t7.日期 and calendar.仓库名称 = t7.仓库名称 and calendar.单据 = t7.单据
where 1=1
    and calendar.日期='${dt}'
    and calendar.仓库名称='BPL-Return'
order by calendar.日期
        ,calendar.仓库名称
        ,calendar.单据