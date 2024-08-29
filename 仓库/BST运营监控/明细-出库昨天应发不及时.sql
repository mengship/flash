with EF as
(
    SELECT 
        *
        -- ,IF(delivery_time<deadline,1,0)及时发货
        ,case when TYPE='B2C' and delivery_time<deadline then 1
            when TYPE='B2B' and pack_time<deadline then 1
            else 0
            end as 及时发货
        -- ,if(audit_time is not null and (delivery_time is not null OR deadline< date_add(now(), interval -60 minute)),1,0) 应发货
        ,case when TYPE='B2C' and audit_time is not null and (delivery_time is not null OR deadline< date_add(now(), interval -60 minute)) then 1
            when TYPE='B2B' and audit_time is not null and (pack_time is not null OR deadline< date_add(now(), interval -60 minute)) then 1
            else 0
            end as 应发货
    FROM
    (
        SELECT 
            TYPE
            ,delivery_sn
            ,goods_num
            ,warehouse_id
            ,case when warehouse_name='AutoWarehouse'   then 'AGV'
                when warehouse_name='BPL-Return Warehouse'  then 'BPL-Return'
                when warehouse_name='BPL3-LIVESTREAM'   then 'BPL3'
                when warehouse_name='BangsaoThong'  then 'BST'
                when warehouse_name IN ('BKK-WH-LAS2电商仓')    then 'LAS'
                when warehouse_name ='LCP Warehouse' then 'LCP' end warehouse_name
            ,CASE WHEN platform_source in('Shopee','Tik Tok','LAZADA')THEN platform_source ELSE 'Other' END platform_source
            ,seller_id
            ,seller_name
            ,created_time Created_Time
            ,created_date
            ,audit_time
            ,audit_date
            ,pack_time
            ,handover_time
            ,delivery_time
            ,created_time_mod
            ,CASE WHEN platform_source='Shopee' AND substr(created_time_mod,12,2)<16 THEN concat(LEFT(created_time_mod,10),' 23:59:59')
                WHEN platform_source='Shopee' AND substr(created_time_mod,12,2)>=16 THEN concat(date1,' 23:59:59')
                WHEN platform_source='Tik Tok' AND substr(created_time_mod,12,2)<18 THEN concat(LEFT(created_time_mod,10),' 23:59:59')
                WHEN platform_source='Tik Tok' AND substr(created_time_mod,12,2)>=18 THEN concat(date1,' 23:59:59')
                WHEN TYPE ='B2B' THEN concat(date2,substr(created_time_mod,11,9))
                ELSE concat(date1,substr(created_time_mod,11,9)) END deadline
            ,date_add(now(),interval -60 minute) ETL
            ,is_time
        FROM
        (
            select
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
                ,do.pack_time
                ,do.handover_time
                ,do.delivery_time
                ,do.is_time
                ,calendar.created
                ,calendar.if_day_off
                ,calendar.created_mod
                ,calendar.date1
                ,calendar.date2
                ,calendar.date3
                ,calendar.date4
                ,case when calendar.if_day_off='是' then concat(calendar.created_mod,' 00:00:00') else concat(calendar.created_mod,substr(do.created_time,11,9)) end created_time_mod 
            FROM
            (
                -- wms平台
                SELECT 
                    'B2C' TYPE
                    ,do.delivery_sn
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
                    ,do.`pack_time` pack_time
                    ,do.`start_receipt ` handover_time
                    ,date_add(do.`delivery_time`, interval -60 minute) delivery_time
                    ,if(locate('DO',do.express_sn)>0 or substring(do.express_sn,1,3)='LBX', 0, 1) is_time 
                    -- ,date_add(do.`delivery_time`, interval -60 minute) finish_delivery
        
                FROM `wms_production`.`delivery_order` do 
                LEFT JOIN `wms_production`.`seller_platform_source` sps on do.`platform_source_id`=sps.`id`
                LEFT JOIN `wms_production`.`platform_source` ps on sps.`platform_source_id`=ps.`id` 
                LEFT JOIN `wms_production`.`seller` sl on do.`seller_id`=sl.`id`
                LEFT JOIN wms_production.warehouse w ON do.warehouse_id=w.id
                WHERE 1=1
                    and do.`created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 30 day), '+07:00', '+08:00')
                    AND do.`status` NOT IN ('1000','1010') -- 取消单
                    AND do.`platform_status`!=9
                    AND do.prompt NOT in (1,2,3,4)-- 剔除拦截
        
                UNION
                -- wms平台
                SELECT 
                    'B2B' TYPE
                    ,return_warehouse_sn
                    ,total_goods_num
                    ,warehouse_id
                    ,w.name warehouse_name
                    ,'B2B'platform_source
                    ,''seller_name
                    ,''seller_id
                    ,date_add(do.`created`, interval -60 minute) created_time
                    ,left(date_add(do.`created`, interval -60 minute), 10) created_date
                    ,date_add(do.`verify_time`, interval -60 minute) audit_time
                    ,left(date_add(do.`verify_time`, interval -60 minute), 10) audit_date
                    ,do.pack_time + interval -1 hour pack_time
                    ,date_add(do.`out_warehouse_time`, interval -60 minute) handover_time
                    ,date_add(do.`out_warehouse_time`, interval -60 minute) delivery_time
                    ,1 is_time
                    from  wms_production.return_warehouse do 
                    LEFT JOIN wms_production.warehouse w ON do.warehouse_id=w.id
                    WHERE prompt ='0' 
                    AND status>='1020'
                    and do.`created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 30 day), '+07:00', '+08:00')
                
                UNION
                -- erp平台
                select
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
                    ,do.pack_time + interval + 7 hour pack_time
                    ,do.delivery_time + interval -1 hour handover_time
                    ,do.delivery_time + interval -1 hour delivery_time
                    ,1 is_time
                    from
                        `erp_wms_prod`.`delivery_order` do
                    left join erp_wms_prod.platform_source ps on do.platform_source_id = ps.id
                    LEFT JOIN erp_wms_prod.seller sl on do.`seller_id`=sl.`id`
                    LEFT JOIN erp_wms_prod.warehouse w ON do.warehouse_id=w.id
                    WHERE do.`created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 30 day), '+07:00', '+08:00')
                        AND do.`status` NOT IN ('1000','1010') -- 取消单
                        AND w.name='BPL3-LIVESTREAM'
                        and do.status <> '3030'
                        -- AND do.`platform_status`!=9
                        -- AND do.prompt NOT in (1,2,3,4)-- 剔除拦截
                UNION
                -- erp平台
                SELECT 
                    'B2B' TYPE
                    ,outbound_sn
                    ,''
                    ,warehouse_id
                    ,w.name warehouse_name
                    ,'B2B'platform_source
                    ,s.name seller_name
                    ,do.seller_id
                    ,date_add(do.`created`, interval -60 minute) created_time
                    ,left(date_add(do.`created`, interval -60 minute), 10) created_date
                    ,date_add(do.`created`, interval -60 minute) audit_time
                    ,left(date_add(do.confirm_time , interval + 7 hour), 10) audit_date
                    ,do.confirm_time + interval + 7 hour    pack_time
                    ,date_add(do.confirm_time , interval + 7 hour) handover_time
                    ,date_add(do.confirm_time , interval + 7 hour) delivery_time
                    ,1 is_time
                    from  erp_wms_prod.outbound_order do 
                    LEFT JOIN erp_wms_prod.warehouse w ON do.warehouse_id=w.id
                    left join `erp_wms_prod`.`seller` s on do.seller_id = s.id
                    WHERE do.status > '0'
                    AND w.name='BPL3-LIVESTREAM'
                    and do.`created` > convert_tz(date_sub(date(now() + interval -1 hour),interval 30 day), '+07:00', '+08:00') 
            ) do 
            left join 
            -- 日历调整// created_mod 是节假日顺延后首日,date1是节假日顺延后第二天
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
                    (
                        select 
                            date 
                        from 
                        tmpale.ods_th_dim_date 
                        where date between date_sub(date(now() + interval -1 hour),interval 180 day) and date_add(date(now() + interval -1 hour), interval 21 day)
                    ) calendar 
                    left join
                    -- 假日表
                    (
                        select 
                            off_date 
                        from 
                        fle_staging.sys_holiday 
                        where deleted = 0 
                            and company_category='2' 
                            and off_date between date_sub(date(now() + interval -1 hour),interval 180 day) and date_add(date(now() + interval -1 hour), interval 21 day)
                        group by off_date
                    ) offdate on calendar.date=off_date
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
                        (
                            select 
                                date 
                            from 
                            tmpale.ods_th_dim_date 
                            where date between date_sub(date(now() + interval -1 hour),interval 180 day) and date_add(date(now() + interval -1 hour), interval 28 day)
                        ) d0
                        left join 
                        (
                            select 
                                off_date 
                            from 
                            fle_staging.sys_holiday 
                            where deleted = 0 
                                and company_category='2' 
                                and off_date between date_sub(date(now() + interval -1 hour),interval 180 day) and date_add(date(now() + interval -1 hour), interval 28 day)
                            group by off_date
                        ) on date=off_date 
                        where off_date is null
                    )workdate on calendar.date>=workdate.date0 and calendar.date<workdate.date
                    where date0 is not null
                ) tca
            ) calendar on calendar.created=do.created_date
        )do
    )
    where warehouse_name IS NOT NULL 
)
SELECT 
    '未及时发货' 指标
    ,type
    ,platform_source
    ,seller_name
    ,warehouse_name
    ,delivery_sn
    ,if(及时发货=1, '及时', '未及时') 是否及时
    ,created_time
    ,audit_time
    ,pack_time
    ,handover_time
    ,delivery_time
    ,deadline
FROM EF
where  1=1
    and 应发货=1 
    and 及时发货=0
    and is_time=1
    -- and LEFT(deadline,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
    and LEFT(deadline,10) = date_sub(date(now() + interval -1 hour),interval 1 day)


union
SELECT 
    '未审核' 指标
    ,type
    ,platform_source
    ,seller_name
    ,warehouse_name
    ,delivery_sn
    ,if(及时发货=1, '及时', '未及时') 是否及时
    ,created_time
    ,audit_time
    ,pack_time
    ,handover_time
    ,delivery_time
    ,deadline
    FROM EF
    where 1=1
    -- and LEFT(created_time,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
    and LEFT(created_time,10) = date_sub(date(now() + interval -1 hour),interval 1 day)
    AND  audit_time IS  NULL