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
                    and do.`created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 120 day), '+07:00', '+08:00')
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
                    and do.`created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 120 day), '+07:00', '+08:00')
                
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
                    WHERE do.`created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 120 day), '+07:00', '+08:00')
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
                    and do.`created` > convert_tz(date_sub(date(now() + interval -1 hour),interval 120 day), '+07:00', '+08:00') 
            ) do 
            left join 
            -- 日历调整// created_mod 是节假日顺延后首日,date1是节假日顺延后第二天
            dwm.dim_th_default_timeV2 calendar on calendar.created=do.created_date
        )do
    )
    where warehouse_name in ('AGV', 'BPL-Return', 'BPL3', 'BST', 'LAS')
)
,T1 as
(
    SELECT 
         仓库名称
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
                AND reg_time >= convert_tz(date_sub(date(now() + interval -1 hour),interval 120 day), '+07:00', '+08:00')

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
            AND arrival_time >= convert_tz(date_sub(date(now() + interval -1 hour),interval 120 day), '+07:00', '+07:00')

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
                where os.shelf_end_time >= convert_tz(date_sub(date(now() + interval -1 hour),interval 120 day), '+07:00', '+00:00')
                group by
                    os.from_order_sn
            ) os on an.notice_number = os.来源入库单号
            WHERE an.complete_time IS NOT NULL 
            AND an.status>='30' 
            AND an.complete_time >= convert_tz(date_sub(date(now() + interval -1 hour),interval 120 day), '+07:00', '+00:00')

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
            AND arrival_time >= convert_tz(date_sub(date(now() + interval -1 hour),interval 120 day), '+07:00', '+00:00')

        ) an

        left join 
        -- 日历调整// created_mod是节假日顺延后首日,date1是节假日顺延后第二天
        dwm.dim_th_default_timeV2 calendar on calendar.created=an.reg_date
        left join wms_production.warehouse w ON an.warehouse_id=w.id
    ) t0 
    where 仓库名称 in ('AGV', 'BPL-Return', 'BPL3', 'BST', 'LAS')
)
SELECT 
    '发货及时率' 类型
    ,warehouse_name
    ,LEFT(deadline,10) 日期
    ,sum(及时发货) 及时完成单量
    ,sum(应发货) 应完成单量
    ,count(delivery_sn) 创建单量
    ,sum(及时发货)/sum(应发货) 及时率
FROM EF
where date(deadline)>=left(now()- INTERVAL 70 day,10)
group by 1,2,3

union
SELECT 
    '24H采购入库及时率' TYPE
    ,仓库名称
    ,to_date(LEFT(reg_time,10))日期
    ,sum(及时入库)
    ,sum(应入库)
    ,count(notice_number)
    ,sum(及时入库)/sum(应入库)及时率
FROM T1
WHERE 1=1
    and LEFT(reg_time,10)>=left(now()- INTERVAL 70 day,10)
    and 单据='采购订单'
GROUP BY 1,2,3

UNION 
SELECT 
    '48H采购上架及时率' type
    ,仓库名称
    ,to_date(LEFT(reg_time,10))日期
    ,sum(及时上架)
    ,sum(应上架)
    ,count(notice_number)
    ,sum(及时上架)/sum(应上架)
FROM T1
WHERE 1=1
    and LEFT(reg_time,10)>=left(now()- INTERVAL 70 day,10)
    and 单据='采购订单'
GROUP BY 1,2,3

union
SELECT 
    '48H销退上架及时率' type
    ,仓库名称
    ,to_date(LEFT(reg_time,10))日期
    ,sum(及时上架)
    ,sum(应上架)
    ,count(notice_number)
    ,sum(及时上架)/sum(应上架)
FROM T1
WHERE 1=1
    and LEFT(reg_time,10)>=left(now()- INTERVAL 70 day,10)
    and 单据='销退订单'
GROUP BY 1,2,3