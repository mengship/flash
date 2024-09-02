with EF as
(
    SELECT 
        *
        ,case when TYPE='B2C' and pack_time<pack_deadline then 1
            else 0
            end as 及时打包
        ,case when TYPE='B2C' and audit_time is not null and (pack_time is not null OR pack_deadline< date_add(now(), interval -60 minute)) then 1
            else 0
            end as 应打包
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
        ,case when (pick_time > deadline) or ( pick_time is null and deadline< date_add(now(), interval -60 minute) ) then '拣货超时' 
            when (pack_time > deadline) or  ( pack_time is null and deadline< date_add(now(), interval -60 minute) ) then  '打包超时'
            when (handover_time > deadline) or ( handover_time is null and deadline< date_add(now(), interval -60 minute) ) then  '交接超时'
            when (delivery_time > deadline) or ( delivery_time is null and deadline< date_add(now(), interval -60 minute) ) then  '揽收超时'
            else '未超时'
            end as 超时节点
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
            ,pick_time
            ,pack_time
            ,handover_time
            ,delivery_time
            ,created_time_mod
            ,CASE WHEN platform_source='Shopee' AND substr(created_time_mod,12,2)<16 THEN concat(LEFT(created_time_mod,10),' 23:59:59')
                WHEN platform_source='Shopee' AND substr(created_time_mod,12,2)>=16 THEN concat(date1,' 23:59:59')
                WHEN platform_source='Tik Tok' AND substr(created_time_mod,12,2)<18 THEN concat(LEFT(created_time_mod,10),' 23:59:59')
                WHEN platform_source='Tik Tok' AND substr(created_time_mod,12,2)>=18 THEN concat(date1,' 23:59:59')
                -- when platform_source='LAZADA'  then concat(date1,substr(created_time_mod,11,9))
                ELSE concat(date1,substr(created_time_mod,11,9)) END pack_deadline

            ,CASE WHEN TYPE ='B2B' THEN concat(date2,substr(created_time_mod,11,9))
                WHEN platform_source='Shopee' AND substr(created_time_mod,12,2)<16 THEN concat(LEFT(created_time_mod,10),' 23:59:59')
                WHEN platform_source='Shopee' AND substr(created_time_mod,12,2)>=16 THEN concat(date1,' 23:59:59')
                WHEN platform_source='Tik Tok' AND substr(created_time_mod,12,2)<18 THEN concat(LEFT(created_time_mod,10),' 23:59:59')
                WHEN platform_source='Tik Tok' AND substr(created_time_mod,12,2)>=18 THEN concat(date1,' 23:59:59')
                WHEN platform_source='LAZADA' THEN concat(date2,substr(created_time_mod,11,9))
                ELSE concat(date1,substr(created_time_mod,11,9)) END deadline
            ,date_add(now(),interval -60 minute) ETL
            ,is_time
            ,no_istime_type
            ,express_name
        FROM
        (
            select
                do.TYPE
                ,do.delivery_sn
                ,do.goods_num 
                ,do.warehouse_id
                ,do.warehouse_name
                ,if(do.is_tiktok is not null, 'Tik Tok',do.platform_source) platform_source
                ,do.seller_name
                ,do.seller_id
                ,do.created_time
                ,do.created_date
                ,do.audit_time
                ,do.audit_date
                ,do.pick_time
                ,do.pack_time
                ,do.handover_time
                ,do.delivery_time
                ,do.is_time
                ,do.no_istime_type
                ,do.express_name
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
                    ,do.`succ_pick` pick_time
                    ,do.`pack_time` pack_time
                    ,do.`start_receipt ` handover_time
                    ,date_add(do.`delivery_time`, interval -60 minute) delivery_time
                    ,if(locate('DO',do.express_sn)>0 or substring(do.express_sn,1,3)='LBX' or do.is_presale=1 or t0.order_sn is not null, 0, 1) is_time -- do.is_presale=1 预售单不参与时效考核
                    ,case when do.is_presale=1 then '预售单'
                        when t0.order_sn is not null then '曾缺货订单'
                        when locate('DO',do.express_sn)>0 then 'DO快递单号'
                        when substring(do.express_sn,1,3)='LBX' then 'LBX快递单号'
                        else '正常时效单'
                        end as no_istime_type
                    ,do.express_name
                    , wd.id is_tiktok
                FROM `wms_production`.`delivery_order` do 
                LEFT JOIN `wms_production`.`seller_platform_source` sps on do.`platform_source_id`=sps.`id`
                LEFT JOIN `wms_production`.`platform_source` ps on sps.`platform_source_id`=ps.`id` 
                LEFT JOIN `wms_production`.`seller` sl on do.`seller_id`=sl.`id`
                LEFT JOIN wms_production.warehouse w ON do.warehouse_id=w.id
                left join
                (
                    select
                        order_sn 
                    from 
                    `wms_production`.operation_log 
                    where 1=1
                        and status_after='1010'
                        and `created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 90 day), '+07:00', '+08:00')
                ) t0 on do.delivery_sn = t0.order_sn
                left join wms_production.`delivery_order_mark_relation` domr on domr.delivery_order_id = do.id
                left join (select id from wms_production.wordbook_detail where `wordbook_id` = 10 and (zh = 'TT3' or zh='TT')) wd on domr.mark_id=wd.id
                WHERE 1=1
                    and do.`created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 90 day), '+07:00', '+08:00')
                    AND do.`status` NOT IN ('1000','1010') -- 取消单
                    AND do.`platform_status`!=9
                    AND do.prompt NOT in (1,2,3,4)-- 剔除拦截
                    and sl.name not in ('FFM-TH', 'Flash -Thailand') -- 剔除物料和资产
        
                UNION
                -- wms平台
                SELECT 
                    'B2B' TYPE
                    ,return_warehouse_sn
                    ,total_goods_num
                    ,do.warehouse_id
                    ,w.name warehouse_name
                    ,'B2B'platform_source
                    ,sl.name seller_name
                    ,''seller_id
                    ,date_add(do.`created`, interval -60 minute) created_time
                    ,left(date_add(do.`created`, interval -60 minute), 10) created_date
                    ,date_add(do.`verify_time`, interval -60 minute) audit_time
                    ,left(date_add(do.`verify_time`, interval -60 minute), 10) audit_date
                    ,do.picking_end_time
                    ,do.pack_time + interval -1 hour pack_time
                    ,date_add(do.`out_warehouse_time`, interval -60 minute) handover_time
                    ,date_add(do.`out_warehouse_time`, interval -60 minute) delivery_time
                    ,if(locate('DO',do.express_sn)>0 or substring(do.express_sn,1,3)='LBX', 0, 1) is_time
                    ,case 
                        when locate('DO',do.express_sn)>0 then 'DO快递单号'
                        when substring(do.express_sn,1,3)='LBX' then 'LBX快递单号'
                        else '正常时效单'
                        end as no_istime_type
                    ,ulc.name
                    ,null is_tiktok
                from  wms_production.return_warehouse do 
                LEFT JOIN wms_production.warehouse w ON do.warehouse_id=w.id
                LEFT JOIN `wms_production`.`seller` sl on do.`seller_id`=sl.`id`
                left join wms_production.logistic_company ul on do.logistic_company_id = ul.id
                left join wms_production.usable_logistic_company ulc on ul.usable_logistic_company_id  = ulc.id
                WHERE 1=1
                    and prompt ='0' 
                    AND status>='1020'
                    and do.`created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 90 day), '+07:00', '+08:00')
                    and sl.name not in ('FFM-TH', 'Flash -Thailand') -- 剔除物料和资产
                
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
                    ,do.succ_pick + interval + 7 hour pick_time
                    ,do.pack_time + interval + 7 hour pack_time
                    ,do.delivery_time + interval -1 hour handover_time
                    ,do.delivery_time + interval -1 hour delivery_time
                    ,if(locate('DO',do.express_sn)>0 or substring(do.express_sn,1,3)='LBX' or t0.order_sn is not null, 0, 1) is_time -- do.is_presale=1 预售单不参与时效考核 is_time
                    ,case -- when do.is_presale=1 then '预售单'
                        when t0.order_sn is not null then '曾缺货订单'
                        when locate('DO',do.express_sn)>0 then 'DO快递单号'
                        when substring(do.express_sn,1,3)='LBX' then 'LBX快递单号'
                        else '正常时效单'
                        end as no_istime_type
                    ,do.express_name
                    ,p.obj_id is_tiktok
                from
                    `erp_wms_prod`.`delivery_order` do
                left join erp_wms_prod.platform_source ps on do.platform_source_id = ps.id
                LEFT JOIN erp_wms_prod.seller sl on do.`seller_id`=sl.`id`
                LEFT JOIN erp_wms_prod.warehouse w ON do.warehouse_id=w.id
                left join
                (
                    select
                        order_sn 
                    from 
                    `erp_wms_prod`.operation_log 
                    where 1=1
                        and status_after='1010'
                        and `created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 90 day), '+07:00', '+08:00')
                ) t0 on do.delivery_sn = t0.order_sn
                left join
                (select obj_id from erp_wms_prod.prompts where type = 1 and prompts in (13, 14) and warehouse_id = 312) p on p.obj_id = do.id
                WHERE do.`created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 90 day), '+07:00', '+08:00')
                    AND do.`status` NOT IN ('1000','1010') -- 取消单
                    AND w.name='BPL3-LIVESTREAM'
                    and do.status <> '3030'
                    and sl.name not in ('FFM-TH', 'Flash -Thailand') -- 剔除物料和资产
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
                    ,do.confirm_time + interval + 7 hour    pick_time
                    ,do.confirm_time + interval + 7 hour    pack_time
                    ,date_add(do.confirm_time , interval + 7 hour) handover_time
                    ,date_add(do.confirm_time , interval + 7 hour) delivery_time
                    ,1 is_time
                    ,null no_istime_type
                    ,or1.carrier
                    ,null is_tiktok
                from  erp_wms_prod.outbound_order do 
                LEFT JOIN erp_wms_prod.warehouse w ON do.warehouse_id=w.id
                left join `erp_wms_prod`.`seller` s on do.seller_id = s.id
                left join erp_wms_prod.outbound_register or1 on do.id = or1.outbound_id
                WHERE do.status > '0'
                    AND w.name='BPL3-LIVESTREAM'
                    and do.`created` > convert_tz(date_sub(date(now() + interval -1 hour),interval 90 day), '+07:00', '+08:00')
                    and s.name not in ('FFM-TH', 'Flash -Thailand') -- 剔除物料和资产
            ) do 
            left join 
            -- 日历调整// created_mod 是节假日顺延后首日,date1是节假日顺延后第二天
            dwm.dim_th_default_timeV2 calendar on calendar.created=do.created_date
        )do
    )
    where warehouse_name IS NOT NULL 
)


SELECT 
 * 
from(
    SELECT 
        LEFT(created_time,10) 日期
        ,null paltform
        ,warehouse_name
        ,TYPE 单据
        ,'已审核单量' 指标
        ,COUNT(delivery_sn) 数值
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where  LEFT(created_time,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
    AND audit_time IS NOT NULL
    GROUP BY 1,2,3,4

    union 
    SELECT 
        LEFT(created_time,10) 日期
        ,null paltform
        ,warehouse_name
        ,type 单据
        ,'未审核单量' 指标
        ,COUNT(delivery_sn)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where LEFT(created_time,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
    AND  audit_time IS  NULL
    GROUP BY 1,2,3,4

    union 
    SELECT 
        LEFT(if(type='B2C', delivery_time, pack_time),10) 日期
        ,null paltform
        ,warehouse_name
        ,type 单据
        ,'商品数量' 指标
        ,sum(goods_num)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where LEFT(created_time,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
    and audit_time is not null
    GROUP BY 1,2,3,4

    UNION
    SELECT 
        LEFT(if(type='B2C', delivery_time, pack_time),10) 日期
        ,null paltform
        ,warehouse_name
        ,type 单据
        ,'出库单量' 指标
        ,COUNT(delivery_sn)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where LEFT(pack_time,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
    GROUP BY 1,2,3,4

    -- Shopee
    UNION 
    SELECT 
        LEFT(deadline,10) 日期
        ,'Shopee' paltform
        ,warehouse_name
        ,type 单据
        ,'及时发货' 指标
        ,sum(及时发货)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(deadline,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='Shopee'
    GROUP BY 1,2,3,4

    UNION 
    SELECT 
        LEFT(deadline,10) 日期
        ,'Shopee' paltform
        ,warehouse_name
        ,type 单据
        ,'应发货' 指标
        ,sum(应发货)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(deadline,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='Shopee'
    GROUP BY 1,2,3,4

    UNION 
    SELECT
        LEFT(deadline,10) 日期
        ,'Shopee' paltform
        ,warehouse_name
        ,type 单据
        ,'未及时发货' 指标
        ,count(delivery_sn)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where  1=1
        and 应发货=1 
        and 及时发货=0
        and is_time=1
        and platform_source='Shopee'
        and LEFT(deadline,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
    GROUP BY 1,2,3,4

    -- Tik Tok
    UNION 
    SELECT 
        LEFT(deadline,10) 日期
        ,'Tik Tok' paltform
        ,warehouse_name
        ,type 单据
        ,'及时发货' 指标
        ,sum(及时发货)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(deadline,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='Tik Tok'
    GROUP BY 1,2,3,4

    UNION 
    SELECT 
        LEFT(deadline,10) 日期
        ,'Tik Tok' paltform
        ,warehouse_name
        ,type 单据
        ,'应发货' 指标
        ,sum(应发货)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(deadline,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='Tik Tok'
    GROUP BY 1,2,3,4

    UNION 
    SELECT
        LEFT(deadline,10) 日期
        ,'Tik Tok' paltform
        ,warehouse_name
        ,type 单据
        ,'未及时发货' 指标
        ,count(delivery_sn)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where  1=1
        and 应发货=1 
        and 及时发货=0
        and is_time=1
        and platform_source='Tik Tok'
        and LEFT(deadline,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
    GROUP BY 1,2,3,4


    -- LAZADA
    UNION 
    SELECT 
        LEFT(deadline,10) 日期
        ,'LAZADA' paltform
        ,warehouse_name
        ,type 单据
        ,'及时发货' 指标
        ,sum(及时发货)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(deadline,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='LAZADA'
    GROUP BY 1,2,3,4

    UNION 
    SELECT 
        LEFT(deadline,10) 日期
        ,'LAZADA' paltform
        ,warehouse_name
        ,type 单据
        ,'应发货' 指标
        ,sum(应发货)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(deadline,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='LAZADA'
    GROUP BY 1,2,3,4

    UNION 
    SELECT
        LEFT(deadline,10) 日期
        ,'LAZADA' paltform
        ,warehouse_name
        ,type 单据
        ,'未及时发货' 指标
        ,count(delivery_sn)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where  1=1
        and 应发货=1 
        and 及时发货=0
        and is_time=1
        and platform_source='LAZADA'
        and LEFT(deadline,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
    GROUP BY 1,2,3,4

        -- Other
    UNION 
    SELECT 
        LEFT(deadline,10) 日期
        ,'Other' paltform
        ,warehouse_name
        ,type 单据
        ,'及时发货' 指标
        ,sum(及时发货)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(deadline,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='Other'
    GROUP BY 1,2,3,4

    UNION 
    SELECT 
        LEFT(deadline,10) 日期
        ,'Other' paltform
        ,warehouse_name
        ,type 单据
        ,'应发货' 指标
        ,sum(应发货)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(deadline,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='Other'
    GROUP BY 1,2,3,4

    UNION 
    SELECT
        LEFT(deadline,10) 日期
        ,'Other' paltform
        ,warehouse_name
        ,type 单据
        ,'未及时发货' 指标
        ,count(delivery_sn)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where  1=1
        and 应发货=1 
        and 及时发货=0
        and is_time=1
        and platform_source='Other'
        and LEFT(deadline,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
    GROUP BY 1,2,3,4

    -- 打包
    -- Shopee
    UNION 
    SELECT 
        LEFT(pack_deadline,10) 日期
        ,'Shopee' paltform
        ,warehouse_name
        ,type 单据
        ,'及时打包' 指标
        ,sum(及时打包)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(pack_deadline,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='Shopee'
    GROUP BY 1,2,3,4

    UNION 
    SELECT 
        LEFT(pack_deadline,10) 日期
        ,'Shopee' paltform
        ,warehouse_name
        ,type 单据
        ,'应打包' 指标
        ,sum(应打包)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(pack_deadline,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='Shopee'
    GROUP BY 1,2,3,4

    UNION 
    SELECT
        LEFT(pack_deadline,10) 日期
        ,'Shopee' paltform
        ,warehouse_name
        ,type 单据
        ,'未及时打包' 指标
        ,count(delivery_sn)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where  1=1
        and 应打包=1 
        and 及时打包=0
        and is_time=1
        and platform_source='Shopee'
        and LEFT(pack_deadline,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
    GROUP BY 1,2,3,4

    -- Tik Tok
    UNION 
    SELECT 
        LEFT(pack_deadline,10) 日期
        ,'Tik Tok' paltform
        ,warehouse_name
        ,type 单据
        ,'及时打包' 指标
        ,sum(及时打包)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(pack_deadline,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='Tik Tok'
    GROUP BY 1,2,3,4

    UNION 
    SELECT 
        LEFT(pack_deadline,10) 日期
        ,'Tik Tok' paltform
        ,warehouse_name
        ,type 单据
        ,'应打包' 指标
        ,sum(应打包)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(pack_deadline,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='Tik Tok'
    GROUP BY 1,2,3,4

    UNION 
    SELECT
        LEFT(pack_deadline,10) 日期
        ,'Tik Tok' paltform
        ,warehouse_name
        ,type 单据
        ,'未及时打包' 指标
        ,count(delivery_sn)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where  1=1
        and 应打包=1 
        and 及时打包=0
        and is_time=1
        and platform_source='Tik Tok'
        and LEFT(pack_deadline,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
    GROUP BY 1,2,3,4


    -- LAZADA
    UNION 
    SELECT 
        LEFT(pack_deadline,10) 日期
        ,'LAZADA' paltform
        ,warehouse_name
        ,type 单据
        ,'及时打包' 指标
        ,sum(及时打包)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(pack_deadline,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='LAZADA'
    GROUP BY 1,2,3,4

    UNION 
    SELECT 
        LEFT(pack_deadline,10) 日期
        ,'LAZADA' paltform
        ,warehouse_name
        ,type 单据
        ,'应打包' 指标
        ,sum(应打包)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(pack_deadline,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='LAZADA'
    GROUP BY 1,2,3,4

    UNION 
    SELECT
        LEFT(pack_deadline,10) 日期
        ,'LAZADA' paltform
        ,warehouse_name
        ,type 单据
        ,'未及时打包' 指标
        ,count(delivery_sn)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where  1=1
        and 应打包=1 
        and 及时打包=0
        and is_time=1
        and platform_source='LAZADA'
        and LEFT(pack_deadline,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
    GROUP BY 1,2,3,4

    -- Other
    UNION 
    SELECT 
        LEFT(pack_deadline,10) 日期
        ,'Other' paltform
        ,warehouse_name
        ,type 单据
        ,'及时打包' 指标
        ,sum(及时打包)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(pack_deadline,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='Other'
    GROUP BY 1,2,3,4

    UNION 
    SELECT 
        LEFT(pack_deadline,10) 日期
        ,'Other' paltform
        ,warehouse_name
        ,type 单据
        ,'应打包' 指标
        ,sum(应打包)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(pack_deadline,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='Other'
    GROUP BY 1,2,3,4

    UNION 
    SELECT
        LEFT(pack_deadline,10) 日期
        ,'Other' paltform
        ,warehouse_name
        ,type 单据
        ,'未及时打包' 指标
        ,count(delivery_sn)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where  1=1
        and 应打包=1 
        and 及时打包=0
        and is_time=1
        and platform_source='Other'
        and LEFT(pack_deadline,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
    GROUP BY 1,2,3,4

) t_out
order by 1,2,3,4


############################################################################ report ###############################################################################################

SELECT 
    *
    ,case when TYPE='B2C' and pack_time<pack_deadline then 1
        else 0
        end as 及时打包
    ,case when TYPE='B2C' and audit_time is not null and (pack_time is not null OR pack_deadline< date_add(now(), interval -60 minute)) then 1
        else 0
        end as 应打包
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
    ,case when pick_time > deadline then '拣货超时' 
        when pack_time > deadline then  '打包超时'
        when handover_time > deadline then  '交接超时'
        when delivery_time > deadline then  '揽收超时'
        else '未超时'
        end as 超时节点
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
        ,pick_time
        ,pack_time
        ,handover_time
        ,delivery_time
        ,created_time_mod
        ,CASE WHEN platform_source='Shopee' AND substr(created_time_mod,12,2)<16 THEN concat(LEFT(created_time_mod,10),' 23:59:59')
            WHEN platform_source='Shopee' AND substr(created_time_mod,12,2)>=16 THEN concat(date1,' 23:59:59')
            WHEN platform_source='Tik Tok' AND substr(created_time_mod,12,2)<18 THEN concat(LEFT(created_time_mod,10),' 23:59:59')
            WHEN platform_source='Tik Tok' AND substr(created_time_mod,12,2)>=18 THEN concat(date1,' 23:59:59')
            -- when platform_source='LAZADA'  then concat(date1,substr(created_time_mod,11,9))
            ELSE concat(date1,substr(created_time_mod,11,9)) END pack_deadline

        ,CASE WHEN TYPE ='B2B' THEN concat(date2,substr(created_time_mod,11,9))
            WHEN platform_source='Shopee' AND substr(created_time_mod,12,2)<16 THEN concat(LEFT(created_time_mod,10),' 23:59:59')
            WHEN platform_source='Shopee' AND substr(created_time_mod,12,2)>=16 THEN concat(date1,' 23:59:59')
            WHEN platform_source='Tik Tok' AND substr(created_time_mod,12,2)<18 THEN concat(LEFT(created_time_mod,10),' 23:59:59')
            WHEN platform_source='Tik Tok' AND substr(created_time_mod,12,2)>=18 THEN concat(date1,' 23:59:59')
            WHEN platform_source='LAZADA' THEN concat(date2,substr(created_time_mod,11,9))
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
            ,do.pick_time
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
                ,do.`succ_pick` pick_time
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
                and sl.name not in ('FFM-TH', 'Flash -Thailand') -- 剔除物料和资产
    
            UNION
            -- wms平台
            SELECT 
                'B2B' TYPE
                ,return_warehouse_sn
                ,total_goods_num
                ,warehouse_id
                ,w.name warehouse_name
                ,'B2B'platform_source
                ,sl.name seller_name
                ,''seller_id
                ,date_add(do.`created`, interval -60 minute) created_time
                ,left(date_add(do.`created`, interval -60 minute), 10) created_date
                ,date_add(do.`verify_time`, interval -60 minute) audit_time
                ,left(date_add(do.`verify_time`, interval -60 minute), 10) audit_date
                ,do.picking_end_time
                ,do.pack_time + interval -1 hour pack_time
                ,date_add(do.`out_warehouse_time`, interval -60 minute) handover_time
                ,date_add(do.`out_warehouse_time`, interval -60 minute) delivery_time
                ,1 is_time
            from  wms_production.return_warehouse do 
            LEFT JOIN wms_production.warehouse w ON do.warehouse_id=w.id
            LEFT JOIN `wms_production`.`seller` sl on do.`seller_id`=sl.`id`
            WHERE 1=1
                and prompt ='0' 
                AND status>='1020'
                and do.`created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 30 day), '+07:00', '+08:00')
                and sl.name not in ('FFM-TH', 'Flash -Thailand') -- 剔除物料和资产
            
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
                ,do.succ_pick + interval + 7 hour pick_time
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
                and sl.name not in ('FFM-TH', 'Flash -Thailand') -- 剔除物料和资产
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
                ,do.confirm_time + interval + 7 hour    pick_time
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
                and s.name not in ('FFM-TH', 'Flash -Thailand') -- 剔除物料和资产
        ) do 
        left join 
        -- 日历调整// created_mod 是节假日顺延后首日,date1是节假日顺延后第二天
        dwm.dim_th_default_timeV2 calendar on calendar.created=do.created_date
    )do
)
where warehouse_name IS NOT NULL 