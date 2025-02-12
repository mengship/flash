with dwd_th_ffm_outbound_dayV2 as 
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
                when warehouse_name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse')   then 'BPL3'
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
            ,out_operator
            
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
                ,do.out_operator
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
                    ,if(locate('DO',do.express_sn)>0 or substring(do.express_sn,1,3)='LBX' or do.is_presale=1 or t0.order_sn is not null or substring(do.express_name,1,3)='SPX', 0, 1) is_time -- do.is_presale=1 预售单不参与时效考核
                    ,case when do.is_presale=1 then '预售单'
                        when t0.order_sn is not null then '曾缺货订单'
                        when locate('DO',do.express_sn)>0 then 'DO快递单号'
                        when substring(do.express_sn,1,3)='LBX' then 'LBX快递单号'
                        when substring(do.express_name,1,3)='SPX' then 'SPX快递单号'
                        else '正常时效单'
                        end as no_istime_type
                    ,do.express_name
                    , wd.id is_tiktok
                    ,do.operator_id out_operator
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
                        -- and `created`>='2023-12-01'
                ) t0 on do.delivery_sn = t0.order_sn
                left join wms_production.`delivery_order_mark_relation` domr on domr.delivery_order_id = do.id
                left join (select id from wms_production.wordbook_detail where `wordbook_id` = 10 and (zh = 'TT3' or zh='TT')) wd on domr.mark_id=wd.id
                WHERE 1=1
                    and do.`created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 90 day), '+07:00', '+08:00')
                    -- and do.`created` >= '2023-12-01'
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
                    ,null out_operator
                from  wms_production.return_warehouse do 
                LEFT JOIN wms_production.warehouse w ON do.warehouse_id=w.id
                LEFT JOIN `wms_production`.`seller` sl on do.`seller_id`=sl.`id`
                left join wms_production.logistic_company ul on do.logistic_company_id = ul.id
                left join wms_production.usable_logistic_company ulc on ul.usable_logistic_company_id  = ulc.id
                WHERE 1=1
                    and prompt ='0' 
                    AND status>='1020'
                    and do.`created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 90 day), '+07:00', '+08:00')
                    -- and do.`created` >= '2023-12-01'
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
                    ,ol.operation_id out_operator
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
                        -- and `created` >='2023-12-01'
                ) t0 on do.delivery_sn = t0.order_sn
                left join
                (select obj_id from erp_wms_prod.prompts where type = 1 and prompts in (13, 14) and warehouse_id = 312) p on p.obj_id = do.id
                left join 
                (
                    select
                          t_in.order_sn
                          ,t_in.operation_id
                          ,t_in.real_name
                      from
                      (
                          select
                              ol.order_sn
                              ,ol.operation_id
                              ,m.real_name real_name
                              ,ol.created
                              ,row_number() over(partition by ol.order_sn order by ol.created desc) rn
                          from
                          erp_wms_prod.operation_log ol
                          left join erp_wms_prod.member m on ol.operation_id = m.id
                          where 1=1
                              and order_type = 'DeliveryOrder' -- 发货单
                              and status_after=2060 -- 拣货完成
                              and operation='delivery'
                              and ol.`created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 90 day), '+07:00', '+08:00')
                              -- and ol.`created` >='2023-12-01'
                      ) t_in
                      where rn = 1
                ) ol on do.delivery_sn = ol.order_sn
                WHERE 1=1
                	and do.`created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 90 day), '+07:00', '+08:00')
                    -- and do.`created` >='2023-12-01'
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
                    ,null out_operator
                from  erp_wms_prod.outbound_order do 
                LEFT JOIN erp_wms_prod.warehouse w ON do.warehouse_id=w.id
                left join `erp_wms_prod`.`seller` s on do.seller_id = s.id
                left join erp_wms_prod.outbound_register or1 on do.id = or1.outbound_id
                WHERE do.status > '0'
                    AND w.name='BPL3-LIVESTREAM'
                    and do.`created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 90 day), '+07:00', '+08:00')
                    -- and do.`created` >='2023-12-01'
                    and s.name not in ('FFM-TH', 'Flash -Thailand') -- 剔除物料和资产
            ) do 
            left join 
            -- 日历调整// created_mod 是节假日顺延后首日,date1是节假日顺延后第二天
            dwm.dim_th_default_timeV2 calendar on calendar.created=do.created_date
        )do
    )
    where warehouse_name IS NOT NULL 
)

select
    warehouse_name
    ,now_date
    ,now_hour
    ,title_name
    ,operator
    ,user_name
    ,pickNum
from
(
    -- AGV 拣货
    select 
        'AGV' warehouse_name
        ,now_date
        ,now_hour
        ,title_name
        ,operator
        ,user_name
        ,pickNum
    from
    (
        SELECT
            -- left(oobt.gmt_modified,10) date
            '拣货' title_name
            -- ,oobt.operator user_id
            -- ,mo.job_number
            -- ,si.name
            ,bau.work_no operator
            ,bau.user_name
            ,date(oobt.gmt_modified) now_date
            ,hour(oobt.gmt_modified) now_hour
            -- ,count(DISTINCT ( oobt.order_code )) pickOrder
            ,sum(case when  wobo.type in(1)  then actual_num end)pickNum -- 2,3是ToB
            -- ,sum(actual_num) pickNum
        FROM was.oub_out_bound_task oobt
        LEFT JOIN was.base_authority_user bau ON oobt.operator = bau.user_id
        LEFT JOIN was.was_out_bound_order wobo  on  wobo.order_code = CONVERT(oobt.order_code using gbk) and oobt.group_id = wobo.group_id
        left join wms_production.member mo on oobt.operator = mo.id
        LEFT JOIN `fle_staging`.`staff_info` si on si.`id` =oobt.operator
        LEFT JOIN wms_production.seller sl on oobt.owner_id=sl.id
        WHERE 1=1
            and oobt.del_flag = 0 -- 未删除
            AND oobt.status = 1
            AND oobt.group_id = 1180 -- AGV仓
            AND oobt.type in (1,5)
            and sl.name not in ('FFM-TH') -- 剔除物料
            /* and oobt.gmt_modified >= convert_tz(CURRENT_DATE, '+07:00', '+08:00') */
            and date(oobt.gmt_modified)>='2024-09-09'
            and date(oobt.gmt_modified)<='2024-09-10'
        GROUP BY 1,2,3,4,5
    ) pick

    union
    -- AGV 打包
    select
        'AGV' warehouse_name
        ,now_date
        ,now_hour
        ,title_name
        ,creator
        ,user_name
        ,packNum
    from
    (
        SELECT
            '打包' title_name
            ,bau.work_no creator
            ,bau.user_name
            -- left(wsg.operation_time,10)date
            -- ,wsg.creator user_id
            -- ,count(DISTINCT ( wsg.relevance_code )) packOrder
            -- ,sum(item_num) packNum
            ,date(wsg.operation_time) now_date
            ,hour(wsg.operation_time) now_hour
            ,sum(case when wobo.type in(1) then item_num END)packNum 
        FROM was.was_status_group wsg
        LEFT JOIN was.base_authority_user bau ON wsg.creator = bau.user_id
        left join was.was_package wp on wsg.relevance_code = wp.order_code
        left join was.was_out_bound_order wobo  on  wobo.order_code = wp.order_code and wp.group_id = wobo.group_id
        LEFT JOIN wms_production.seller sl on wp.owner_id=sl.id
        WHERE 1=1
            and wsg.status = 'FINISH_PACK'
            AND wsg.del_flag = 0
            AND wsg.type = 1
            and sl.name not in ('FFM-TH') -- 剔除物料
            # AND wsg.group_id = 1180
            -- AND left(wsg.operation_time,7)=left(date_sub(date_add(now(),interval -60 minute),interval 1 month),7) -- @todo
            -- and wsg.operation_time >= convert_tz(CURRENT_DATE, '+07:00', '+08:00')
            and date(wsg.operation_time) >= '2024-09-09'
            and date(wsg.operation_time) <= '2024-09-10'
        GROUP BY 1, 2 ,3 ,4 ,5
    ) pack

    union
    -- AGV 出库
    select
        'AGV' warehouse_name
        ,now_date
        ,now_hour
        ,out.title_name
        ,out.out_operator
        ,out.real_name
        ,out.outnum

    from
    (
        select 
            '出库' title_name
            ,mb.job_number out_operator
            ,mb.real_name
            ,date(do.handover_time) now_date
            ,hour(do.handover_time) now_hour
            ,count (distinct do.delivery_sn) outnum
        from dwd_th_ffm_outbound_dayV2 do
        LEFT JOIN `wms_production`.`member` mb on do.out_operator=mb.`id`
        where 1=1
            and do.warehouse_id='36'
            and 'B2C'=do.TYPE
            and date(do.handover_time) >= '2024-09-09'
            and date(do.handover_time) <= '2024-09-10'
        group by 1,2,3,4,5
    ) out

    -- 'BPL-Return', 'BST', 'LAS' 拣货
    union
    select
        warehouse_name
        ,t_out.now_date
        ,t_out.now_hour
        ,t_out.title_name
        ,t_out.picker_id
        ,t_out.real_name
        ,t_out.picknum
    from
    (
        select
            '拣货' title_name
            ,warehouse_name
            ,picker_id
            ,real_name
            ,date(created ) now_date
            ,hour(created ) now_hour
            ,sum(picknum) picknum
        from
        (
            select 
                '拣货' title_name
                ,case when w.name='AutoWarehouse'   then 'AGV'
                    when w.name='BPL-Return Warehouse'  then 'BPL-Return'
                    when w.name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
                    when w.name='BangsaoThong'  then 'BST'
                    when w.name IN ('BKK-WH-LAS2电商仓')    then 'LAS'
                    when w.name ='LCP Warehouse' then 'LCP' end warehouse_name
                ,m.job_number picker_id
                ,m.real_name
                -- ,do.goods_num picknum
                ,jg.num picknum
                ,po.pick_sn
                ,jg.created + interval -1 hour created
                ,po.type
            from `wms_production`.`pick_order` po 
            left join (select from_order_id,task_sn from wms_production.task where task_type=3) t on po.id = t.from_order_id
            left join wms_production.job j on t.task_sn = j.task_sn
            left join wms_production.job_goods jg on jg.job_id = j.id
            left join `wms_production`.`warehouse` w on po.warehouse_id = w.id
            LEFT JOIN `wms_production`.`member` m on po.picker_id = m.id
            where 1=1
                and date(jg.created + interval -1 hour) >= '2024-09-09'
                and date(jg.created + interval -1 hour) <= '2024-09-10'
                /* and do.status >= 2030 
                and do.status <= 3020  */
                and po.type = 1 
                /* and 'PL2408207766'=po.pick_sn */
                /* and po.picker_id is not null */
        )
        group by 1,2,3,4,5,6
    ) t_out
    where 1=1
        and warehouse_name in ('BPL-Return', 'BST', 'LAS')

    union
    -- 'BPL-Return', 'BST', 'LAS'  打包
    select
        warehouse_name
        ,t_out.now_date
        ,t_out.now_hour
        ,t_out.title_name
        ,t_out.creator_id
        ,t_out.real_name
        ,t_out.packnum
    from
    (
        select
            '打包' title_name
            ,case when w.name='AutoWarehouse'   then 'AGV'
                when w.name='BPL-Return Warehouse'  then 'BPL-Return'
                when w.name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
                when w.name='BangsaoThong'  then 'BST'
                when w.name IN ('BKK-WH-LAS2电商仓')    then 'LAS'
                when w.name ='LCP Warehouse' then 'LCP' end warehouse_name
            ,db.creator_id
            ,db.real_name
            ,date(do.pack_time) now_date
            ,hour(do.pack_time) now_hour
            ,sum(do.goods_num) packnum
        from
        (
            select
                db.delivery_order_id,
                m.job_number pack_id,
                db.warehouse_id,
                m.job_number creator_id,
                m.real_name,
                count(1) as boxNum
            from
                wms_production.delivery_box db
                LEFT JOIN `wms_production`.`member` m on db.creator_id = m.id
                where 1=1 
                -- date(db.created)>= 
                -- date(db.created + interval -1 hour) >= CURRENT_DATE
                and date(db.created + interval -1 hour)>='2024-09-01'
            group by
                delivery_order_id,
                db.warehouse_id,
                db.pack_id,
                db.creator_id
        ) db
        left join `wms_production`.`delivery_order` do on do.id = db.delivery_order_id
        left join `wms_production`.`warehouse` w on db.warehouse_id = w.id
        where date(do.pack_time)>='2024-09-09'
            and date(do.pack_time)<='2024-09-10'
        group by 1,2,3,4,5,6
    )t_out where 1=1
        and warehouse_name in ('BPL-Return', 'BST', 'LAS')

    union
    -- 'BPL-Return', 'BST', 'LAS', 'BPL3' 出库
    select
        out.warehouse_name
        ,out.now_date
        ,out.now_hour
        ,out.title_name
        ,out.out_operator
        ,out.real_name
        ,out.outnum
    from
    (
        select 
            '出库' title_name
            ,warehouse_name
            ,mb.job_number out_operator
            ,mb.real_name
            ,date(do.handover_time) now_date
            ,hour(do.handover_time) now_hour
            ,count(distinct do.delivery_sn) outnum
        from dwd_th_ffm_outbound_dayV2 do
        LEFT JOIN `wms_production`.`member` mb on do.out_operator=mb.`id`
        where 1=1
            and do.warehouse_name in ('BPL-Return', 'BST', 'LAS', 'BPL3')
            and 'B2C'=do.TYPE
            -- and left (do.delivery_time,7)=left(date_sub(date_add(now(),interval -60 minute),interval 1 month),7) -- @todo
            -- and date(do.delivery_time) >= CURRENT_DATE
            and date(do.handover_time)>='2024-09-09'
            and date(do.handover_time)<='2024-09-10'
        group by 1,2,3,4,5,6
    ) out

    union

    -- 'BPL3' 拣货
    select
        warehouse_name
        ,out.now_date
        ,out.now_hour
        ,title_name
        ,operation_id
        ,real_name
        ,picknum
    from
    (
        select 
            do.warehouse_name
            ,'拣货' title_name
            ,ol.operation_id
            ,ol.real_name
            ,date(ol.created) now_date
            ,hour(ol.created) now_hour
            ,sum(do.goods_num) picknum
        from dwd_th_ffm_outbound_dayV2 do
        left join
        (
            select
                t_in.order_sn
                ,t_in.operation_id
                ,t_in.real_name
                ,t_in.created
            from
            (
                select
                    ol.order_sn
                    ,m.job_number operation_id
                    ,m.real_name real_name
                    ,ol.created + interval -1 hour created
                    ,row_number() over(partition by ol.order_sn order by ol.created desc) rn
                from
                erp_wms_prod.operation_log ol
                left join erp_wms_prod.member m on ol.operation_id = m.id
                where 1=1
                    and order_type = 'DeliveryOrder' -- 发货单
                    and status_after=2030 -- 拣货完成
                    and operation='confirmPick'
                    and ol.`created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 90 day), '+07:00', '+08:00')
                    -- and ol.`created` >='2023-12-01'
            ) t_in
            where rn = 1
        ) ol on do.delivery_sn = ol.order_sn
        where 1=1
            and do.warehouse_name='BPL3' 
            and date(ol.created) >= '2024-09-09'
            and date(ol.created) <= '2024-09-10'
        group by 1,2,3,4,5,6
    ) out

    union

    -- 'BPL3' 打包
    select
        warehouse_name
        ,out.now_date
        ,out.now_hour
        ,title_name
        ,operation_id
        ,real_name
        ,picknum
    from
    (
        select 
            do.warehouse_name
            ,'打包' title_name
            ,ol.operation_id
            ,ol.real_name
            ,date(ol.created) now_date
            ,hour(ol.created) now_hour
            ,sum(do.goods_num) picknum
        from dwd_th_ffm_outbound_dayV2 do
        left join
        (
            select
                t_in.order_sn
                ,t_in.operation_id
                ,t_in.real_name
                ,t_in.created
            from
            (
                select
                    ol.order_sn
                    ,m.job_number operation_id
                    ,m.real_name real_name
                    ,ol.created + interval -1 hour created
                    ,row_number() over(partition by ol.order_sn order by ol.created desc) rn
                from
                erp_wms_prod.operation_log ol
                left join erp_wms_prod.member m on ol.operation_id = m.id
                where 1=1
                    and order_type = 'DeliveryOrder' -- 发货单
                    and status_after=2040 -- 打包完成
                    and operation='packageFinish'
                    -- and ol.`created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 90 day), '+07:00', '+08:00')
                    and ol.`created` >='2023-12-01'
            ) t_in
            where rn = 1
        ) ol on do.delivery_sn = ol.order_sn
        where 1=1
            and do.warehouse_name='BPL3' 
            and date(ol.created)>='2024-09-09'
            and date(ol.created)<='2024-09-10'

            -- and left (do.delivery_time,7)=left(date_sub(date_add(now(),interval -60 minute),interval 1 month),7) -- @todo
            -- and date(ol.created) >= CURRENT_DATE
        group by 1,2,3,4
    ) out
) temp
order by 1,2,3,4,7 desc