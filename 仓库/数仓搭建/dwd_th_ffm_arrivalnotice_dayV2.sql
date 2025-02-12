/*=====================================================================+
表名称：  dwd_th_ffm_arrivalnotice_dayV2
功能描述：泰国入库明细表
                        
需求来源：
编写人员: 王昱棋
设计日期：2024/8/23
        修改日期: 
        修改人员:            
        修改原因: 
-----------------------------------------------------------------------
---存在问题：
-----------------------------------------------------------------------
+=====================================================================*/ 
      

-- 入库单明细表
-- drop table if exists dwm.dwd_th_ffm_arrivalnotice_dayV2;
-- create table dwm.dwd_th_ffm_arrivalnotice_dayV2 as
delete from dwm.dwd_th_ffm_arrivalnotice_dayV2 where reg_time >= date_sub(date(now() + interval -1 hour),interval 120 day); -- 先删除数据
insert into dwm.dwd_th_ffm_arrivalnotice_dayV2 -- 再插入数据

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
    ,case 
        when (complete_time is not null and complete_time <= concat(date1,substr(reg_time_mod,11,9))) then '及时入库'
        when (complete_time is null and concat(date1,substr(reg_time_mod,11,9)) < now() + interval -1 hour ) or (complete_time is not null and complete_time > concat(date1,substr(reg_time_mod,11,9))) then '未及时入库'
        when (complete_time is null and concat(date1,substr(reg_time_mod,11,9)) >= now() + interval -1 hour) then '未到入库考核时间'
        end as completetimetype
    ,case 
        when (shelf_complete_time is not null and shelf_complete_time <= concat(date1,substr(reg_time_mod,11,9))) then '及时上架'
        when (shelf_complete_time is null and concat(date1,substr(reg_time_mod,11,9)) < now() + interval -1 hour ) or (shelf_complete_time is not null and shelf_complete_time > concat(date1,substr(reg_time_mod,11,9))) then '未及时上架'
        when (shelf_complete_time is null and concat(date1,substr(reg_time_mod,11,9)) >= now() + interval -1 hour) then '未到入库考核时间'
        end as shelfompletetimetype
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
            ,case when w.name in ('AutoWarehouse', 'AutoWarehouse-人工仓') then 'AGV'
                when w.name='BPL-Return Warehouse' then 'BPL-Return'
                when w.name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
                when w.name='BangsaoThong' then 'BST'
                when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS' # ,'PMD-WH','BKK-WH-Ecommerce','BKK-WH-LAS物料仓'
                when w.name='LCP Warehouse' then 'LCP' end 仓库名称
            ,sl.name seller_name
            ,'' shelf_status
            ,reg_time - interval 1 hour reg_time
            ,left(reg_time - interval 1 hour,10) reg_date
            ,complete_time
            ,if(w.name='AutoWarehouse', irb.finish_date, shelf_complete_time) shelf_complete_time
        FROM wms_production.arrival_notice an
        left join (select receive_external_no,finish_date from was.inb_receive_bill where is_deleted=0 and create_time >= convert_tz(date_sub(date(now() + interval -1 hour),interval 130 day), '+07:00', '+08:00') )irb on an.notice_number = irb.receive_external_no
        left join wms_production.warehouse w ON an.warehouse_id=w.id
        LEFT JOIN `wms_production`.`seller` sl on an.`seller_id`=sl.`id`
            WHERE reg_time IS NOT NULL 
            AND status>='30' 
            AND reg_time >= convert_tz(date_sub(date(now() + interval -1 hour),interval 120 day), '+07:00', '+08:00')
            and reg_time>='2023-12-01'

        UNION 
        SELECT 
            back_sn
            ,"销退订单" 单据
            ,warehouse_id
            ,case when w.name in ('AutoWarehouse', 'AutoWarehouse-人工仓') then 'AGV'
                when w.name='BPL-Return Warehouse' then 'BPL-Return'
                when w.name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
                when w.name='BangsaoThong' then 'BST'
                when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS' # ,'PMD-WH','BKK-WH-Ecommerce','BKK-WH-LAS物料仓'
                when w.name='LCP Warehouse' then 'LCP' end 仓库名称
            ,sl.name seller_name
            ,shelf_status
            ,arrival_time arrival_time
            ,left(arrival_time ,10) arrival_date
            ,complete_time
            ,if(w.name='AutoWarehouse', irb.finish_date, dro.shelf_end_time) shelf_end_time
        FROM wms_production.delivery_rollback_order dro
        left join (select receive_external_no,finish_date from was.inb_receive_bill where is_deleted=0 and create_time >= convert_tz(date_sub(date(now() + interval -1 hour),interval 130 day), '+07:00', '+08:00') )irb on dro.back_sn = irb.receive_external_no
        left join wms_production.warehouse w ON dro.warehouse_id=w.id
        LEFT JOIN `wms_production`.`seller` sl on dro.`seller_id`=sl.`id`
        WHERE arrival_time IS NOT NULL AND status >= '1045' AND STATUS<>'9000'
          AND back_express_status NOT IN('20','30')
          AND arrival_time >= convert_tz(date_sub(date(now() + interval -1 hour),interval 120 day), '+07:00', '+07:00')
          and arrival_time>='2023-12-01'

        UNION
        select
            an.notice_number
            ,"采购订单" 单据
            ,warehouse_id
            ,case when w.name in ('AutoWarehouse', 'AutoWarehouse-人工仓') then 'AGV'
                when w.name='BPL-Return Warehouse' then 'BPL-Return'
                when w.name='BPL3-LIVESTREAM' OR warehouse_id='312' then 'BPL3'
                when w.name='BangsaoThong' then 'BST'
                when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS' # ,'PMD-WH','BKK-WH-Ecommerce','BKK-WH-LAS物料仓'
                when w.name='LCP Warehouse' then 'LCP' end 仓库名称
            ,sl.name seller_name
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
            where 1=1
                and os.shelf_end_time >= convert_tz(date_sub(date(now() + interval -1 hour),interval 120 day), '+07:00', '+00:00')
                and os.shelf_end_time >= '2023-12-01'
            group by
                os.from_order_sn
        ) os on an.notice_number = os.来源入库单号
        left join erp_wms_prod.warehouse w on an.warehouse_id = w.id
        LEFT JOIN erp_wms_prod.seller sl on an.`seller_id`=sl.`id`
        WHERE an.complete_time IS NOT NULL 
        AND an.status>='30' 
        and w.name='BPL3-LIVESTREAM'
        AND an.complete_time >= convert_tz(date_sub(date(now() + interval -1 hour),interval 120 day), '+07:00', '+00:00')
        and an.complete_time>='2023-12-01'

        UNION
        SELECT rollback_sn
            ,"销退订单" 单据
            ,warehouse_id
            ,case when w.name in ('AutoWarehouse', 'AutoWarehouse-人工仓') then 'AGV'
                when w.name='BPL-Return Warehouse' then 'BPL-Return'
                when w.name='BPL3-LIVESTREAM' OR warehouse_id='312' then 'BPL3'
                when w.name='BangsaoThong' then 'BST'
                when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS' # ,'PMD-WH','BKK-WH-Ecommerce','BKK-WH-LAS物料仓'
                when w.name='LCP Warehouse' then 'LCP' end 仓库名称
            ,sl.name seller_name
            ,status shelf_status
            ,arrival_time + interval 7 hour arrival_time
            ,left(arrival_time ,10) arrival_date
            ,receive_time + interval 7 hour complete_time
            ,complete_time + interval 7 hour shelf_end_time
        FROM erp_wms_prod.rollback_order ro
        left join erp_wms_prod.warehouse w on ro.warehouse_id = w.id
        left join erp_wms_prod.seller sl on ro.`seller_id`=sl.`id`
        where arrival_time IS NOT NULL 
        AND status >= '1045' 
        AND STATUS<>'9000'
        and w.name='BPL3-LIVESTREAM'
        AND arrival_time >= convert_tz(date_sub(date(now() + interval -1 hour),interval 120 day), '+07:00', '+00:00')
        and arrival_time >= '2023-12-01'

    ) an

    left join 
    -- 日历调整// created_mod是节假日顺延后首日,date1是节假日顺延后第二天
    dwm.dim_th_default_timeV2 calendar on calendar.created=an.reg_date
    left join wms_production.warehouse w ON an.warehouse_id=w.id
) t0 
where 仓库名称 in ('AGV', 'BPL-Return', 'BPL3', 'BST', 'LAS')