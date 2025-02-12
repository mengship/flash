/*=====================================================================+
表名称：  dim_th_ffm_warehouseseller_day
功能描述：  泰国ffm 仓库+货主+日期 维表

需求来源：
编写人员: wangdongchen
设计日期：2024/10/29
修改日期: 
修改人员:    	
修改原因: 

-----------------------------------------------------------------------
---存在问题：
-----------------------------------------------------------------------
+===================================================================== */ 
drop table if exists dwm.dim_th_ffm_warehouseseller_day;
create table dwm.dim_th_ffm_warehouseseller_day as

select
    warehouse_name.warehouse_name
    , t0.dt
    , t1.seller_id
    , t1.seller_name
from
(
    select
        'AGV' warehouse_name

    union
    select
        'BST' warehouse_name

    union
    select
        'BPL3' warehouse_name

    union
    select
        'BPL-Return' warehouse_name

    union
    select
        'LAS' warehouse_name
) warehouse_name
join
(
    select
        date dt
    from
    tmpale.ods_th_dim_date
    where date >= '2024-01-01'
      and date <= current_date+ interval 1 day
) t0
on 1=1
join 
(
    select 
        swf.warehouse_id
        ,swf.seller_id
        ,s.name seller_name
        ,s.created
        ,date(s.created) created_dt
        ,s.modified 
        ,case when w.name in ('AutoWarehouse', 'AutoWarehouse-人工仓')   then 'AGV'
            when w.name='BPL-Return Warehouse'  then 'BPL-Return'
            when w.name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
            when w.name='BangsaoThong'  then 'BST'
            when w.name IN ('BKK-WH-LAS2电商仓')    then 'LAS'
            when w.name ='LCP Warehouse' then 'LCP' end warehouse_name
        ,s.disabled # 0 启用 1 禁用
        ,case when s.disabled = 0 then (current_date + interval 1 day)
            when s.disabled = 1 then date(s.modified)
            end as end_dt
    from
        wms_production.seller_warehouse_ref swf 
    left join wms_production.seller s on swf.seller_id = s.id
    left join wms_production.warehouse w on swf.warehouse_id = w.id
) t1 on warehouse_name.warehouse_name = t1.warehouse_name and t0.dt <= t1.end_dt and t0.dt >= created_dt