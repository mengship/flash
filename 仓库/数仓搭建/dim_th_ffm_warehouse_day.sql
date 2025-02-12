/*=====================================================================+
表名称：  dim_th_ffm_warehouse_day
功能描述：  泰国ffm 仓库+日期 维表

需求来源：
编写人员: wangdongchen
设计日期：2024/10/25
修改日期: 
修改人员:    	
修改原因: 

-----------------------------------------------------------------------
---存在问题：
-----------------------------------------------------------------------
+===================================================================== */ 
drop table if exists dwm.dim_th_ffm_warehouse_day;
create table dwm.dim_th_ffm_warehouse_day as
select
    warehouse_name
    , t0.dt
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
    where date>='2023-10-01'
        and date<= current_date+ interval 1 day
) t0
on 1=1
