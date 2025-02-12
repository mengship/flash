with inv as 
(
  select
      ''
    ,ic.id
    ,ic.inv_count_sn
    ,ic.first_complete_id
    ,ic.first_complete_time
    ,ic.second_complete_id
    ,ic.second_complete_time
    ,ic.third_complete_id
    ,ic.third_complete_time
    ,ic.result_confirm_id
    ,ic.result_confirm_time
    ,icg.id icg_id
    ,icg.seller_goods_id
    ,sg.name goods_name
    ,icg.inventory_num
    ,icg.end_num
    ,icg.end_diff_num
    ,w.name warehouvse_name
    ,case when w.name in ('AutoWarehouse', 'AutoWarehouse-人工仓')   then 'AGV'
          when w.name='BPL-Return Warehouse'  then 'BPL-Return'
          when w.name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
          when w.name='BangsaoThong'  then 'BST'
          when w.name IN ('BKK-WH-LAS2电商仓')    then 'LAS'
          when w.name ='LCP Warehouse' then 'LCP' end warehouse_short_name
    from wms_production.inv_count ic
    left join wms_production.inv_count_goods icg on ic.id = icg.inv_count_id
    left join wms_production.seller_goods sg on icg.seller_goods_id = sg.id
    left join wms_production.warehouse w on ic.warehouse_id = w.id
    where 1=1
      -- and ic.inv_count_sn='PD2409107684'
      and ic.`result_confirm_time` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 14 day), '+07:00', '+08:00')
)
select
    date(result_confirm_time) result_confirm_date
    ,warehouse_short_name
    ,'计划行数'
    ,count(1) num
from inv 
group by 1,2,3

union all
select
    date(result_confirm_time) result_confirm_date
    ,warehouse_short_name
    ,'计划库存'
    ,sum(inventory_num) inventory_num
from inv 
group by 1,2,3

union all
select
    date(result_confirm_time) result_confirm_date
    ,warehouse_short_name
    ,'盘点库存'
    ,sum(end_num) end_num
from inv 
group by 1,2,3

union all
select
    date(result_confirm_time) result_confirm_date
    ,warehouse_short_name
    ,'准确行数'
    ,sum(if(end_diff_num=0, 1, 0)) end_num
from inv 
group by 1,2,3

union all
select
    date(result_confirm_time) result_confirm_date
    ,warehouse_short_name
    ,'准确库存'
    ,sum(if(end_diff_num=0, end_num, 0)) end_num
from inv 
group by 1,2,3

union all
select
    date(result_confirm_time) result_confirm_date
    ,warehouse_short_name
    ,'盘盈行数'
    ,sum(if(end_diff_num>0, 1, 0)) end_num
from inv 
group by 1,2,3

union all
select
    date(result_confirm_time) result_confirm_date
    ,warehouse_short_name
    ,'盘盈库存'
    ,ABS(sum(if(end_diff_num>0, end_diff_num, 0))) end_num
from inv 
group by 1,2,3

union all
select
    date(result_confirm_time) result_confirm_date
    ,warehouse_short_name
    ,'盘亏行数'
    ,sum(if(end_diff_num<0, 1, 0)) end_num
from inv 
group by 1,2,3

union all
select
    date(result_confirm_time) result_confirm_date
    ,warehouse_short_name
    ,'盘亏库存'
    ,ABS(sum(if(end_diff_num<0, end_diff_num, 0))) end_num
from inv 
group by 1,2,3
