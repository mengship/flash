/*=====================================================================+
表名称：  dwd_th_ffm_invcount
功能描述： 盘点明细表
                        
需求来源：
编写人员: 王昱棋
设计日期：2024/10/29
        修改日期: 
        修改人员:            
        修改原因: 
-----------------------------------------------------------------------
---存在问题：
-----------------------------------------------------------------------
+=====================================================================*/ 

-- drop table if exists dwm.dwd_th_ffm_invcount;
-- create table dwm.dwd_th_ffm_invcount as
delete from dwm.dwd_th_ffm_invcount where result_confirm_date >= date_sub(date(now() + interval -1 hour),interval 90 day); -- 先删除数据
insert into dwm.dwd_th_ffm_invcount -- 再插入数据
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
    ,date(ic.result_confirm_time) result_confirm_date
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
    ,s.name seller_name
from wms_production.inv_count ic
left join wms_production.inv_count_goods icg on ic.id = icg.inv_count_id
left join wms_production.seller_goods sg on icg.seller_goods_id = sg.id
left join wms_production.warehouse w on ic.warehouse_id = w.id
left join wms_production.seller s on ic.seller_id = s.id
left join `wms_production`.`member` m on ic.creator_id = m.id
where 1=1
    -- and ic.inv_count_sn='PD2409107684'
    and s.name <> 'FFM-TH'
    and ic.`result_confirm_time` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 90 day), '+07:00', '+08:00')
    and m.job_number not in ('686204', '629986', '616389', '617783', '617782', '618997', '679056', '707241', '708967') -- 去掉郑骐老师盘点的人
;

