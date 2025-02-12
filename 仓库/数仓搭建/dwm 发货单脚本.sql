dwm 发货单脚本

-- 发货单创建日期的审核情况
    SELECT 
        LEFT(created_time,10) dt
        ,warehouse_name
        ,TYPE
        ,if(audit_time IS  NULL, 'audit', 'noaudit') is_audit
        ,'创建日期的审核单量' title
        ,COUNT(delivery_sn) auditnum
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where  1=1
    and LEFT(created_time,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
  	and is_visible=1
    GROUP BY 1,2,3,4,5


-- 2B（打包日期）2C（出库日期）出库情况
    SELECT 
        LEFT(if(type='B2C', delivery_time, pack_time),10) dt
        ,warehouse_name
        ,type
        ,'出库日期的出库单量' title
        ,COUNT(delivery_sn) outnumorder
        ,sum(goods_num) outnumgoods
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
    and LEFT(created_time,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
    and audit_time is not null
  	and is_visible=1
    GROUP BY 1,2,3,4

-- 交接单量
    SELECT 
        LEFT(handover_time,10) dt
        ,warehouse_name
        ,type
        ,'交接日期的交接单量' title
        ,COUNT(delivery_sn) handovernumorder
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
    and LEFT(handover_time,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
  	and is_visible=1
    GROUP BY 1,2,3,4

-- 最晚出库日期汇总
    SELECT 
        LEFT(deadline,10) dt
        ,platform_source paltform
        ,warehouse_name
        ,type
        ,'最晚出库日期汇总时效' title
        ,sum(及时发货) timelyout
        ,sum(应发货) shouldout
        ,count(if(应发货=1 and 及时发货=0, delivery_sn, null)) notimelyout
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(deadline,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        -- and platform_source='Shopee'
  		and is_visible=1
  		and type='B2C'
    GROUP BY 1,2,3,4,5

-- 最晚打包日期汇总
    SELECT 
        LEFT(pack_deadline,10) dt
        ,platform_source paltform
        ,warehouse_name
        ,type
        ,'最晚打包日期汇总时效' title
        ,sum(及时打包) timelypack
        ,sum(应打包) shouldpack
        ,count(if(应打包=1 and 及时打包=0, delivery_sn, null)) notimelypack
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(pack_deadline,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        -- and platform_source='Shopee'
  		and is_visible=1
    GROUP BY 1,2,3,4,5

############################################################################################################

flag=4
shellname=dwm_th_ffm_ordertimelyout_day
type=th
erremail=wangdongchen@flashexpress.com
sh /home/deploy/script/shell/base.sh ${flag} ${shellname} ${type} ${erremail}


/*=====================================================================+
表名称：  dwm_th_ffm_orderaudit_day
功能描述：泰国发货单（2B 2C)审核情况，天粒度汇总
                        
需求来源：
编写人员: 王昱棋
设计日期：2024/10/22
        修改日期: 
        修改人员:            
        修改原因: 
-----------------------------------------------------------------------
---存在问题：
-----------------------------------------------------------------------
+=====================================================================*/ 
      
-- drop table if exists dwm.dwm_th_ffm_orderaudit_day;
-- create table dwm.dwm_th_ffm_orderaudit_day as
delete from dwm.dwm_th_ffm_orderaudit_day where dt >= date_sub(date(now() + interval -1 hour),interval 60 day); -- 先删除数据
insert into dwm.dwm_th_ffm_orderaudit_day -- 再插入数据
    SELECT 
        LEFT(created_time,10) dt
        ,warehouse_name
        ,TYPE
        ,if(audit_time IS  NULL, 'audit', 'noaudit') is_audit
        ,'创建日期的审核单量' title
        ,COUNT(delivery_sn) auditnum
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where  1=1
    and created_date >= date_sub(date(now() + interval -1 hour),interval 60 day)
  	and is_visible=1
    GROUP BY 1,2,3,4,5


/*=====================================================================+
表名称：  dwm_th_ffm_orderout_day
功能描述：泰国发货单 2B（打包日期）2C（出库日期）出库情况
                        
需求来源：
编写人员: 王昱棋
设计日期：2024/10/22
        修改日期: 
        修改人员:            
        修改原因: 
-----------------------------------------------------------------------
---存在问题：
-----------------------------------------------------------------------
+=====================================================================*/ 
      
-- drop table if exists dwm.dwm_th_ffm_orderout_day;
-- create table dwm.dwm_th_ffm_orderout_day as
delete from dwm.dwm_th_ffm_orderout_day where dt >= date_sub(date(now() + interval -1 hour),interval 60 day); -- 先删除数据
insert into dwm.dwm_th_ffm_orderout_day -- 再插入数据
    SELECT 
        LEFT(if(type='B2C', delivery_time, pack_time),10) dt
        ,warehouse_name
        ,type
        ,'出库日期的出库单量' title
        ,COUNT(delivery_sn) outnumorder
        ,sum(goods_num) outnumgoods
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
    -- and LEFT(created_time,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
    and if(type='B2C', delivery_time, pack_time) >= date_sub(date(now() + interval -1 hour),interval 60 day)
    and audit_time is not null
  	and is_visible=1
    GROUP BY 1,2,3,4


/*=====================================================================+
表名称：  dwm_th_ffm_orderhandover_day
功能描述：泰国发货单 交接单量 情况
                        
需求来源：
编写人员: 王昱棋
设计日期：2024/10/22
        修改日期: 
        修改人员:            
        修改原因: 
-----------------------------------------------------------------------
---存在问题：
-----------------------------------------------------------------------
+=====================================================================*/ 
      
drop table if exists dwm.dwm_th_ffm_orderhandover_day;
create table dwm.dwm_th_ffm_orderhandover_day as
-- delete from dwm.dwm_th_ffm_orderhandover_day where dt >= date_sub(date(now() + interval -1 hour),interval 60 day); -- 先删除数据
-- insert into dwm.dwm_th_ffm_orderhandover_day -- 再插入数据
    SELECT 
        LEFT(handover_time,10) dt
        ,warehouse_name
        ,type
        ,'交接日期的交接单量' title
        ,COUNT(delivery_sn) handovernumorder
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
    -- and handover_time >= date_sub(date(now() + interval -1 hour),interval 60 day)
  	and is_visible=1
    GROUP BY 1,2,3,4

/*=====================================================================+
表名称：  dwm_th_ffm_ordertimelyout_day
功能描述：泰国发货单 出库时效 情况
                        
需求来源：
编写人员: 王昱棋
设计日期：2024/10/22
        修改日期: 
        修改人员:            
        修改原因: 
-----------------------------------------------------------------------
---存在问题：
-----------------------------------------------------------------------
+=====================================================================*/ 
      
drop table if exists dwm.dwm_th_ffm_ordertimelyout_day;
create table dwm.dwm_th_ffm_ordertimelyout_day as
-- delete from dwm.dwm_th_ffm_ordertimelyout_day where dt >= date_sub(date(now() + interval -1 hour),interval 60 day); -- 先删除数据
-- insert into dwm.dwm_th_ffm_ordertimelyout_day -- 再插入数据
-- 最晚出库日期汇总
    SELECT 
        LEFT(deadline,10) dt
        ,platform_source paltform
        ,warehouse_name
        ,type
        ,'最晚出库日期汇总时效' title
        ,sum(及时发货) timelyout
        ,sum(应发货) shouldout
        ,count(if(应发货=1 and 及时发货=0, delivery_sn, null)) notimelyout
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        -- and deadline >= date_sub(date(now() + interval -1 hour),interval 60 day)
        and is_time=1
  		and is_visible=1
  		and type='B2C'
    GROUP BY 1,2,3,4,5

union all
-- 最晚打包日期汇总
    SELECT 
        LEFT(pack_deadline,10) dt
        ,platform_source paltform
        ,warehouse_name
        ,type
        ,'最晚打包日期汇总时效' title
        ,sum(及时打包) timelypack
        ,sum(应打包) shouldpack
        ,count(if(应打包=1 and 及时打包=0, delivery_sn, null)) notimelypack
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and pack_deadline >= date_sub(date(now() + interval -1 hour),interval 60 day)
        and is_time=1
  		and is_visible=1
    GROUP BY 1,2,3,4,5