
/*=====================================================================+
表名称：  dwd_th_ffm_sellergoodslocation
功能描述：  泰国ffm库存库位数据

需求来源：
编写人员: wangdongchen
设计日期：2024/10/22
修改日期: 
修改人员:    	
修改原因: 

-----------------------------------------------------------------------
---存在问题：
-----------------------------------------------------------------------
+===================================================================== */ 
-- drop table if exists dwm.dwd_th_ffm_sellergoodslocation;
-- create table dwm.dwd_th_ffm_sellergoodslocation as
delete from dwm.dwd_th_ffm_sellergoodslocation where statc_date >= date_sub(date(now() + interval -1 hour),interval 14 day); -- 先删除数据
insert into dwm.dwd_th_ffm_sellergoodslocation -- 再插入数据
select
	statc_date
    ,仓库名称 warehouse_name
    ,TYPE
    ,货架类型 ref_type
    ,库区 locationarea
    ,seller_goods_id
    ,seller_id
    ,location_code
    ,LENGTH
    ,width
    ,height
    ,7D活跃货主 7Dactivesellername
    ,14D活跃货主 14Dactivesellername
    ,inventory
    ,quality_status
    ,use_attribute
    ,location_id
    ,volume
    ,volumeAll
from
(
    SELECT 
	    sglr.date as statc_date
        ,case when w.name in ('AutoWarehouse', 'AutoWarehouse-人工仓') then 'AGV'
            when w.name='BPL-Return Warehouse' then 'BPL-Return'
            when w.name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
            when w.name='BangsaoThong' then 'BST'
            when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS'
            when w.name='LCP Warehouse' then 'LCP' end 仓库名称
        ,CASE WHEN greatest(SLG.LENGTH,SLG.width,SLG.height)<=250 AND SLG.weight <= 3000 AND SLG.weight>0 THEN '小件'
            WHEN greatest(SLG.LENGTH,SLG.width,SLG.height)<=500 AND SLG.weight <= 5000 AND SLG.weight>0 THEN '中件'
            WHEN greatest(SLG.LENGTH,SLG.width,SLG.height)<=1000 AND SLG.weight <= 15000 AND SLG.weight>0 THEN '大件'
            WHEN (greatest(SLG.LENGTH,SLG.width,SLG.height)>1000 AND SLG.weight>0) OR (SLG.weight > 15000 and SLG.LENGTH>0 and SLG.width>0 and SLG.height>0 ) THEN '超大件' 
            ELSE '信息不全' END TYPE
        ,CASE WHEN left(location_code,1) IN ('B','E') THEN '轻型货架'
            WHEN left(location_code,1) IN ('C','D','J') THEN '地堆'
            WHEN left(location_code,1) ='A' AND length(location_code)=5 THEN '地堆'
            WHEN left(location_code,1) ='A' THEN '高位货架'
            ELSE '地堆' END AS 货架类型
        , CASE WHEN sglr.location_type = 'pick' THEN '拣选区'
                WHEN sglr.location_type = 'stock' THEN '存储区'
                ELSE '中转区' END 库区
        ,sglr.seller_goods_id
        ,sglr.seller_id
        ,location_code
        ,SLG.LENGTH
        ,SLG.width
        ,SLG.height
        ,CASE WHEN 7天销量>0 THEN '7D活跃货主' END 7D活跃货主
        ,CASE WHEN 14天销量>0 THEN '14D活跃货主' END 14D活跃货主
        ,total_inventory as inventory
        ,quality_status
        ,use_attribute
        ,location_id
        ,slg.volume/1000000000 volume
        ,total_inventory*slg.volume/1000000000 volumeAll
    FROM wms_production.seller_goods_location_ref_snapshot sglr 
    LEFT JOIN wms_production.warehouse w ON sglr.warehouse_id=w.id
    LEFT JOIN wms_production.repository rp on sglr.repository_id=rp.id
    LEFT JOIN wms_production.location lc ON sglr.location_id=lc.id
    LEFT JOIN  wms_production.seller sl ON sglr.seller_id=sl.id
    LEFT JOIN  wms_production.seller_goods slg ON sglr.seller_goods_id=slg.id
    LEFT JOIN
    (
        SELECT 
            seller_id
            ,left(date_add(`audit_time`, interval -60 minute), 10) audit_date
            ,sum(goods_num) 14天销量
            ,sum(case when date_diff(now(),audit_time)<=7 then goods_num else 0 end ) 7天销量
        FROM wms_production.delivery_order 
        where 1=1
            and audit_time >= convert_tz(date_sub(date(now() + interval -1 hour),interval 28 day), '+07:00', '+08:00')
            and audit_time >= convert_tz('2023-12-01', '+07:00', '+08:00')
        group BY 1,2
    ) T1 ON sglr.seller_id=T1.SELLER_ID and sglr.date = audit_date
	where 1=1
	and sglr.date >= date_sub(current_date, interval 14 day)
    and sglr.date >= '2023-12-01'
	AND total_inventory>0 -- 库存大于0
	AND sl.disabled='0' -- 启用状态的货主
	and sl.name not in ('FFM-TH') -- 剔除物料

    union
	select
        iwc.record_date as statc_date
        ,case when w.name in ('AutoWarehouse', 'AutoWarehouse-人工仓') then 'AGV'
            when w.name='BPL-Return Warehouse' then 'BPL-Return'
            when w.name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
            when w.name='BangsaoThong' then 'BST'
            when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS'
            when w.name='LCP Warehouse' then 'LCP' end 仓库名称
        ,CASE WHEN greatest(SLG.LENGTH,SLG.width,SLG.height)<=250 AND SLG.weight <= 3000 THEN '小件'
            WHEN greatest(SLG.LENGTH,SLG.width,SLG.height)<=500 AND SLG.weight <= 5000 THEN '中件'
            WHEN greatest(SLG.LENGTH,SLG.width,SLG.height)<=1000 AND SLG.weight <= 15000 THEN '大件'
            WHEN greatest(SLG.LENGTH,SLG.width,SLG.height)>1000 OR SLG.weight > 15000 THEN '超大件' 
            ELSE '信息不全' END TYPE
        ,CASE WHEN left(l.location_code,1) IN ('B','E') THEN '轻型货架'
            WHEN left(l.location_code,1) IN ('C','D','J') THEN '地堆'
            WHEN left(l.location_code,1) ='A' AND length(l.location_code)=5 THEN '地堆'
            WHEN left(l.location_code,1) ='A' THEN '高位货架'
            ELSE '地堆' END AS 货架类型
        , CASE WHEN rp.use_attribute = '1' THEN '拣选区'
                WHEN rp.use_attribute = '2' THEN '存储区'
                ELSE '中转区' END 库区
        ,iwc.seller_goods_id
        ,iwc.seller_id
        ,l.location_code
        ,SLG.LENGTH
        ,SLG.width
        ,SLG.height
        ,CASE WHEN 7天销量>0 THEN '7D活跃货主' END 7D活跃货主
        ,CASE WHEN 14天销量>0 THEN '14D活跃货主' END 14D活跃货主
        ,total_number
        ,if(iwc.quality_status='1', 'normal', 'bad') quality_status
        ,if(rp.use_attribute='1', 'pick', 'stock') use_attribute
        ,location_id
        ,slg.volume/1000000000 volume
        ,total_number*slg.volume/1000000000 volumeAll
    from
        erp_wms_prod.in_warehouse_cost_snapshot  iwc
    LEFT JOIN erp_wms_prod.seller sl on iwc.seller_id = sl.id
    LEFT JOIN erp_wms_prod.warehouse w ON iwc.warehouse_id = w.id
    LEFT JOIN erp_wms_prod.repository rp on iwc.repository_id=rp.id      
    left join erp_wms_prod.location l on iwc.location_id = l.id
    LEFT JOIN erp_wms_prod.seller_goods slg ON iwc.seller_goods_id=slg.id
    LEFT JOIN
        (
            SELECT 
                seller_id
				,left(date_add(`created`, interval -60 minute), 10) audit_date
                ,sum(goods_num) 14天销量
                ,sum(case when date_diff(now(),date_add(do.`created`, interval -60 minute))<=7 then goods_num else 0 end ) 7天销量
            FROM erp_wms_prod.delivery_order do
            where 1=1 
            	and do.`created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 28 day), '+07:00', '+08:00')
                and do.`created` >= convert_tz('2023-12-01', '+07:00', '+08:00')
            group BY 1,2
        ) T1 ON iwc.seller_id=T1.SELLER_ID and iwc.record_date = audit_date
    where 1=1
	    and iwc.record_date >= date_sub(current_date, interval 14 day)
        and iwc.record_date >= '2023-12-01'
        and sl.disabled='0' -- 启用状态的货主
        and iwc.total_number>0 -- 库存大于0
        and sl.name not in ('FFM-TH') -- 剔除物料
) t0 where 仓库名称 is not null

