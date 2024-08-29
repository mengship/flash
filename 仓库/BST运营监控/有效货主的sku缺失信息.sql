WITH A AS
(
    SELECT 
        case when w.name='AutoWarehouse' then 'AGV'
            when w.name='BPL-Return Warehouse' then 'BPL-Return'
            when w.name='BPL3-LIVESTREAM' then 'BPL3'
            when w.name='BangsaoThong' then 'BST'
            when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS'
            when w.name='LCP Warehouse' then 'LCP' end 仓库名称
        ,CASE WHEN greatest(SLG.LENGTH,SLG.width,SLG.height)<=250 AND weight <= 3000 THEN '小件'
            WHEN greatest(SLG.LENGTH,SLG.width,SLG.height)<=500 AND weight <= 5000 THEN '中件'
            WHEN greatest(SLG.LENGTH,SLG.width,SLG.height)<=1000 AND weight <= 15000 THEN '大件'
            WHEN greatest(SLG.LENGTH,SLG.width,SLG.height)>1000 OR weight > 15000 THEN '超大件' 
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
        ,slg.bar_code
        ,slg.name goods_name
        ,sglr.seller_id
        ,location_code
        ,SLG.LENGTH
        ,SLG.width
        ,SLG.height
        ,CASE WHEN 7天销量>0 THEN '7D活跃货主' END 7D活跃货主
        ,CASE WHEN 14天销量>0 THEN '14D活跃货主' END 14D活跃货主
        ,inventory
        ,quality_status
        ,use_attribute
        ,location_id
        ,slg.volume/1000000000 volume
        ,inventory*slg.volume/1000000000 volumeAll
    FROM wms_production.seller_goods_location_ref sglr 
    LEFT JOIN wms_production.warehouse w ON sglr.warehouse_id=w.id
    LEFT JOIN wms_production.repository rp on sglr.repository_id=rp.id
    LEFT JOIN wms_production.location lc ON sglr.location_id=lc.id
    LEFT JOIN  wms_production.seller sl ON sglr.seller_id=sl.id
    LEFT JOIN  wms_production.seller_goods slg ON sglr.seller_goods_id=slg.id
    LEFT JOIN
        (
            SELECT 
                seller_id
                ,sum(goods_num) 14天销量
                ,sum(case when date_diff(now(),audit_time)<=7 then goods_num else 0 end ) 7天销量
            FROM wms_production.delivery_order 
            where 1=1
                and audit_time >= convert_tz(date_sub(date(now() + interval -1 hour),interval 14 day), '+07:00', '+08:00')
            group BY 1
        ) T1 ON sglr.seller_id=T1.SELLER_ID
        where 1=1
        AND inventory>0 -- 库存大于0
        AND sl.disabled='0' -- 启用状态的货主
    
    union
    select
        case when w.name='AutoWarehouse' then 'AGV'
            when w.name='BPL-Return Warehouse' then 'BPL-Return'
            when w.name='BPL3-LIVESTREAM' then 'BPL3'
            when w.name='BangsaoThong' then 'BST'
            when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS'
            when w.name='LCP Warehouse' then 'LCP' end 仓库名称
        ,CASE WHEN greatest(SLG.LENGTH,SLG.width,SLG.height)<=250 AND weight <= 3000 THEN '小件'
            WHEN greatest(SLG.LENGTH,SLG.width,SLG.height)<=500 AND weight <= 5000 THEN '中件'
            WHEN greatest(SLG.LENGTH,SLG.width,SLG.height)<=1000 AND weight <= 15000 THEN '大件'
            WHEN greatest(SLG.LENGTH,SLG.width,SLG.height)>1000 OR weight > 15000 THEN '超大件' 
            ELSE '信息不全' END TYPE
        ,CASE WHEN left(l.location_code,1) IN ('B','E') THEN '轻型货架'
            WHEN left(l.location_code,1) IN ('C','D','J') THEN '地堆'
            WHEN left(l.location_code,1) ='A' AND length(l.location_code)=5 THEN '地堆'
            WHEN left(l.location_code,1) ='A' THEN '高位货架'
            ELSE '地堆' END AS 货架类型
        , CASE WHEN iwc.use_attribute = '1' THEN '拣选区'
                WHEN iwc.use_attribute = '2' THEN '存储区'
                ELSE '中转区' END 库区
        ,iwc.seller_goods_id
        ,slg.bar_code
        ,slg.name goods_name
        ,iwc.seller_id
        ,l.location_code
        ,SLG.LENGTH
        ,SLG.width
        ,SLG.height
        ,CASE WHEN 7天销量>0 THEN '7D活跃货主' END 7D活跃货主
        ,CASE WHEN 14天销量>0 THEN '14D活跃货主' END 14D活跃货主
        ,total_number
        ,if(iwc.quality_status='1', 'normal', 'bad') quality_status
        ,if(use_attribute='1', 'pick', 'stock') use_attribute
        ,location_id
        ,slg.volume/1000000000 volume
        ,total_number*slg.volume/1000000000 volumeAll
    from
        erp_wms_prod.in_warehouse_cost iwc
    LEFT JOIN erp_wms_prod.seller sl on iwc.seller_id = sl.id
    LEFT JOIN erp_wms_prod.warehouse w ON iwc.warehouse_id = w.id
    left join erp_wms_prod.location l on iwc.location_id = l.id
    LEFT JOIN  erp_wms_prod.seller_goods slg ON iwc.seller_goods_id=slg.id
    LEFT JOIN
        (
            SELECT 
                seller_id
                ,sum(goods_num) 14天销量
                ,sum(case when date_diff(now(),date_add(do.`created`, interval -60 minute))<=7 then goods_num else 0 end ) 7天销量
            FROM erp_wms_prod.delivery_order do
            where do.`created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 14 day), '+07:00', '+08:00')
            group BY 1
        ) T1 ON iwc.seller_id=T1.SELLER_ID
    where 1=1
        and sl.disabled='0' -- 启用状态的货主
        and iwc.total_number>0 -- 库存大于0
)
select 
    仓库名称
    ,seller_goods_id
    ,bar_code
    ,goods_name
    ,LENGTH
    ,width
    ,height

from 
a
where 1=1
  and (LENGTH is null or width is null or height is null or volume is null)
  and 仓库名称 is not null
group by
    仓库名称
    ,seller_goods_id
    ,bar_code
    ,goods_name
    ,LENGTH
    ,width
    ,height
;


select * from erp_wms_prod.seller_goods where length(LENGTH)=0 or length(width)=0 or length(height)=0 


select * from wms_production.seller_goods where (LENGTH is null or width is null or height is null)