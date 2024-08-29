WITH KS AS
(
    select  
        wo.id id 
        ,case when w.name='AutoWarehouse' then 'AGV'
            when w.name='BPL-Return Warehouse' then 'BPL-Return'
            when w.name='BPL3-LIVESTREAM' then 'BPL3'
            when w.name='BangsaoThong' then 'BST'
            when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS'
            when w.name='LCP Warehouse' then 'LCP' end 仓库名称
        ,LEFT(wo.created - interval 1 hour,10) 创建日期
        ,week(wo.created + interval 23 hour) 周
        ,case when RESULT =2 then '有责投诉' else '无责投诉' end 仓责判断
        ,complete_time
        ,wo.created
        ,IF(complete_time <= wo.created + INTERVAL 23 HOUR,1,0) 是否及时
    from  wms_production.work_order wo 
    left join wms_production.warehouse w on w.id=WO.warehouse_id
    LEFT JOIN wms_production.seller sl on wo.seller_id =sl.id 
    left join wms_production.judgement jm on wo.id = jm.work_order_id AND left(wo.created,10) >=left(now()- INTERVAL 70 day,10))

SELECT 
    创建日期
    ,仓库名称
    ,'有责投诉' type
    ,sum(CASE WHEN 仓责判断='有责投诉' THEN 1 ELSE 0 end)客诉量 
FROM KS 
WHERE 仓库名称 IS NOT null
GROUP BY 1,2,3 

UNION
SELECT 
    创建日期
    ,仓库名称
    ,'无责投诉' type
    ,sum(CASE WHEN 仓责判断='无责投诉' THEN 1 ELSE 0 END )客诉量 
FROM KS 
WHERE 仓库名称 IS NOT null
GROUP BY 1,2,3

UNION
SELECT 
    创建日期
    ,仓库名称
    ,'及时关闭工单' type
    ,sum(是否及时)客诉量 
FROM KS 
WHERE 仓库名称 IS NOT NULL
GROUP BY 1,2,3
