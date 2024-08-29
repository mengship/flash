SELECT T1.created 时间,
case when w.name='AutoWarehouse' then 'AGV'
        when w.name='BPL-Return Warehouse' then 'BPL-Return'
        when w.name='BPL3-LIVESTREAM' then 'BPL3'
        when w.name='BangsaoThong' then 'BST'
        when w.name IN ('BKK-WH-LAS2电商仓','PMD-WH','BKK-WH-Ecommerce','BKK-WH-LAS物料仓')         then 'LAS'
        when w.name='LCP Warehouse' then 'LCP' end 仓库
,单据,指标,sum(num)数值
from
(select 
LEFT(created - INTERVAL 1 HOUR,10) created
,warehouse_id
,'拦截单' 单据
,'生成拦截单量' 指标 
,count(id)num 
from wms_production.intercept_place 
where status <>'1000'
and LEFT(created - INTERVAL 1 HOUR,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now() - INTERVAL 1 day,10)
GROUP BY 1,2,3,4

UNION
select 
LEFT(shelf_on_end_time,10) completed
,warehouse_id
,'拦截单' 单据
,'完成拦截单量' TYPE 
,count(id)num -- 及时上架
from wms_production.intercept_place 
where status <>'1000'
and LEFT(shelf_on_end_time,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now() - INTERVAL 1 day,10)
GROUP BY 1,2,3,4

union
select 
LEFT(created + INTERVAL 23 HOUR,10) deadline
,warehouse_id
,'拦截单' 单据
,'及时上架' TYPE 
,sum(IF(shelf_on_end_time<(created + INTERVAL 23 HOUR ),1,0))num -- 及时上架
from wms_production.intercept_place 
where status <>'1000'
and LEFT(created + INTERVAL 23 HOUR,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now() - INTERVAL 1 day,10)
GROUP BY 1,2,3,4

UNION
select 
LEFT(created + INTERVAL 23 HOUR,10) deadline
,warehouse_id
,'拦截单' 单据
,'应上架' TYPE 
,sum(if((shelf_on_end_time is not null OR (created + INTERVAL 23 HOUR)< date_add(now(), interval -60 minute)),1,0)) -- 应上架
from wms_production.intercept_place 
where status <>'1000'
and LEFT(created + INTERVAL 23 HOUR,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now() - INTERVAL 1 day,10) -- deadline 近7天
GROUP BY 1,2,3,4


UNION
select 
LEFT(now() - INTERVAL 1 day,10) date
,warehouse_id
,'拦截单' 单据
,'超时未完结' TYPE 
,count(id)
from wms_production.intercept_place 
where status <>'1000'
AND shelf_on_end_time IS null
and (created + INTERVAL 23 HOUR)< now() - INTERVAL 1 hour
GROUP BY 1,2,3,4

UNION
select 
LEFT(create_time - INTERVAL 1 HOUR,10) created
,warehouse_id
,'异常单' 单据
,'生成异常单量' TYPE 
,count(id)num 
from wms_production.abnormal_order  
where 1=1
and LEFT(create_time - INTERVAL 1 HOUR,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now() - INTERVAL 1 day,10)
GROUP BY 1,2,3,4

UNION
select 
LEFT(finish_time,10) completed
,warehouse_id
,'异常单' 单据
,'完成异常单量' TYPE 
,count(id)num -- 及时上架
from wms_production.abnormal_order 
where 1=1
and LEFT(finish_time ,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now() - INTERVAL 1 day,10)
GROUP BY 1,2,3,4

union
select 
LEFT(create_time + INTERVAL 23 HOUR,10) deadline
,warehouse_id
,'异常单' 单据
,'及时完成' TYPE 
,sum(IF(finish_time<(create_time + INTERVAL 23 HOUR ),1,0))num -- 及时上架
from wms_production.abnormal_order 
where 1=1
and LEFT(create_time + INTERVAL 23 HOUR,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now() - INTERVAL 1 day,10)
GROUP BY 1,2,3,4

UNION
select 
LEFT(create_time + INTERVAL 23 HOUR,10) deadline
,warehouse_id
,'异常单' 单据
,'应完成' TYPE 
,sum(if((finish_time is not null OR (create_time + INTERVAL 23 HOUR)< date_add(now(), interval -60 minute)),1,0)) -- 应上架
from wms_production.abnormal_order 
where 1=1
and LEFT(create_time + INTERVAL 23 HOUR,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now() - INTERVAL 1 day,10) -- deadline 近7天
GROUP BY 1,2,3,4

UNION
select 
LEFT(now() - INTERVAL 1 day,10) date
,warehouse_id
,'异常单' 单据
,'超时未完结' TYPE 
,count(id)
from wms_production.abnormal_order 
where 1=1
AND finish_time IS null
and (create_time + INTERVAL 23 HOUR)< now() - INTERVAL 1 hour
GROUP BY 1,2,3,4


)T1 
LEFT JOIN wms_production.warehouse w ON T1.warehouse_id=w.id
group by 1,2,3,4


