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
               --  date_diff(now(),audit_time)<=14
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
, agv as
(
    SELECT 
        'AGV' as '仓库'
        ,bes.`code` '库位编码'
        ,bes.`width`*bes.`height`*bes.`depth` '体积'
        ,if(bi.`num`>0,1,0)  '正品库存'
    FROM `was`.`base_entity_slot` bes 
    LEFT JOIN 
    (
        select
            slot_code
            ,MAX(num) 'num'
        FROM `was`.`base_inventory` i
        where i.`group_id` = '1180'   #一个库位有多个业务实体，园区code一致
        group by slot_code
    ) bi on bes.`code`= bi.`slot_code`
    LEFT JOIN `was`.`base_entity_bucket` beb on bes.`bucket_code`=beb.`code` and bes.park_id=beb.park_id
    LEFT JOIN `was`.`base_entity_opr_area` beoa on beb.`opr_area_code`=beoa.`code`  and beb.`park_id` = beoa.`park_id`  #+园区条件
    LEFT JOIN `was`.`base_entity_opr_area_group` beoag on beoa.`park_id`=beoag.`park_id`
    WHERE bes.`del_flag`= 0 
        AND bes.`enable_flag`= 1     #库位启用状态为启用
        AND beoa.`status`='SELLABLE'   #作业区为可售状态
        AND beoag.`name`='AGV WH GROUP 01'  #作业区组为AGV
        AND bes.park_id = 16          #园区为AGV
        AND (bes.`code` like 'H%' or bes.`code` like 'FA-B%')
)

SELECT 
    仓库名称
    ,'货主数'指标
    ,count(DISTINCT seller_id)数值  
FROM  A WHERE 仓库名称 IS NOT NULL AND quality_status ='normal'
GROUP BY 1,2

UNION 
SELECT 
    仓库名称
    ,'7D活跃货主'指标
    ,count(DISTINCT seller_id) 
FROM  A
WHERE 仓库名称 IS NOT null 
    AND quality_status ='normal'
    AND 7D活跃货主='7D活跃货主' 
GROUP BY 1,2

UNION 
SELECT 
    仓库名称
    ,'14D活跃货主'指标
    ,count(DISTINCT seller_id) FROM  A
WHERE 仓库名称 IS NOT NULL
    AND quality_status ='normal'
    and 14D活跃货主='14D活跃货主'
GROUP BY 1,2

UNION
SELECT 
    仓库名称
    ,'SKU数'指标
    ,count(DISTINCT seller_goods_id) 
FROM  A
WHERE 仓库名称 IS NOT NULL
    AND quality_status ='normal'
GROUP BY 1,2

UNION
SELECT 
    仓库名称
    ,'小件'指标
    ,count(DISTINCT CASE WHEN TYPE ='小件' THEN seller_goods_id end) 
FROM  A
WHERE 仓库名称 IS NOT NULL
    AND quality_status ='normal'
GROUP BY 1,2

UNION
SELECT 
    仓库名称
    ,'中件'指标
    ,count(DISTINCT CASE WHEN TYPE ='中件' THEN seller_goods_id end) 
FROM  A
WHERE 仓库名称 IS NOT NULL
    AND quality_status ='normal'
GROUP BY 1,2

UNION
SELECT 
    仓库名称
    ,'大件'指标
    ,count(DISTINCT CASE WHEN TYPE ='大件' THEN seller_goods_id end) 
FROM  A
WHERE 仓库名称 IS NOT NULL
    AND quality_status ='normal'
GROUP BY 1,2

UNION
SELECT 
    仓库名称
    ,'超大件'指标
    ,count(DISTINCT CASE WHEN TYPE ='超大件' THEN seller_goods_id end) 
FROM  A
WHERE 仓库名称 IS NOT NULL
    AND quality_status ='normal'
GROUP BY 1,2

UNION
SELECT 
    仓库名称,
    '信息不全'指标
    ,count(DISTINCT CASE WHEN TYPE ='信息不全' THEN seller_goods_id end) 
FROM  A
WHERE 仓库名称 IS NOT NULL
AND  quality_status ='normal'
GROUP BY 1,2

UNION
SELECT 
    仓库名称
    ,'正品库存'指标
    ,sum(inventory) 
FROM  A
WHERE 仓库名称 IS NOT NULL
AND quality_status ='normal'
GROUP BY 1,2

UNION
SELECT 仓库名称,'小件库存'指标,sum(CASE WHEN TYPE ='小件' THEN inventory ELSE 0 end)/sum(inventory) FROM  A
WHERE 仓库名称 IS NOT NULL
AND  quality_status ='normal'
GROUP BY 1,2

UNION
SELECT 仓库名称,'中件库存'指标,sum(CASE WHEN TYPE ='中件' THEN inventory ELSE 0  end)/sum(inventory) FROM  A
WHERE 仓库名称 IS NOT NULL
AND quality_status ='normal'
GROUP BY 1,2

UNION
SELECT 仓库名称,'大件库存'指标,sum(CASE WHEN TYPE ='大件' THEN inventory ELSE 0  end)/sum(inventory) FROM  A
WHERE 仓库名称 IS NOT NULL
AND quality_status ='normal'
GROUP BY 1,2

UNION
SELECT 仓库名称,'超大件库存'指标,sum(CASE WHEN TYPE ='超大件' THEN inventory ELSE 0  end)/sum(inventory) FROM  A
WHERE 仓库名称 IS NOT NULL
AND quality_status ='normal'
GROUP BY 1,2

UNION
SELECT 仓库名称,'信息不全库存'指标,sum(CASE WHEN TYPE ='信息不全' THEN inventory ELSE 0  end)/sum(inventory) FROM  A
WHERE 仓库名称 IS NOT NULL
AND quality_status ='normal'
GROUP BY 1,2

UNION
SELECT 仓库名称,'体积'指标,sum(volumeAll) FROM  A
WHERE 仓库名称 IS NOT NULL
AND quality_status ='normal'
GROUP BY 1,2
UNION
SELECT 仓库名称,'小件库存-体积'指标,sum(CASE WHEN TYPE ='小件' THEN volumeAll ELSE 0 end)/sum(volumeAll) FROM  A
WHERE 仓库名称 IS NOT NULL
AND  quality_status ='normal'
GROUP BY 1,2
UNION
SELECT 仓库名称,'中件库存-体积'指标,sum(CASE WHEN TYPE ='中件' THEN volumeAll ELSE 0  end)/sum(volumeAll) FROM  A
WHERE 仓库名称 IS NOT NULL
AND quality_status ='normal'
GROUP BY 1,2
UNION
SELECT 仓库名称,'大件库存-体积'指标,sum(CASE WHEN TYPE ='大件' THEN volumeAll ELSE 0  end)/sum(volumeAll) FROM  A
WHERE 仓库名称 IS NOT NULL
AND quality_status ='normal'
GROUP BY 1,2
UNION
SELECT 仓库名称,'超大件库存-体积'指标,sum(CASE WHEN TYPE ='超大件' THEN volumeAll ELSE 0  end)/sum(volumeAll) FROM  A
WHERE 仓库名称 IS NOT NULL
AND quality_status ='normal'
GROUP BY 1,2
UNION
SELECT 仓库名称,'信息不全库存-体积'指标,sum(CASE WHEN TYPE ='信息不全' THEN volumeAll ELSE 0  end)/sum(volumeAll) FROM  A
WHERE 仓库名称 IS NOT NULL
AND quality_status ='normal'
GROUP BY 1,2

UNION
SELECT 仓库名称,'残品库存'指标,sum(inventory) FROM  A
WHERE 仓库名称 IS NOT NULL
AND quality_status <>'normal'
GROUP BY 1,2


union 
select 
    仓库名称
    ,'拣选区一品一位'type
    , sum(case when num = 1 then 1 else 0 end)/count(seller_goods_id) 
from 
    (
        select 
            仓库名称
            ,seller_goods_id
            ,count(location_id)num 
        from 
            (
                SELECT 
                    仓库名称
                    ,location_id
                    ,seller_goods_id
                    ,sum(inventory) 
                FROM a
                where 仓库名称 IS NOT null
                    and quality_status='normal'
                    AND use_attribute='pick'
                group by 1,2,3
            ) 
        group by 1,2
    ) GROUP BY 1,2
UNION 
select 
    仓库名称
    ,'拣选区一位一品'type
    , sum(case when num = 1 then 1 else 0 end)/count(location_id) 
from 
(
    select 
        仓库名称
        ,location_id
        ,count(seller_goods_id)num 
    from 
        (
            SELECT 
                仓库名称
                ,location_id
                ,seller_goods_id
                ,sum(inventory) 
            FROM a
            where 仓库名称 IS NOT null
                and quality_status='normal'
                AND use_attribute='pick'
            group by 1,2,3
        ) group by 1,2
)GROUP BY 1,2

union
SELECT 
    仓库名称
    ,'拣选区SKU覆盖率' type
    ,count( distinct case when use_attribute='pick' then seller_goods_id end)/count(distinct seller_goods_id )
FROM a where 仓库名称 IS NOT null
and quality_status='normal'
GROUP BY 1,2

UNION
SELECT 
    仓库名称
    ,'总体积' type
    ,sum(LENGTH*width*height*inventory)/1e9 
FROM  A WHERE 仓库名称 in ('BST', 'LAS') 
AND quality_status ='normal'
and(LENGTH is not null and width is not null and height is not null)
GROUP BY 1,2
UNION 
SELECT 
    仓库名称
    ,'高位货架体积' type
    ,sum(LENGTH*width*height*inventory)/1e9 
FROM  A WHERE 仓库名称 in ('BST', 'LAS') 
    AND quality_status ='normal'
    and(LENGTH is not null and width is not null and height is not null)
    AND 货架类型='高位货架'
GROUP BY 1,2
UNION 
SELECT 
    仓库名称
    ,'轻型货架体积' type
    ,sum(LENGTH*width*height*inventory)/1e9 
FROM  A WHERE 仓库名称 in ('BST', 'LAS') 
    AND quality_status ='normal'
    and(LENGTH is not null and width is not null and height is not null)
    AND 货架类型='轻型货架'
GROUP BY 1,2
UNION 
SELECT 
    仓库名称
    ,'地堆体积' type
    ,sum(LENGTH*width*height*inventory)/1e9 
FROM  A WHERE 仓库名称 in ('BST', 'LAS') 
    AND quality_status ='normal'
    and(LENGTH is not null and width is not null and height is not null)
AND 货架类型='地堆'
GROUP BY 1,2

-- 库容只看BST
UNION
SELECT 
    仓库名称
    ,'总使用库位' type
    ,count(DISTINCT location_code)
FROM  A WHERE 仓库名称 in ('BST', 'LAS') 
AND quality_status ='normal'
AND left(location_code,1)IN('A','B','C','D','E','J')
GROUP BY 1,2

UNION 
SELECT 
    仓库名称
    ,'高位货架库位' type
    ,count(DISTINCT location_code)
FROM  A WHERE 仓库名称 in ('BST', 'LAS') 
    AND quality_status ='normal'
    AND 货架类型='高位货架'
    AND left(location_code,1)IN('A','B','C','D','E','J')
GROUP BY 1,2

UNION 
SELECT 
    仓库名称
    ,'轻型货架库位' type
    ,count(DISTINCT location_code)
FROM  A WHERE 仓库名称 in ('BST', 'LAS') 
    AND quality_status ='normal'
    AND 货架类型='轻型货架'
    AND left(location_code,1)IN('A','B','C','D','E','J')
GROUP BY 1,2

UNION 
SELECT 
    仓库名称
    ,'地堆库位' type
    ,count(DISTINCT location_code)
FROM  A WHERE 仓库名称 in ('BST', 'LAS') 
    AND quality_status ='normal'
    AND 货架类型='地堆'
    AND left(location_code,1)IN('A','B','C','D','E','J')
GROUP BY 1,2

-- AGV
union
SELECT 
    仓库
    ,'规划库位数' type
    ,count(DISTINCT t1.'库位编码') 规划库位数
FROM  AGV t1 
    WHERE 1=1
GROUP BY 1,2

union
SELECT 
    仓库
    ,'使用库位数' type
    ,sum(if(t1.'正品库存' > 0, 1, 0)) 使用库位数
FROM  AGV t1 
    WHERE 1=1
GROUP BY 1,2

union
SELECT 
    仓库
    ,'库位利用率' type
    ,sum(if(t1.'正品库存' > 0, 1, 0)) / count(distinct t1.'库位编码') '库位利用率' 
FROM  AGV t1 
    WHERE 1=1
GROUP BY 1,2

union
SELECT 
    仓库
    ,'规划库容' type
    ,sum(t1.'体积') / 1000000000                      '规划库容'
FROM  AGV t1 
    WHERE 1=1
GROUP BY 1,2

union
SELECT 
    仓库
    ,'使用库容' type
    ,sum(if(t1.'正品库存' > 0, t1.'体积', 0)) / 1000000000                '使用库容'
FROM  AGV t1 
    WHERE 1=1
GROUP BY 1,2

union
SELECT 
    仓库
    ,'库容利用率' type
    ,sum(if(t1.'正品库存' > 0, t1.'体积', 0))/sum(t1.'体积')                   '库容利用率' 
FROM  AGV t1 
    WHERE 1=1
GROUP BY 1,2

union
SELECT 
    仓库
    ,'高位货架库位-规划' type
    ,count(DISTINCT (if(t1.'库位编码' like 'H%', t1.'库位编码',0)))  -1             '高位货架库位-规划'
FROM  AGV t1 
    WHERE 1=1
GROUP BY 1,2

union
SELECT 
    仓库
    ,'高位货架库位' type
    ,sum(if(((t1.'库位编码' like 'H%') and t1.'正品库存' > 0 ), 1, 0))                 '高位货架库位'
FROM  AGV t1 
    WHERE 1=1
GROUP BY 1,2

union
SELECT 
    仓库
    ,'高位货架库容-规划' type
    ,sum(if(t1.'库位编码' like 'H%',t1.'体积',0)） / 1000000000                      '高位货架库容-规划'
FROM  AGV t1 
    WHERE 1=1
GROUP BY 1,2

union
SELECT 
    仓库
    ,'高位货架库容' type
    ,sum(if(((t1.'库位编码' like 'H%') and t1.'正品库存' > 0), t1.'体积', 0)) / 1000000000    '高位货架库容'
FROM  AGV t1 
    WHERE 1=1
GROUP BY 1,2

union
SELECT 
    仓库
    ,'轻型货架库位-规划' type
    ,count(DISTINCT (if(t1.'库位编码' like 'FA-B%', t1.'库位编码',0)))  -1             '轻型货架库位-规划'
FROM  AGV t1 
    WHERE 1=1
GROUP BY 1,2

union
SELECT 
    仓库
    ,'轻型货架库位' type
    ,sum(if(((t1.'库位编码' like 'FA-B%') and t1.'正品库存' > 0 ), 1, 0))                 '轻型货架库位'
FROM  AGV t1 
    WHERE 1=1
GROUP BY 1,2

union
SELECT 
    仓库
    ,'轻型货架库容-规划' type
    ,sum(if(t1.'库位编码' like 'FA-B%',t1.'体积',0)） / 1000000000                      '轻型货架库容-规划'
FROM  AGV t1 
    WHERE 1=1
GROUP BY 1,2

union
SELECT 
    仓库
    ,'轻型货架库容' type
    ,sum(if(((t1.'库位编码' like 'FA-B%') and t1.'正品库存' > 0), t1.'体积', 0)) / 1000000000    '轻型货架库容' #使用
FROM  AGV t1 
    WHERE 1=1
GROUP BY 1,2

################################################# 第二版 ##################################################
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
               --  date_diff(now(),audit_time)<=14
            group BY 1
        ) T1 ON sglr.seller_id=T1.SELLER_ID
        where 1=1
        AND inventory>0 -- 库存大于0
        AND sl.disabled='0' -- 启用状态的货主
        and sl.name not in ('FFM-TH') -- 剔除物料
    
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
        and sl.name not in ('FFM-TH') -- 剔除物料
)
, agv as
(
    SELECT 
        'AGV' as '仓库'
        ,bes.`code` '库位编码'
        ,bes.`width`*bes.`height`*bes.`depth` '体积'
        ,if(bi.`num`>0,1,0)  '正品库存'
    FROM `was`.`base_entity_slot` bes 
     JOIN 
    (
        select
            slot_code
            ,MAX(num) 'num'
        FROM `was`.`base_inventory` bi
        LEFT JOIN wms_production.seller sl on bi.owner_id=sl.id
        where 1=1
            and bi.`group_id` = '1180'   #一个库位有多个业务实体，园区code一致
            and sl.name not in ('FFM-TH') -- 剔除物料
        group by slot_code
    ) bi on bes.`code`= bi.`slot_code`
    LEFT JOIN `was`.`base_entity_bucket` beb on bes.`bucket_code`=beb.`code` and bes.park_id=beb.park_id
    LEFT JOIN `was`.`base_entity_opr_area` beoa on beb.`opr_area_code`=beoa.`code`  and beb.`park_id` = beoa.`park_id`  #+园区条件
    LEFT JOIN `was`.`base_entity_opr_area_group` beoag on beoa.`park_id`=beoag.`park_id`
    WHERE bes.`del_flag`= 0 
        AND bes.`enable_flag`= 1     #库位启用状态为启用
        AND beoa.`status`='SELLABLE'   #作业区为可售状态
        AND beoag.`name`='AGV WH GROUP 01'  #作业区组为AGV
        AND bes.park_id = 16          #园区为AGV
        AND (bes.`code` like 'H%' or bes.`code` like 'FA-B%')
)

SELECT 
    仓库名称
    ,'货主数'指标
    ,count(DISTINCT seller_id)数值  
FROM  A WHERE 仓库名称 IS NOT NULL AND quality_status ='normal'
GROUP BY 1,2

UNION 
SELECT 
    仓库名称
    ,'7D活跃货主'指标
    ,count(DISTINCT seller_id) 
FROM  A
WHERE 仓库名称 IS NOT null 
    AND quality_status ='normal'
    AND 7D活跃货主='7D活跃货主' 
GROUP BY 1,2

UNION 
SELECT 
    仓库名称
    ,'14D活跃货主'指标
    ,count(DISTINCT seller_id) FROM  A
WHERE 仓库名称 IS NOT NULL
    AND quality_status ='normal'
    and 14D活跃货主='14D活跃货主'
GROUP BY 1,2

UNION
SELECT 
    仓库名称
    ,'SKU数'指标
    ,count(DISTINCT seller_goods_id) 
FROM  A
WHERE 仓库名称 IS NOT NULL
    AND quality_status ='normal'
GROUP BY 1,2

UNION
SELECT 
    仓库名称
    ,'小件'指标
    ,count(DISTINCT CASE WHEN TYPE ='小件' THEN seller_goods_id end) 
FROM  A
WHERE 仓库名称 IS NOT NULL
    AND quality_status ='normal'
GROUP BY 1,2

UNION
SELECT 
    仓库名称
    ,'中件'指标
    ,count(DISTINCT CASE WHEN TYPE ='中件' THEN seller_goods_id end) 
FROM  A
WHERE 仓库名称 IS NOT NULL
    AND quality_status ='normal'
GROUP BY 1,2

UNION
SELECT 
    仓库名称
    ,'大件'指标
    ,count(DISTINCT CASE WHEN TYPE ='大件' THEN seller_goods_id end) 
FROM  A
WHERE 仓库名称 IS NOT NULL
    AND quality_status ='normal'
GROUP BY 1,2

UNION
SELECT 
    仓库名称
    ,'超大件'指标
    ,count(DISTINCT CASE WHEN TYPE ='超大件' THEN seller_goods_id end) 
FROM  A
WHERE 仓库名称 IS NOT NULL
    AND quality_status ='normal'
GROUP BY 1,2

UNION
SELECT 
    仓库名称,
    '信息不全'指标
    ,count(DISTINCT CASE WHEN TYPE ='信息不全' THEN seller_goods_id end) 
FROM  A
WHERE 仓库名称 IS NOT NULL
AND  quality_status ='normal'
GROUP BY 1,2

UNION
SELECT 
    仓库名称
    ,'正品库存'指标
    ,sum(inventory) 
FROM  A
WHERE 仓库名称 IS NOT NULL
AND quality_status ='normal'
GROUP BY 1,2

UNION
SELECT 仓库名称,'小件库存'指标,sum(CASE WHEN TYPE ='小件' THEN inventory ELSE 0 end)/sum(inventory) FROM  A
WHERE 仓库名称 IS NOT NULL
AND  quality_status ='normal'
GROUP BY 1,2

UNION
SELECT 仓库名称,'中件库存'指标,sum(CASE WHEN TYPE ='中件' THEN inventory ELSE 0  end)/sum(inventory) FROM  A
WHERE 仓库名称 IS NOT NULL
AND quality_status ='normal'
GROUP BY 1,2

UNION
SELECT 仓库名称,'大件库存'指标,sum(CASE WHEN TYPE ='大件' THEN inventory ELSE 0  end)/sum(inventory) FROM  A
WHERE 仓库名称 IS NOT NULL
AND quality_status ='normal'
GROUP BY 1,2

UNION
SELECT 仓库名称,'超大件库存'指标,sum(CASE WHEN TYPE ='超大件' THEN inventory ELSE 0  end)/sum(inventory) FROM  A
WHERE 仓库名称 IS NOT NULL
AND quality_status ='normal'
GROUP BY 1,2

UNION
SELECT 仓库名称,'信息不全库存'指标,sum(CASE WHEN TYPE ='信息不全' THEN inventory ELSE 0  end)/sum(inventory) FROM  A
WHERE 仓库名称 IS NOT NULL
AND quality_status ='normal'
GROUP BY 1,2

UNION
SELECT 仓库名称,'体积'指标,sum(volumeAll) FROM  A
WHERE 仓库名称 IS NOT NULL
AND quality_status ='normal'
GROUP BY 1,2
UNION
SELECT 仓库名称,'小件库存-体积'指标,sum(CASE WHEN TYPE ='小件' THEN volumeAll ELSE 0 end)/sum(volumeAll) FROM  A
WHERE 仓库名称 IS NOT NULL
AND  quality_status ='normal'
GROUP BY 1,2
UNION
SELECT 仓库名称,'中件库存-体积'指标,sum(CASE WHEN TYPE ='中件' THEN volumeAll ELSE 0  end)/sum(volumeAll) FROM  A
WHERE 仓库名称 IS NOT NULL
AND quality_status ='normal'
GROUP BY 1,2
UNION
SELECT 仓库名称,'大件库存-体积'指标,sum(CASE WHEN TYPE ='大件' THEN volumeAll ELSE 0  end)/sum(volumeAll) FROM  A
WHERE 仓库名称 IS NOT NULL
AND quality_status ='normal'
GROUP BY 1,2
UNION
SELECT 仓库名称,'超大件库存-体积'指标,sum(CASE WHEN TYPE ='超大件' THEN volumeAll ELSE 0  end)/sum(volumeAll) FROM  A
WHERE 仓库名称 IS NOT NULL
AND quality_status ='normal'
GROUP BY 1,2
UNION
SELECT 仓库名称,'信息不全库存-体积'指标,sum(CASE WHEN TYPE ='信息不全' THEN volumeAll ELSE 0  end)/sum(volumeAll) FROM  A
WHERE 仓库名称 IS NOT NULL
AND quality_status ='normal'
GROUP BY 1,2

UNION
SELECT 仓库名称,'残品库存'指标,sum(inventory) FROM  A
WHERE 仓库名称 IS NOT NULL
AND quality_status <>'normal'
GROUP BY 1,2


union 
select 
    仓库名称
    ,'拣选区一品一位'type
    , sum(case when num = 1 then 1 else 0 end)/count(seller_goods_id) 
from 
    (
        select 
            仓库名称
            ,seller_goods_id
            ,count(location_id)num 
        from 
            (
                SELECT 
                    仓库名称
                    ,location_id
                    ,seller_goods_id
                    ,sum(inventory) 
                FROM a
                where 仓库名称 IS NOT null
                    and quality_status='normal'
                    AND use_attribute='pick'
                group by 1,2,3
            ) 
        group by 1,2
    ) GROUP BY 1,2
UNION 
select 
    仓库名称
    ,'拣选区一位一品'type
    , sum(case when num = 1 then 1 else 0 end)/count(location_id) 
from 
(
    select 
        仓库名称
        ,location_id
        ,count(seller_goods_id)num 
    from 
        (
            SELECT 
                仓库名称
                ,location_id
                ,seller_goods_id
                ,sum(inventory) 
            FROM a
            where 仓库名称 IS NOT null
                and quality_status='normal'
                AND use_attribute='pick'
            group by 1,2,3
        ) group by 1,2
)GROUP BY 1,2

union
SELECT 
    仓库名称
    ,'拣选区SKU覆盖率' type
    ,count( distinct case when use_attribute='pick' then seller_goods_id end)/count(distinct seller_goods_id )
FROM a where 仓库名称 IS NOT null
and quality_status='normal'
GROUP BY 1,2

UNION
SELECT 
    仓库名称
    ,'总体积' type
    ,sum(LENGTH*width*height*inventory)/1e9 
FROM  A WHERE 仓库名称 in ('BST', 'LAS') 
AND quality_status ='normal'
and(LENGTH is not null and width is not null and height is not null)
GROUP BY 1,2
UNION 
SELECT 
    仓库名称
    ,'高位货架体积' type
    ,sum(LENGTH*width*height*inventory)/1e9 
FROM  A WHERE 仓库名称 in ('BST', 'LAS') 
    AND quality_status ='normal'
    and(LENGTH is not null and width is not null and height is not null)
    AND 货架类型='高位货架'
GROUP BY 1,2
UNION 
SELECT 
    仓库名称
    ,'轻型货架体积' type
    ,sum(LENGTH*width*height*inventory)/1e9 
FROM  A WHERE 仓库名称 in ('BST', 'LAS') 
    AND quality_status ='normal'
    and(LENGTH is not null and width is not null and height is not null)
    AND 货架类型='轻型货架'
GROUP BY 1,2
UNION 
SELECT 
    仓库名称
    ,'地堆体积' type
    ,sum(LENGTH*width*height*inventory)/1e9 
FROM  A WHERE 仓库名称 in ('BST', 'LAS') 
    AND quality_status ='normal'
    and(LENGTH is not null and width is not null and height is not null)
AND 货架类型='地堆'
GROUP BY 1,2

-- 库容只看BST
UNION
SELECT 
    仓库名称
    ,'总使用库位' type
    ,count(DISTINCT location_code)
FROM  A WHERE 仓库名称 in ('BST', 'LAS') 
AND quality_status ='normal'
AND left(location_code,1)IN('A','B','C','D','E','J')
GROUP BY 1,2

UNION 
SELECT 
    仓库名称
    ,'高位货架库位' type
    ,count(DISTINCT location_code)
FROM  A WHERE 仓库名称 in ('BST', 'LAS') 
    AND quality_status ='normal'
    AND 货架类型='高位货架'
    AND left(location_code,1)IN('A','B','C','D','E','J')
GROUP BY 1,2

UNION 
SELECT 
    仓库名称
    ,'轻型货架库位' type
    ,count(DISTINCT location_code)
FROM  A WHERE 仓库名称 in ('BST', 'LAS') 
    AND quality_status ='normal'
    AND 货架类型='轻型货架'
    AND left(location_code,1)IN('A','B','C','D','E','J')
GROUP BY 1,2

UNION 
SELECT 
    仓库名称
    ,'地堆库位' type
    ,count(DISTINCT location_code)
FROM  A WHERE 仓库名称 in ('BST', 'LAS') 
    AND quality_status ='normal'
    AND 货架类型='地堆'
    AND left(location_code,1)IN('A','B','C','D','E','J')
GROUP BY 1,2

-- AGV
-- union
-- SELECT 
--     仓库
--     ,'规划库位数' type
--     ,count(DISTINCT t1.'库位编码') 规划库位数
-- FROM  AGV t1 
--     WHERE 1=1
-- GROUP BY 1,2

union
SELECT 
    仓库
    ,'总使用库位' type
    ,sum(if(t1.'正品库存' > 0, 1, 0)) 使用库位数
FROM  AGV t1 
    WHERE 1=1
GROUP BY 1,2

union
SELECT 
    仓库
    ,'库位利用率' type
    ,sum(if(t1.'正品库存' > 0, 1, 0)) / count(distinct t1.'库位编码') '库位利用率' 
FROM  AGV t1 
    WHERE 1=1
GROUP BY 1,2

-- union
-- SELECT 
--     仓库
--     ,'规划库容' type
--     ,sum(t1.'体积') / 1000000000                      '规划库容'
-- FROM  AGV t1 
--     WHERE 1=1
-- GROUP BY 1,2

union
SELECT 
    仓库
    ,'总体积' type
    ,sum(if(t1.'正品库存' > 0, t1.'体积', 0)) / 1000000000                '使用库容'
FROM  AGV t1 
    WHERE 1=1
GROUP BY 1,2

union
SELECT 
    仓库
    ,'库容利用率' type
    ,sum(if(t1.'正品库存' > 0, t1.'体积', 0))/sum(t1.'体积')                   '库容利用率' 
FROM  AGV t1 
    WHERE 1=1
GROUP BY 1,2

-- union
-- SELECT 
--     仓库
--     ,'高位货架库位-规划' type
--     ,count(DISTINCT (if(t1.'库位编码' like 'H%', t1.'库位编码',0)))  -1             '高位货架库位-规划'
-- FROM  AGV t1 
--     WHERE 1=1
-- GROUP BY 1,2

union
SELECT 
    仓库
    ,'高位货架库位' type
    ,sum(if(((t1.'库位编码' like 'H%') and t1.'正品库存' > 0 ), 1, 0))                 '高位货架库位'
FROM  AGV t1 
    WHERE 1=1
GROUP BY 1,2

-- union
-- SELECT 
--     仓库
--     ,'高位货架库容-规划' type
--     ,sum(if(t1.'库位编码' like 'H%',t1.'体积',0)） / 1000000000                      '高位货架库容-规划'
-- FROM  AGV t1 
--     WHERE 1=1
-- GROUP BY 1,2

union
SELECT 
    仓库
    ,'高位货架体积' type
    ,sum(if(((t1.'库位编码' like 'H%') and t1.'正品库存' > 0), t1.'体积', 0)) / 1000000000    '高位货架库容'
FROM  AGV t1 
    WHERE 1=1
GROUP BY 1,2

-- union
-- SELECT 
--     仓库
--     ,'轻型货架库位-规划' type
--     ,count(DISTINCT (if(t1.'库位编码' like 'FA-B%', t1.'库位编码',0)))  -1             '轻型货架库位-规划'
-- FROM  AGV t1 
--     WHERE 1=1
-- GROUP BY 1,2

union
SELECT 
    仓库
    ,'轻型货架库位' type
    ,sum(if(((t1.'库位编码' like 'FA-B%') and t1.'正品库存' > 0 ), 1, 0))                 '轻型货架库位'
FROM  AGV t1 
    WHERE 1=1
GROUP BY 1,2

-- union
-- SELECT 
--     仓库
--     ,'轻型货架库容-规划' type
--     ,sum(if(t1.'库位编码' like 'FA-B%',t1.'体积',0)） / 1000000000                      '轻型货架库容-规划'
-- FROM  AGV t1 
--     WHERE 1=1
-- GROUP BY 1,2

union
SELECT 
    仓库
    ,'轻型货架体积' type
    ,sum(if(((t1.'库位编码' like 'FA-B%') and t1.'正品库存' > 0), t1.'体积', 0)) / 1000000000    '轻型货架库容' #使用
FROM  AGV t1 
    WHERE 1=1
GROUP BY 1,2
