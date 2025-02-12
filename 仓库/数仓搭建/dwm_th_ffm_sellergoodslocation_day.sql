/*=====================================================================+
表名称：  dwm_th_ffm_sellergoodslocation_day
功能描述： 泰国ffm库存库位数据（天粒度汇总）
                        
需求来源：
编写人员: 王昱棋
设计日期：2024/11/09
        修改日期: 
        修改人员:         
        修改原因: 
-----------------------------------------------------------------------
---存在问题：
-----------------------------------------------------------------------
+=====================================================================*/ 

drop table if exists dwm.dwm_th_ffm_sellergoodslocation_day;
create table dwm.dwm_th_ffm_sellergoodslocation_day as
-- delete from dwm.dwm_th_ffm_sellergoodslocation_day where statc_date >= date_sub(date(now() + interval -1 hour),interval 90 day); -- 先删除数据
-- insert into dwm.dwm_th_ffm_sellergoodslocation_day -- 再插入数据
select 
    a.statc_date
    ,a.warehouse_name
    ,a.货主数
    ,a.7D活跃货主
    ,a.14D活跃货主
    ,a.SKU数
    ,a.小件
    ,a.中件
    ,a.大件
    ,a.超大件
    ,a.信息不全
    ,a.其他
    ,a.正品库存
    ,a.小件库存
    ,a.中件库存
    ,a.大件库存
    ,a.超大件库存
    ,a.信息不全库存
    ,a.其他库存
    ,b.残品库存
    ,c.拣选区一品一位
    ,d.拣选区一位一品	
    ,a.拣选区SKU覆盖率
    ,f.规划库位
    ,f.规划库位_轻型货架
    ,f.规划库位_地堆
    ,f.规划库位_高位货架
    ,e.总使用库位
    ,e.高位货架使用库位
    ,e.轻型货架使用库位
    ,e.地堆使用库位	
    ,f.规划库容
    ,f.规划库容_轻型货架
    ,f.规划库容_地堆
    ,f.规划库容_高位货架
    ,e.总使用库容
    ,e.高位货架使用库容
    ,e.轻型货架使用库容
    ,e.地堆使用库容
    ,e.总使用库位/f.规划库位 as 库位利用率
    ,e.轻型货架使用库位/f.规划库位_轻型货架 as 轻型货架库位利用率
    ,e.地堆使用库位/f.规划库位_地堆 as 地堆库位利用率
    ,e.高位货架使用库位/f.规划库位_高位货架 as 高位货架库位利用率
    ,e.总使用库容/(f.规划库容_轻型货架*0.53+f.规划库容_地堆*0.77+f.规划库容_高位货架*0.64) as 库容利用率
    ,e.轻型货架使用库容/(f.规划库容_轻型货架*0.53) as 轻型货架库容利用率
    ,e.地堆使用库容/(f.规划库容_地堆*0.77) as 地堆库容利用率
    ,e.高位货架使用库容/(f.规划库容_高位货架*0.64) as 高位货架库容利用率		
from 
(
    SELECT 
        statc_date
        ,warehouse_name
        ,count(DISTINCT seller_id) 货主数
        ,count(distinct if(7Dactivesellername='7D活跃货主', seller_id, null)) 7D活跃货主
        ,count(DISTINCT if(14Dactivesellername='14D活跃货主', seller_id, null)) 14D活跃货主
        ,count(DISTINCT seller_goods_id)  SKU数
        ,count(DISTINCT CASE WHEN TYPE ='小件' THEN seller_goods_id end) 小件
        ,count(DISTINCT CASE WHEN TYPE ='中件' THEN seller_goods_id end) 中件
        ,count(DISTINCT CASE WHEN TYPE ='大件' THEN seller_goods_id end) 大件
        ,count(DISTINCT CASE WHEN TYPE ='超大件' THEN seller_goods_id end) 超大件
        ,count(DISTINCT CASE WHEN TYPE ='信息不全' THEN seller_goods_id end) 信息不全
        ,count(DISTINCT CASE WHEN TYPE ='其他' THEN seller_goods_id end) 其他
        ,sum(inventory)  正品库存
        ,sum(CASE WHEN TYPE ='小件' THEN inventory ELSE 0 end)/sum(inventory) 小件库存
        ,sum(CASE WHEN TYPE ='中件' THEN inventory ELSE 0 end)/sum(inventory) 中件库存
        ,sum(CASE WHEN TYPE ='大件' THEN inventory ELSE 0 end)/sum(inventory) 大件库存
        ,sum(CASE WHEN TYPE ='超大件' THEN inventory ELSE 0 end)/sum(inventory) 超大件库存
        ,sum(CASE WHEN TYPE ='信息不全' THEN inventory ELSE 0 end)/sum(inventory) 信息不全库存		
        ,sum(CASE WHEN TYPE ='其他' THEN inventory ELSE 0 end)/sum(inventory) 其他库存			
        ,count( distinct case when use_attribute='pick' then seller_goods_id end)/count(distinct seller_goods_id )	拣选区SKU覆盖率	
    FROM  dwm.dwd_th_ffm_sellergoodslocation 
    WHERE warehouse_name IS NOT NULL 
    AND quality_status ='normal'
    -- AND statc_date >= date_sub(date(now() + interval -1 hour),interval 90 day)
    GROUP BY 1,2
) a 
left join 
(
    -- 残品
    SELECT
        statc_date
        ,warehouse_name
        ,sum(inventory) 残品库存 
    FROM  dwm.dwd_th_ffm_sellergoodslocation
    WHERE warehouse_name IS NOT NULL
    AND quality_status <>'normal'
    -- AND statc_date >= date_sub(date(now() + interval -1 hour),interval 90 day)
    GROUP BY 1,2
) b on a.statc_date = b.statc_date and a.warehouse_name = b.warehouse_name
left join 
(
    select 
        statc_date
        ,warehouse_name
        ,'拣选区一品一位'type
        , sum(case when num = 1 then 1 else 0 end)/count(seller_goods_id)  拣选区一品一位
    from 
    (
        select
                statc_date			
            ,warehouse_name
            ,seller_goods_id
            ,count(DISTINCT location_id)num 
        from 
        (
            SELECT
                statc_date  					
                ,warehouse_name
                ,location_id
                ,seller_goods_id
            FROM dwm.dwd_th_ffm_sellergoodslocation
            where warehouse_name IS NOT null
                and warehouse_name<>'AGV'
                and quality_status='normal'
                AND use_attribute='pick'
                -- AND statc_date >= date_sub(date(now() + interval -1 hour),interval 90 day)
            group by 1,2,3,4
        ) t 
        group by 1,2,3
    ) t
    GROUP BY 1,2,3
    union all -- AGV 注： AGV库位信息只有当前信息，历史暂时没有
    select
        date_sub(current_date, 1) as statc_date
        ,warehouse_name
        ,'拣选区一品一位' type
        ,sum(case when num = 1 then 1 else 0 end)/count(bar_code) 拣选区一品一位 
    from
    (
        select
            warehouse_name
            ,bar_code
            ,count(DISTINCT location_code)num
        from(
            select
                warehouse_name
                ,location_code
                ,bar_code
            from dwm.dwd_th_ffm_sellergoodslocationagv
            where location_code like 'FA-B%' -- 拣货区
            and normalinventory>0
            group by 1,2,3
        )
        group by 1,2			
    ) t0
    group by 1,2,3
) c on a.statc_date = c.statc_date and a.warehouse_name = c.warehouse_name
left join 
(
    select
        statc_date	
        ,warehouse_name
        ,'拣选区一位一品'type
        , sum(case when num = 1 then 1 else 0 end)/count(location_id)  拣选区一位一品
    from 
    (
        select
            statc_date		
            ,warehouse_name
            ,location_id
            ,count(DISTINCT seller_goods_id)num 
        from(
                SELECT
                    statc_date				
                    ,warehouse_name
                    ,location_id
                    ,seller_goods_id 
                FROM dwm.dwd_th_ffm_sellergoodslocation
                where warehouse_name IS NOT null
                    and warehouse_name<>'AGV'
                    and quality_status='normal'
                    AND use_attribute='pick'
                    -- AND statc_date >= date_sub(date(now() + interval -1 hour),interval 90 day)
                group by 1,2,3,4
            ) group by 1,2,3
    ) GROUP BY 1,2,3

    union all   -- AGV 注： AGV库位信息只有当前信息，历史暂时没有
    select
        date_sub(current_date, 1) as statc_date
        ,warehouse_name
        ,'拣选区一位一品' type
        ,sum(case when num = 1 then 1 else 0 end)/count(location_code)  拣选区一位一品
    from
    (
        select
            warehouse_name
            ,location_code
            ,count(DISTINCT bar_code) num
        from
        (
            select
                warehouse_name
                ,location_code
                ,bar_code
            from dwm.dwd_th_ffm_sellergoodslocationagv
            where location_code like 'FA-B%' -- 拣货区
            and normalinventory>0
            group by 1,2,3
        ) t0
        group by 1,2
    ) t1
    group by 1,2,3

) d  on a.statc_date = d.statc_date and a.warehouse_name = d.warehouse_name
left join 
(
    SELECT 
        statc_date
        ,warehouse_name	
        ,count(DISTINCT case when left(location_code,1) IN ('A','B','C','D','E','J') then location_code else null end) as 总使用库位
        ,count(DISTINCT case when left(location_code,1) IN ('A','B','C','D','E','J') and ref_type='高位货架' then location_code else null end) as 高位货架使用库位
        ,count(DISTINCT case when left(location_code,1) IN ('A','B','C','D','E','J') and ref_type='轻型货架' then location_code else null end) as 轻型货架使用库位
        ,count(DISTINCT case when left(location_code,1) IN ('A','B','C','D','E','J') and ref_type='地堆' then location_code else null end) as 地堆使用库位		
        ,sum(case when LENGTH is not null and width is not null and height is not null then LENGTH*width*height*inventory else 0 end)/1e9 as 总使用库容
        ,sum(case when LENGTH is not null and width is not null and height is not null and ref_type='高位货架' then LENGTH*width*height*inventory else 0 end)/1e9 as 高位货架使用库容
        ,sum(case when LENGTH is not null and width is not null and height is not null and ref_type='轻型货架' then LENGTH*width*height*inventory else 0 end)/1e9 as 轻型货架使用库容
        ,sum(case when LENGTH is not null and width is not null and height is not null and ref_type='地堆' then LENGTH*width*height*inventory else 0 end)/1e9 as 地堆使用库容
        
    FROM  dwm.dwd_th_ffm_sellergoodslocation 
    WHERE warehouse_name in ('BST', 'LAS') 
    AND quality_status ='normal'
    -- AND statc_date >= date_sub(date(now() + interval -1 hour),interval 90 day)
    GROUP BY 1,2
    UNION ALL  -- AGV 
    SELECT 
        date_sub(current_date, 1) as statc_date
        ,warehouse_name
        ,count(distinct location_code) 总使用库位
        ,count(distinct if(location_code like 'H%', location_code, null)) 高位货架使用库位
        ,count(distinct if(location_code like 'FA-B%', location_code, null)) 轻型货架使用库位
        ,null 地堆使用库位	
        ,sum(goodsvolume) 总使用库容
        ,sum(if(location_code like 'H%', goodsvolume, 0)) 高位货架使用库容
        ,sum(if(location_code like 'FA-B%', goodsvolume, 0)) 轻型货架使用库容
        ,null 地堆使用库容
    FROM dwm.dwd_th_ffm_sellergoodslocationagv
    WHERE normalinventory > 0
    GROUP BY 1,2
) e on a.statc_date = e.statc_date and a.warehouse_name = e.warehouse_name
left join 
(
    select
        'BST'   as warehouse_name
        ,19268  as 规划库位
        ,12636  as 规划库位_轻型货架
        ,1392   as 规划库位_地堆
        ,5240   as 规划库位_高位货架
        ,15249  as 规划库容  
        ,924    as 规划库容_轻型货架
        ,3007   as 规划库容_地堆
        ,11318  as 规划库容_高位货架
    union all
    select
        'AGV'   as warehouse_name
        ,46974  as 规划库位
        ,45152  as 规划库位_轻型货架
        ,682    as 规划库位_地堆
        ,1140   as 规划库位_高位货架
        ,7250   as 规划库容  
        ,3315   as 规划库容_轻型货架
        ,1473   as 规划库容_地堆
        ,2462   as 规划库容_高位货架       
    union all
    select
        'LAS' as warehouse_name 
        ,6729 as 规划库位 
        ,4680 as 规划库位_轻型货架 
        ,861  as 规划库位_地堆 
        ,1188  as 规划库位_高位货架 
        ,4707  as 规划库容
        ,281  as 规划库容_轻型货架 
        ,1860 as 规划库容_地堆 
        ,2566 as 规划库容_高位货架 

) f on a.warehouse_name = f.warehouse_name