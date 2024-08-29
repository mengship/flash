select 
    '请假情况'
    ,仓库
    ,人员信息 工号
    ,sum(旷工) + sum(请假) 缺勤天数
    ,sum(旷工) 旷工天数
    ,sum(请假) 请假天数
    ,sum(年假) 年假
    ,sum(事假) 事假
    ,sum(病假) 病假
    ,sum(产假) 产假
    ,sum(丧假) 丧假
    ,sum(婚假) 婚假
    ,sum(公司培训假) 公司培训假
    ,sum(跨国探亲假) 跨国探亲假
from dwm.dwd_th_ffm_staff_dayV2
where 1=1
    and 统计日期 >= SUBDATE(subdate(curdate(),date_format(curdate(),'%w') - 1),2) -- 大于等于上周六
    and 统计日期 <= subdate(curdate(),if(date_format(curdate(),'%w')=0,7,date_format(curdate(),'%w'))-5) -- 小于等于本周五
    and 仓库 in ('AGV', 'BPL-Return', 'BPL3', 'BST', 'LAS')
group by
    仓库
    ,人员信息
having sum(旷工) + sum(请假) >0
order by 
    仓库
    ,sum(旷工) + sum(请假) desc
;

select 
    '临时工工时'
    ,warehouse 仓库
    ,sum(num_people) 临时工人次
    ,sum(num_people*8+num_ot) 总工时
    ,sum(num_people)*8 普通工时
    ,sum(num_ot) OT工时
FROM dwm.th_ffm_tempworker_input 
WHERE 1=1
    and warehouse in ('AGV', 'BPL-Return', 'BPL3', 'BST', 'LAS')
    and left(dt,10) >= SUBDATE(subdate(curdate(),date_format(curdate(),'%w') - 1),2) -- 大于等于上周六
    and left(dt,10) <= subdate(curdate(),if(date_format(curdate(),'%w')=0,7,date_format(curdate(),'%w'))-5) -- 小于等于本周五
GROUP BY 1,2
;


select
    '本周不活跃货主'
    ,seller.仓库名称
    ,seller.seller_name
from
(
    -- wms
    select
        case when w.name='AutoWarehouse' then 'AGV'
            when w.name='BPL-Return Warehouse' then 'BPL-Return'
            when w.name='BPL3-LIVESTREAM' then 'BPL3'
            when w.name='BangsaoThong' then 'BST'
            when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS'
            when w.name='LCP Warehouse' then 'LCP' end 仓库名称
        ,sl.name seller_name
        ,sglr.seller_id
    from
    wms_production.seller_goods_location_ref sglr
    LEFT JOIN wms_production.warehouse w ON sglr.warehouse_id=w.id
    LEFT JOIN  wms_production.seller sl ON sglr.seller_id=sl.id
    where 1=1
        and sglr.inventory>0 -- 库存大于0
        AND sl.disabled='0' -- 启用状态的货主   
    group by 1,2,3

    union
    -- erp
    select
        case when w.name='AutoWarehouse' then 'AGV'
            when w.name='BPL-Return Warehouse' then 'BPL-Return'
            when w.name='BPL3-LIVESTREAM' then 'BPL3'
            when w.name='BangsaoThong' then 'BST'
            when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS'
            when w.name='LCP Warehouse' then 'LCP' end 仓库名称
        ,sl.name seller_name
        ,iwc.seller_id
    from
        erp_wms_prod.in_warehouse_cost iwc
    LEFT JOIN erp_wms_prod.seller sl on iwc.seller_id = sl.id
    LEFT JOIN erp_wms_prod.warehouse w ON iwc.warehouse_id = w.id
    where 1=1
        and sl.disabled='0' -- 启用状态的货主
        and iwc.total_number>0 -- 库存大于0
    group by 1,2,3
) seller
left join
(
    select
        seller_id
        ,count(delivery_sn) 14天销量
    from
    dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and TYPE='B2C'
        and created_date >= SUBDATE(subdate(curdate(),date_format(curdate(),'%w') - 1),2) -- 大于等于上周六
        and created_date <= subdate(curdate(),if(date_format(curdate(),'%w')=0,7,date_format(curdate(),'%w'))-5) -- 小于等于本周五
        and warehouse_name in ('AGV', 'BPL-Return', 'BPL3', 'BST', 'LAS')
    group by 1
    having count(delivery_sn)>0
) cnt on seller.seller_id = cnt.seller_id
where 1=1
    and cnt.seller_id is null
    and seller.仓库名称 in ('AGV', 'BPL-Return', 'BPL3', 'BST', 'LAS')



WITH A AS
(
    select
        warehouse_name
        ,seller_goods_id
        ,goods_name
        ,sum(inventory) inventory
    from
    (
        SELECT 
            case when w.name='AutoWarehouse' then 'AGV'
                when w.name='BPL-Return Warehouse' then 'BPL-Return'
                when w.name='BPL3-LIVESTREAM' then 'BPL3'
                when w.name='BangsaoThong' then 'BST'
                when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS'
                when w.name='LCP Warehouse' then 'LCP' end warehouse_name
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
            ,slg.name goods_name
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
            and sl.name in ('FFM-TH') -- 只看物料
    ) a
    where warehouse_name in ('AGV', 'BPL-Return', 'BPL3', 'BST', 'LAS')
    group by warehouse_name
        ,seller_goods_id
        ,goods_name
    
), b as 
(
-- wms平台 耗材使用
    select
        warehouse_name
        ,seller_goods_id
        ,goods_name
        ,round(sum(goods_number)/30 ,2) goods_number_avg30
    from
    (
        SELECT 
            'B2B' TYPE
            ,return_warehouse_sn
            ,case when w.name='AutoWarehouse' then 'AGV'
                when w.name='BPL-Return Warehouse' then 'BPL-Return'
                when w.name='BPL3-LIVESTREAM' then 'BPL3'
                when w.name='BangsaoThong' then 'BST'
                when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS'
                when w.name='LCP Warehouse' then 'LCP' end warehouse_name
            ,'B2B'platform_source
            ,goods.goods_name
            ,goods.seller_goods_id
            ,goods.goods_number
        from  wms_production.return_warehouse do 
        LEFT JOIN wms_production.warehouse w ON do.warehouse_id=w.id
        LEFT JOIN `wms_production`.`seller` sl on do.`seller_id`=sl.`id`
        left join 
        (
            SELECT
                rwg.`return_warehouse_id` return_warehouse_id,
                rwg.`seller_goods_id` seller_goods_id,
                sg.name goods_name,
                rwg.`out_num` goods_number
            FROM
            `wms_production`.`return_warehouse_goods` rwg
            LEFT JOIN `wms_production`.`seller_goods` sg ON rwg.`seller_goods_id` = sg.`id`
            where rwg.created >= convert_tz(date_sub(date(now() + interval -1 hour),interval 40 day), '+07:00', '+08:00')
            group by 1,2,3
        ) goods on do.id = goods.return_warehouse_id
        WHERE 1=1
            and prompt ='0' 
            AND status>='1020'
            and do.`created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 30 day), '+07:00', '+08:00')
            and sl.name in ('FFM-TH') -- 只看物料
    ) goods
    where warehouse_name in ('AGV', 'BPL-Return', 'BPL3', 'BST', 'LAS')
    group by 1,2,3
)
select
    '包材周转天数'
    ,a.warehouse_name
    ,a.goods_name
    ,a.inventory
    ,b.goods_number_avg30
    ,round(a.inventory / b.goods_number_avg30, 2) 
from
A
left join
b on a.warehouse_name = b.warehouse_name and a.seller_goods_id = b.seller_goods_id

union

-- erp的周转率
select
    'erp'
    ,cs.name goods_name
    ,cs.total_inventory
    ,round(cul.num/30, 2) avgnum_30
    ,round(cs.total_inventory/(cul.num/30), 2) turnover
from
(
    -- erp 包材物料库存
    select
        ''
        ,c1.name
        ,c1.bar_code
        ,sum(total_inventory) total_inventory
        
    from
    erp_wms_prod.consumables_stock cs 
    left join erp_wms_prod.container c1 on cs.consumables_id = c1.id
    where cs.total_inventory>0
        and cs.warehouse_id=312
    group by c1.name
        ,c1.bar_code
) cs
left join
(
    -- erp 包材使用
    select 
        'erp包材耗材使用'
        ,c1.name
        ,c1.bar_code
        ,sum(cul.num) num
    from 
    erp_wms_prod.consumables_use_log cul
    left join erp_wms_prod.container c1 on cul.consumables_id=c1.id
    where 1=1
        and cul.in_out_type=2
        and cul.type=8
        and cul.modified >= convert_tz(date_sub(date(now() + interval -1 hour),interval 30 day), '+07:00', '+08:00')
    group by 
        c1.name
        ,c1.bar_code
) cul on cs.bar_code = cul.bar_code
