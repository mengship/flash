-- 1 请假情况
select 
    '请假情况'
    ,仓库
    ,人员信息 工号
    ,si.name
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
from dwm.dwd_th_ffm_staff_dayV2 sd
left join `fle_staging`.`staff_info` si on sd.人员信息 = si.id
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

-- 2 临时工工时
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

-- 3 本周不活跃货主
with temp as
(
    select
        '本周不活跃货主'
        ,seller.warehouse_name
        ,seller.seller_name
        ,seller.seller_id
    from
    (
        -- wms
        select
            case when w.name='AutoWarehouse' then 'AGV'
                when w.name='BPL-Return Warehouse' then 'BPL-Return'
                when w.name='BPL3-LIVESTREAM' then 'BPL3'
                when w.name='BangsaoThong' then 'BST'
                when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS'
                when w.name='LCP Warehouse' then 'LCP' end warehouse_name
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
        ,warehouse_name
            ,count(delivery_sn) 14天销量
        from
        dwm.dwd_th_ffm_outbound_dayV2
        where 1=1
            and TYPE='B2C'
            and date(delivery_time) >= SUBDATE(subdate(curdate(),date_format(curdate(),'%w') - 1),2) -- 大于等于上周六
            and date(delivery_time) <= subdate(curdate(),if(date_format(curdate(),'%w')=0,7,date_format(curdate(),'%w'))-5) -- 小于等于本周五
            and warehouse_name in ('AGV', 'BPL-Return', 'BPL3', 'BST', 'LAS')
        group by 1,2
        having count(delivery_sn)>0
    ) cnt on seller.seller_id = cnt.seller_id and seller.warehouse_name = cnt.warehouse_name
        where 1=1
        and cnt.seller_id is null
        and seller.warehouse_name in ('AGV', 'BPL-Return', 'BPL3', 'BST', 'LAS')
)
select
    '本周不活跃货主'
    ,temp.warehouse_name
    ,temp.seller_name
    ,max(out.delivery_time)
from
(
    select
    seller_id
    ,delivery_time
    , warehouse_name
    FROM dwm.dwd_th_ffm_outbound_dayV2
  where  TYPE='B2C'and warehouse_name in ('AGV', 'BPL-Return', 'BPL3', 'BST', 'LAS')

) out
join temp on out.seller_id = temp.seller_id and out.warehouse_name = temp.warehouse_name
where 1=1
temp.warehouse_name not in ('FFM-TH', '北京测试货主-请勿禁用')
group by temp.warehouse_name
,temp.seller_name


-- 4 包材周转天数
WITH A AS
(
    select
        warehouse_name
        ,seller_goods_id
        ,bar_code
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
            ,slg.bar_code
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
        ,bar_code
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
    title_name
    ,warehouse_name
    ,bar_code
    ,goods_name
    ,inventory
    ,goods_number_avg30
    ,turnover_days
from
(
    select
        '包材周转天数' title_name
        ,a.warehouse_name
        ,a.bar_code
        ,a.goods_name
        ,a.inventory
        ,b.goods_number_avg30
        ,round(a.inventory / b.goods_number_avg30, 2) turnover_days
    from
    A
    left join
    b on a.warehouse_name = b.warehouse_name and a.seller_goods_id = b.seller_goods_id

    union

    -- erp的周转率
    select
        '包材周转天数' title_name
        ,'erp' warehouse_name
        ,cs.bar_code
        ,cs.name goods_name
        ,cs.total_inventory inventory
        ,round(cul.num/30, 2) goods_number_avg30
        ,round(cs.total_inventory/(cul.num/30), 2) turnover_days
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
) t_out
;


# 工作量

        

-- AGV 拣货
select 
    'AGV' warehouse_name
    ,title_name
    ,operator
    ,user_name
    ,pickNum
    ,row_number() over(order by pickNum desc) rn
    ,pickNum pickNum_week
    ,pickNum/7 pickNum_week_avg
from
(
    -- 拣选
    SELECT
        -- left(oobt.gmt_modified,10) date
        '拣货' title_name
        -- ,oobt.operator user_id
        -- ,mo.job_number
        -- ,si.name
        ,oobt.operator
        ,bau.user_name
        -- ,count(DISTINCT ( oobt.order_code )) pickOrder
        ,sum(case when  wobo.type in(1)  then actual_num end)pickNum -- 2,3是ToB
        -- ,sum(actual_num) pickNum
    FROM was.oub_out_bound_task oobt
    LEFT JOIN was.base_authority_user bau ON oobt.operator = bau.user_id
    LEFT JOIN was.was_out_bound_order wobo  on  wobo.order_code = CONVERT(oobt.order_code using gbk) and oobt.group_id = wobo.group_id
    left join wms_production.member mo on oobt.operator = mo.id
    LEFT JOIN `fle_staging`.`staff_info` si on si.`id` =oobt.operator
    LEFT JOIN wms_production.seller sl on oobt.owner_id=sl.id
    WHERE 1=1
        and oobt.del_flag = 0 -- 未删除
        AND oobt.status = 1
        AND oobt.group_id = 1180 -- AGV仓
        AND oobt.type in (1,5)
        and sl.name not in ('FFM-TH') -- 剔除物料
        and oobt.gmt_modified >= convert_tz(SUBDATE(subdate(curdate(),date_format(curdate(),'%w') - 1),2) , '+07:00', '+08:00') -- 大于等于上周六
        and oobt.gmt_modified <= convert_tz(subdate(curdate(),if(date_format(curdate(),'%w')=0,7,date_format(curdate(),'%w'))-6) , '+07:00', '+08:00') -- 小于等于本周五
        /* and left(oobt.gmt_modified,7)=left(date_sub(date_add(now(),interval -60 minute),interval 1 month),7) -- @todo */
    GROUP BY 1,2 
) pick
union
-- AGV 打包
select
    'AGV' warehouse_name
    ,title_name
    ,creator
    ,user_name
    ,packNum
    ,row_number() over(order by packNum desc) rn
    ,packNum packNum_week
    ,packNum/7 packNum_week_avg
from
(
    SELECT
        '打包' title_name
        ,wsg.creator
        ,bau.user_name
        -- left(wsg.operation_time,10)date
        -- ,wsg.creator user_id
        -- ,count(DISTINCT ( wsg.relevance_code )) packOrder
        -- ,sum(item_num) packNum
        ,sum(case when wobo.type in(1) then item_num END)packNum 
    FROM was.was_status_group wsg
    LEFT JOIN was.base_authority_user bau ON wsg.creator = bau.user_id
    left join was.was_package wp on wsg.relevance_code = wp.order_code
    left join was.was_out_bound_order wobo  on  wobo.order_code = wp.order_code and wp.group_id = wobo.group_id
    LEFT JOIN wms_production.seller sl on wp.owner_id=sl.id
    WHERE 1=1
        and wsg.status = 'FINISH_PACK'
        AND wsg.del_flag = 0
        AND wsg.type = 1
        and sl.name not in ('FFM-TH') -- 剔除物料
        # AND wsg.group_id = 1180
        -- AND left(wsg.operation_time,7)=left(date_sub(date_add(now(),interval -60 minute),interval 1 month),7) -- @todo
        and wsg.operation_time >= convert_tz(SUBDATE(subdate(curdate(),date_format(curdate(),'%w') - 1),2) , '+07:00', '+08:00') -- 大于等于上周六
        and wsg.operation_time <= convert_tz(subdate(curdate(),if(date_format(curdate(),'%w')=0,7,date_format(curdate(),'%w'))-6) , '+07:00', '+08:00') -- 小于等于本周五
    GROUP BY 1, 2 ,3
) pack
union
-- AGV 出库
select
    'AGV' warehouse_name
    ,out.title_name
    ,out.out_operator
    ,out.real_name
    ,out.outnum
    ,row_number() over(order by out.outnum desc) rn
    ,out.outnum outNum_week
    ,out.outnum/7 outNum_week_avg
from
(
    select 
        '出库' title_name
        ,do.out_operator
        ,mb.real_name
        ,count (distinct do.delivery_sn) outnum
    from dwm.dwd_th_ffm_outbound_dayV2 do
    LEFT JOIN `wms_production`.`member` mb on do.out_operator=mb.`id`
    where 1=1
        and do.warehouse_id='36'
        and 'B2C'=do.TYPE
        -- and left (do.delivery_time,7)=left(date_sub(date_add(now(),interval -60 minute),interval 1 month),7) -- @todo
        and date(do.delivery_time) >= SUBDATE(subdate(curdate(),date_format(curdate(),'%w') - 1),2) -- 大于等于上周六
        and date(do.delivery_time) <= subdate(curdate(),if(date_format(curdate(),'%w')=0,7,date_format(curdate(),'%w'))-5) -- 小于等于本周五
    group by 1,2,3
) out

-- 'BPL-Return', 'BST', 'LAS' 拣货
union
select
    warehouse_name
    ,t_out.title_name
    ,t_out.picker_id
    ,t_out.real_name
    ,t_out.picknum
    ,row_number() over(order by t_out.picknum desc) rn
    ,t_out.picknum picknum_week
    ,t_out.picknum/7 picknum_week_avg
from
(
    select
        '拣货' title_name
        ,warehouse_name
        ,picker_id
        ,real_name
        ,sum(picknum) picknum
    from
    (
        select 
            '拣货' title_name
            ,case when w.name='AutoWarehouse'   then 'AGV'
                when w.name='BPL-Return Warehouse'  then 'BPL-Return'
                when w.name='BPL3-LIVESTREAM'   then 'BPL3'
                when w.name='BangsaoThong'  then 'BST'
                when w.name IN ('BKK-WH-LAS2电商仓')    then 'LAS'
                when w.name ='LCP Warehouse' then 'LCP' end warehouse_name
            ,po.picker_id
            ,m.real_name
            ,po.goods_num picknum
            ,po.pick_sn

        from `wms_production`.`pick_order` po 
        left join `wms_production`.`pick_order_delivery_ref` podr  on po.id = podr.pick_order_id
        left join `wms_production`.`delivery_order` do  on podr.delivery_order_id = do.id
        left join `wms_production`.`warehouse` w on do.warehouse_id = w.id
        LEFT JOIN `wms_production`.`member` m on po.picker_id = m.id
        where 1=1
            and date(po.created) >= SUBDATE(subdate(curdate(),date_format(curdate(),'%w') - 1),2) -- 大于等于上周六
            and date(po.created) <= subdate(curdate(),if(date_format(curdate(),'%w')=0,7,date_format(curdate(),'%w'))-5) -- 小于等于本周五
            -- and date(po.created) ='2024-08-24'
            and do.prepacker is null 
            and do.status >= 2030 
            and do.status <= 3020 
            and po.type = 1 
            and po.picker_id is not null
        group by 1,2,3,4,5,6
    )
    group by 1,2,3
) t_out
where 1=1
    and warehouse_name in ('BPL-Return', 'BST', 'LAS')
union
-- 打包
select
    warehouse_name
    ,t_out.title_name
    ,t_out.pack_id
    ,t_out.real_name
    ,t_out.packnum
    ,row_number() over(order by t_out.packnum desc) rn
    ,t_out.packnum packnum_week
    ,t_out.packnum/7 packnum_week_avg
from
(
    select
        '打包' title_name
        ,case when w.name='AutoWarehouse'   then 'AGV'
            when w.name='BPL-Return Warehouse'  then 'BPL-Return'
            when w.name='BPL3-LIVESTREAM'   then 'BPL3'
            when w.name='BangsaoThong'  then 'BST'
            when w.name IN ('BKK-WH-LAS2电商仓')    then 'LAS'
            when w.name ='LCP Warehouse' then 'LCP' end warehouse_name
        ,pack_id
        ,real_name
        ,sum(do.goods_num) packnum
    from
    (
            select
                delivery_order_id,
                pack_id,
                db.warehouse_id,
                db.creator_id,
                m.real_name,
                count(1) as boxNum
            from
                wms_production.delivery_box db
                LEFT JOIN `wms_production`.`member` m on db.pack_id = m.id
                where date(db.created)>= SUBDATE(subdate(curdate(),date_format(curdate(),'%w') - 1),2) -- 大于等于上周六
                and date(db.created)<=subdate(curdate(),if(date_format(curdate(),'%w')=0,7,date_format(curdate(),'%w'))-5) -- 小于等于本周五
            group by
                delivery_order_id,
                db.warehouse_id,
                pack_id,
                db.creator_id
    ) db
    left join `wms_production`.`delivery_order` do on do.id = db.delivery_order_id
    left join `wms_production`.`warehouse` w on db.warehouse_id = w.id
    group by 1,2,3,4
)t_out where 1=1
    and warehouse_name in ('BPL-Return', 'BST', 'LAS')


union 
-- 'BPL-Return', 'BST', 'LAS' 出库
select
    out.warehouse_name
    ,out.title_name
    ,out.out_operator
    ,out.real_name
    ,out.outnum
    ,row_number() over(order by out.outnum desc) rn
    ,out.outnum outNum_week
    ,out.outnum/7 outNum_week_avg
from
(
    select 
        '出库' title_name
        ,warehouse_name
        ,do.out_operator
        ,mb.real_name
        ,count (distinct do.delivery_sn) outnum
    from dwm.dwd_th_ffm_outbound_dayV2 do
    LEFT JOIN `wms_production`.`member` mb on do.out_operator=mb.`id`
    where 1=1
        and do.warehouse_name in ('BPL-Return', 'BST', 'LAS', 'BPL3')
        and 'B2C'=do.TYPE
        -- and left (do.delivery_time,7)=left(date_sub(date_add(now(),interval -60 minute),interval 1 month),7) -- @todo
        and date(do.delivery_time) >= SUBDATE(subdate(curdate(),date_format(curdate(),'%w') - 1),2) -- 大于等于上周六
        and date(do.delivery_time) <= subdate(curdate(),if(date_format(curdate(),'%w')=0,7,date_format(curdate(),'%w'))-5) -- 小于等于本周五
    group by 1,2,3,4
) out




union


-- 'BPL3' 拣货
select
    warehouse_name
    ,title_name
    ,operation_id
    ,real_name
    ,picknum
    ,row_number() over(order by out.picknum desc) rn
    ,out.picknum picknum_week
    ,out.picknum/7 picknum_week_avg
from
(
    select 
        do.warehouse_name
        ,'拣货' title_name
        ,ol.operation_id
        ,ol.real_name
        ,sum(do.goods_num) picknum
    from dwm.dwd_th_ffm_outbound_dayV2 do
    left join
    (
        select
            t_in.order_sn
            ,t_in.operation_id
            ,t_in.real_name
        from
        (
            select
                ol.order_sn
                ,ol.operation_id
                ,m.real_name real_name
                ,ol.created
                ,row_number() over(partition by ol.order_sn order by ol.created desc) rn
            from
            erp_wms_prod.operation_log ol
            left join erp_wms_prod.member m on ol.operation_id = m.id
            where 1=1
                and order_type = 'DeliveryOrder' -- 发货单
                and status_after=2030 -- 拣货完成
                and operation='confirmPick'
                -- and ol.`created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 90 day), '+07:00', '+08:00')
                and ol.`created` >='2023-12-01'
        ) t_in
        where rn = 1
    ) ol on do.delivery_sn = ol.order_sn
    where 1=1
        and do.warehouse_name='BPL3' 
        -- and left (do.delivery_time,7)=left(date_sub(date_add(now(),interval -60 minute),interval 1 month),7) -- @todo
        and date(do.delivery_time) >= SUBDATE(subdate(curdate(),date_format(curdate(),'%w') - 1),2) -- 大于等于上周六
        and date(do.delivery_time) <= subdate(curdate(),if(date_format(curdate(),'%w')=0,7,date_format(curdate(),'%w'))-5) -- 小于等于本周五
    group by 1,2,3,4
) out

union

-- 'BPL3' 打包
select
    warehouse_name
    ,title_name
    ,operation_id
    ,real_name
    ,picknum
    ,row_number() over(order by out.picknum desc) rn
    ,out.picknum picknum_week
    ,out.picknum/7 picknum_week_avg
from
(
    select 
        do.warehouse_name
        ,'打包' title_name
        ,ol.operation_id
        ,ol.real_name
        ,sum(do.goods_num) picknum
    from dwm.dwd_th_ffm_outbound_dayV2 do
    left join
    (
        select
            t_in.order_sn
            ,t_in.operation_id
            ,t_in.real_name
        from
        (
            select
                ol.order_sn
                ,ol.operation_id
                ,m.real_name real_name
                ,ol.created
                ,row_number() over(partition by ol.order_sn order by ol.created desc) rn
            from
            erp_wms_prod.operation_log ol
            left join erp_wms_prod.member m on ol.operation_id = m.id
            where 1=1
                and order_type = 'DeliveryOrder' -- 发货单
                and status_after=2040 -- 拣货完成
                and operation='packageFinish'
                -- and ol.`created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 90 day), '+07:00', '+08:00')
                and ol.`created` >='2023-12-01'
        ) t_in
        where rn = 1
    ) ol on do.delivery_sn = ol.order_sn
    where 1=1
        and do.warehouse_name='BPL3' 
        -- and left (do.delivery_time,7)=left(date_sub(date_add(now(),interval -60 minute),interval 1 month),7) -- @todo
        and date(do.delivery_time) >= SUBDATE(subdate(curdate(),date_format(curdate(),'%w') - 1),2) -- 大于等于上周六
        and date(do.delivery_time) <= subdate(curdate(),if(date_format(curdate(),'%w')=0,7,date_format(curdate(),'%w'))-5) -- 小于等于本周五
    group by 1,2,3,4
) out

-- BPL3 出库