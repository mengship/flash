with a as 
(
    -- wms平台
    SELECT 
        'B2C' TYPE
        ,do.delivery_sn
        ,goods_num 
        ,warehouse_id
        ,w.name warehouse_name
        ,ps.`name` platform_source
        ,sl.`name` seller_name
        ,do.seller_id
        ,date_add(do.`created`, interval -60 minute) created_time
        ,left(date_add(do.`created`, interval -60 minute), 10) created_date
        ,date_add(do.`audit_time`, interval -60 minute) audit_time
        ,left(date_add(do.`audit_time`, interval -60 minute), 10) audit_date
        ,do.`succ_pick` pick_time
        ,do.`pack_time` pack_time
        ,do.`start_receipt ` handover_time
        ,date_add(do.`delivery_time`, interval -60 minute) delivery_time
        ,if(locate('DO',do.express_sn)>0 or substring(do.express_sn,1,3)='LBX' or do.is_presale=1 or t0.order_sn is not null or substring(do.express_name,1,3)='SPX', 0, 1) is_time -- do.is_presale=1 预售单不参与时效考核
        ,case when do.is_presale=1 then '预售单'
            when t0.order_sn is not null then '曾缺货订单'
            when locate('DO',do.express_sn)>0 then 'DO快递单号'
            when substring(do.express_sn,1,3)='LBX' then 'LBX快递单号'
            when substring(do.express_name,1,3)='SPX' then 'SPX快递单号'
            else '正常时效单'
            end as no_istime_type
        ,do.express_name
        , wd.id is_tiktok
        ,do.operator_id out_operator
        ,case do.`status`
            when 1000 then '取消发货'
            when 1002 then '等待激活'
            when 1003 then '预售订单'
            when 1005 then '待分仓'
            when 1007 then '已分仓'
            -- 货主审核订单前，已经知道系统缺货进行的状态提示。正常状态客户不应该审核通过这些订单
            when 1010 then '缺货'
            when 1015 then '已分仓(废弃)'
            when 1020 then '等待审核'
            when 1030 then '审核完成'
            -- 调用快递系统。多长时间提示? 金额是否有小数点？收件人，寄件人，电话是否有？ 号单是否匹配？  超过30分钟获取面单失败的数据
            when 1035 then '获取电子面单号失败'
            when 1040 then '获取电子面单号成功'
            -- 审核通过且获取面单成功后，系统分配库存失败
            when 2000 then '库存分配暂停'
            -- 审核通过且获取面单成功后，系统分配库存失败
            when 2005 then '分配库存失败'
            when 2010 then '分配库存成功'
            when 2015 then '分配预打包成功'
            when 2016 then '生成波次成功'
            when 2020 then '等待拣货'
            when 2030 then '拣货完成'
            when 2035 then '换单待打印'
            when 2040 then '打包完成'
            when 2050 then '开始交接'
            when 2060 then '发货完成'
            when 3010 then '配送中'
            when 3013 then '配送异常'
            when 3015 then '已拒收'
            when 3018 then '部分拒收'
            when 3020 then '已签收'
            when 3050 then '虚拟发货'
            else '其他'
            end as status
            ,case when do.`status` NOT IN ('1000','1010') and do.`platform_status` != 9 and do.prompt NOT in (1,2,3,4) and sl.name not in ('FFM-TH', 'Flash -Thailand') then 1
                else 0
            end as is_visible
            ,do.express_sn
            ,case when left(do.express_sn, 4)='TH24' then 'SPX'
            when left(do.express_sn, 2)='TH' then 'FLASH'
            when left(do.express_sn, 2)='66' then 'BEST'
            when left(do.express_sn, 2)='BS' then 'BEST'
            when left(do.express_sn, 2)='59' then 'DHL'
            when left(do.express_sn, 2)='90' then 'FLASH'
            when left(do.express_sn, 2)='FE' then 'FLASH'
            when left(do.express_sn, 2)='75' then 'J&T'
            when left(do.express_sn, 2)='61' then 'J&T'
            when left(do.express_sn, 2)='52' then 'J&T'
            when left(do.express_sn, 2)='KE' then 'Kerry'
            when left(do.express_sn, 2)='OD' then 'Kerry'
            when left(do.express_sn, 2)='ON' then 'Kerry'
            when left(do.express_sn, 2)='SH' then 'Kerry'
            when left(do.express_sn, 2)='TI' then 'Kerry'
            when left(do.express_sn, 2)='LE' then 'LEX'
            when left(do.express_sn, 2)='LB' then 'LAZ-JIT'
            when left(do.express_sn, 2)='JA' then 'Post'
            when left(do.express_sn, 2)='DO' then '自提'
            else '其他' end as   express_name_fixup
    FROM `wms_production`.`delivery_order` do 
    LEFT JOIN `wms_production`.`seller_platform_source` sps on do.`platform_source_id`=sps.`id`
    LEFT JOIN `wms_production`.`platform_source` ps on sps.`platform_source_id`=ps.`id` 
    LEFT JOIN `wms_production`.`seller` sl on do.`seller_id`=sl.`id`
    LEFT JOIN wms_production.warehouse w ON do.warehouse_id=w.id
    left join
    (
        select
            order_sn 
        from 
        `wms_production`.operation_log 
        where 1=1
            and status_after='1010'
            and `created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 90 day), '+07:00', '+08:00')
            -- and `created`>='2023-12-01'
    ) t0 on do.delivery_sn = t0.order_sn
    left join (select delivery_order_id,mark_id from wms_production.`delivery_order_mark_relation` where mark_id in (201, 200)) domr on domr.delivery_order_id = do.id
    left join (select id from wms_production.wordbook_detail where `wordbook_id` = 10 and (zh = 'TT3' or zh='TT')) wd on domr.mark_id=wd.id
    WHERE 1=1
        and do.`created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 90 day), '+07:00', '+08:00')
        -- and do.`created` >= '2023-12-01'
        -- AND do.`status` NOT IN ('1000','1010') -- 取消单
        -- AND do.`platform_status` != 9
        -- AND do.prompt NOT in (1,2,3,4) -- 剔除拦截
        -- and sl.name not in ('FFM-TH', 'Flash -Thailand') -- 剔除物料和资产

    UNION
    -- erp平台
    select
        'B2C' TYPE
        ,do.`delivery_sn`
        ,goods_num 
        ,warehouse_id
        ,w.name warehouse_name
        ,ps.`name` platform_source
        ,sl.`name` seller_name
        ,do.`seller_id`
        ,date_add(do.`created`, interval -60 minute) created_time
        ,left(date_add(do.`created`, interval -60 minute), 10) created_date
        ,date_add(do.`created`, interval -60 minute) audit_time
        ,left(date_add(do.`created`, interval -60 minute), 10) audit_date
        ,do.succ_pick + interval + 7 hour pick_time
        ,do.pack_time + interval + 7 hour pack_time
        ,do.delivery_time + interval -1 hour handover_time
        ,do.delivery_time + interval -1 hour delivery_time
        ,if(locate('DO',do.express_sn)>0 or substring(do.express_sn,1,3)='LBX' or t0.order_sn is not null, 0, 1) is_time -- do.is_presale=1 预售单不参与时效考核 is_time
        ,case -- when do.is_presale=1 then '预售单'
            when t0.order_sn is not null then '曾缺货订单'
            when locate('DO',do.express_sn)>0 then 'DO快递单号'
            when substring(do.express_sn,1,3)='LBX' then 'LBX快递单号'
            else '正常时效单'
            end as no_istime_type
        ,do.express_name
        ,p.obj_id is_tiktok
        ,ol.operation_id out_operator
        ,case do.status when 1007 then '已分仓'
            when 1010 then '缺货'
            when 2016 then '生成波次成功'
            when 2020 then '等待拣货'
            when 2030 then '拣货完成'
            when 2040 then '打包完成'
            when 2060 then '发货完成'
            when 3010 then '配送中'
            when 3020 then '已签收'
            when 3030 then '订单关闭'
            else do.status
        end as status
        ,case when do.`status` NOT IN ('1000','1010') AND w.name='BPL3-LIVESTREAM'and do.status <> '3030' and sl.name not in ('FFM-TH', 'Flash -Thailand') then 1
            else 0 end as is_visible
            ,do.express_sn
            ,case when left(do.express_sn, 4)='TH24' then 'SPX'
            when left(do.express_sn, 2)='TH' then 'FLASH'
            when left(do.express_sn, 2)='66' then 'BEST'
            when left(do.express_sn, 2)='BS' then 'BEST'
            when left(do.express_sn, 2)='59' then 'DHL'
            when left(do.express_sn, 2)='90' then 'FLASH'
            when left(do.express_sn, 2)='FE' then 'FLASH'
            when left(do.express_sn, 2)='75' then 'J&T'
            when left(do.express_sn, 2)='61' then 'J&T'
            when left(do.express_sn, 2)='52' then 'J&T'
            when left(do.express_sn, 2)='KE' then 'Kerry'
            when left(do.express_sn, 2)='OD' then 'Kerry'
            when left(do.express_sn, 2)='ON' then 'Kerry'
            when left(do.express_sn, 2)='SH' then 'Kerry'
            when left(do.express_sn, 2)='TI' then 'Kerry'
            when left(do.express_sn, 2)='LE' then 'LEX'
            when left(do.express_sn, 2)='LB' then 'LAZ-JIT'
            when left(do.express_sn, 2)='JA' then 'Post'
            when left(do.express_sn, 2)='DO' then '自提'
            else '其他' end as   express_name_fixup
    from
        `erp_wms_prod`.`delivery_order` do
    left join erp_wms_prod.platform_source ps on do.platform_source_id = ps.id
    LEFT JOIN erp_wms_prod.seller sl on do.`seller_id`=sl.`id`
    LEFT JOIN erp_wms_prod.warehouse w ON do.warehouse_id=w.id
    left join
    (
        select
            order_sn 
        from 
        `erp_wms_prod`.operation_log 
        where 1=1
            and status_after='1010'
            and `created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 90 day), '+07:00', '+08:00')
            -- and `created` >='2023-12-01'
    ) t0 on do.delivery_sn = t0.order_sn
    left join
    (select obj_id from erp_wms_prod.prompts where type = 1 and prompts in (13, 14) and warehouse_id = 312) p on p.obj_id = do.id
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
                    and status_after=2060 -- 拣货完成
                    and operation='delivery'
                    and ol.`created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 90 day), '+07:00', '+08:00')
                    -- and ol.`created` >='2023-12-01'
            ) t_in
            where rn = 1
    ) ol on do.delivery_sn = ol.order_sn
    WHERE 1=1
        and do.`created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 90 day), '+07:00', '+08:00')
)
select
    '货主是否开启自动审单' title
    ,od.created_date
    ,case when warehouse_name='AutoWarehouse'   then 'AGV'
                when warehouse_name='BPL-Return Warehouse'  then 'BPL-Return'
                when warehouse_name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
                when warehouse_name='BangsaoThong'  then 'BST'
                when warehouse_name IN ('BKK-WH-LAS2电商仓')    then 'LAS'
                when warehouse_name ='LCP Warehouse' then 'LCP' end warehouse_name
    ,od.seller_id
    ,od.seller_name
    ,svo.is_smart_name
    ,svo.store_name
    ,svo.platform_source
from
a od 
left join
(
    select 
        '开自动审核的'
        ,svo.is_smart
        ,if(svo.is_smart=1, 'open', 'close') is_smart_name
        ,svo.seller_id
        ,s.name seller_name
        ,sps.store_name
        ,ps.`name` platform_source
    from wms_production.smart_verify_order svo
    left join wms_production.smart_verify_order_relation svor on svo.id = svor.smart_verify_order_id
    LEFT JOIN `wms_production`.`seller_platform_source` sps on svor.`platform_source_id`=sps.`id`
    LEFT JOIN `wms_production`.`platform_source` ps on sps.`platform_source_id`=ps.`id`
    left join wms_production.seller s on s.id = svo.seller_id
  where 1=1
) svo on od.seller_id = svo.seller_id
where 1=1
    and od.TYPE='B2C'
    and od.created_date=CURRENT_DATE
    and case when warehouse_name='AutoWarehouse'   then 'AGV'
                when warehouse_name='BPL-Return Warehouse'  then 'BPL-Return'
                when warehouse_name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
                when warehouse_name='BangsaoThong'  then 'BST'
                when warehouse_name IN ('BKK-WH-LAS2电商仓')    then 'LAS'
                when warehouse_name ='LCP Warehouse' then 'LCP' end is not null
group by 1,2,3,4,5,6,7,8;


# 查询case
select 
        '开自动审核的'
        ,svo.is_smart
        ,if(svo.is_smart=1, 'open', 'close') is_smart_name
        ,svo.seller_id
        ,s.name seller_name
    from wms_production.smart_verify_order svo
    left join wms_production.seller s on s.id = svo.seller_id
where svo.seller_id='364';

select * from erp_wms_prod.seller s where id='364';

select * from wms_production.smart_verify_order where seller_id='364';