
/*=====================================================================+
表名称：  dwd_th_ffm_outbound_dayV2
功能描述：  泰国ffm出库明细数据表

需求来源：
编写人员: wangdongchen
设计日期：2024/8/22
修改日期: 
修改人员:    	
修改原因: 

-----------------------------------------------------------------------
---存在问题：
-----------------------------------------------------------------------
+===================================================================== */ 

-- drop table if exists dwm.dwd_th_ffm_outbound_dayV2;
-- create table dwm.dwd_th_ffm_outbound_dayV2 as
-- delete from dwm.dwd_th_ffm_outbound_dayV2 where created_date >= date_sub(date(now() + interval -1 hour),interval 90 day); -- 先删除数据
REPLACE into dwm.dwd_th_ffm_outbound_dayV2 -- 再插入数据
(TYPE
,delivery_sn
,goods_num
,warehouse_id
,warehouse_name
,platform_source
,seller_id
,seller_name
,Created_Time
,created_date
,audit_time
,audit_date
,pick_time
,pack_time
,handover_time
,delivery_time
,created_time_mod
,pack_deadline
,deadline
,ETL
,is_time
,no_istime_type
,audit_type
,express_name
,out_operator
,packtimetype
,outtimetype
,TimeoutNode
,status
,is_visible
,express_sn
,express_name_fixup
)
 
    SELECT
        TYPE
        ,delivery_sn
        ,goods_num
        ,warehouse_id
        ,warehouse_name
        ,platform_source
        ,seller_id
        ,seller_name
        ,Created_Time
        ,created_date
        ,audit_time
        ,audit_date
        ,pick_time
        ,pack_time
        ,handover_time
        ,delivery_time
        ,created_time_mod
        ,pack_deadline
        ,deadline
        ,ETL
        ,is_time
        ,no_istime_type
        ,audit_type
        ,express_name
        ,out_operator
        	/* -- 2B 2C 监控打包时间
        ,case when TYPE in ('B2C', 'B2B') and pack_time<pack_deadline then 1
            else 0
            end as 及时打包
        ,case when TYPE in ('B2C', 'B2B') and Created_Time is not null and (pack_time is not null OR pack_deadline< date_add(now(), interval -60 minute)) then 1
            else 0
            end as 应打包
            -- 2C监控发货时间，2B不监控发货时间
        ,case when TYPE='B2C' and delivery_time<deadline then 1
            else 0
            end as 及时发货
            -- 2C监控发货时间，2B不监控发货时间
        ,case when TYPE='B2C' and Created_Time is not null and (delivery_time is not null OR deadline< date_add(now(), interval -60 minute)) then 1
            else 0
            end as 应发货 */
        ,case when no_istime_type <> '正常时效单' then no_istime_type
            when pack_deadline > date_add(now(), interval -60 minute) then 'notlatest packtime'
            when pack_time is null or ( pack_time > pack_deadline) then 'nopack intime'
            when pack_time <= pack_deadline then 'pack intime'
            end as packtimetype

        ,case when 'B2C'=TYPE and no_istime_type <> '正常时效单' then no_istime_type
            when 'B2C'=TYPE and deadline > date_add(now(), interval -60 minute) then 'notlatest outboundtime'
            when 'B2C'=TYPE and delivery_time is null or ( delivery_time > deadline) then 'nooutbound intime'
            when 'B2C'=TYPE and delivery_time <= deadline then 'outbound intime'
            end as outtimetype
        ,case 
            when (audit_time > pack_deadline) or ( audit_time is null and pack_deadline< date_add(now(), interval -60 minute) ) then '审核超时' 
        	when (pick_time > pack_deadline) or ( pick_time is null and pack_deadline< date_add(now(), interval -60 minute) ) then '拣货超时' 
            when (pack_time > pack_deadline) or  ( pack_time is null and pack_deadline< date_add(now(), interval -60 minute) ) then  '打包超时'
            when 'B2C'=TYPE and ((handover_time > deadline) or ( handover_time is null and deadline< date_add(now(), interval -60 minute) )) then  '交接超时'
            when 'B2C'=TYPE and ((delivery_time > deadline) or ( delivery_time is null and deadline< date_add(now(), interval -60 minute) )) then  '揽收超时'
            else '未超时'
            end as TimeoutNode
         ,status
         ,is_visible
         ,express_sn
		 ,express_name_fixup
    FROM
    (
        SELECT 
            TYPE
            ,delivery_sn
            ,goods_num
            ,warehouse_id
            ,case when warehouse_name in ('AutoWarehouse', 'AutoWarehouse-人工仓')   then 'AGV'
                when warehouse_name='BPL-Return Warehouse'  then 'BPL-Return'
                when warehouse_name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
                when warehouse_name='BangsaoThong'  then 'BST'
                when warehouse_name IN ('BKK-WH-LAS2电商仓')    then 'LAS'
                when warehouse_name ='LCP Warehouse' then 'LCP' end warehouse_name
            ,CASE WHEN platform_source in('Shopee','Tik Tok','LAZADA')THEN platform_source ELSE 'Other' END platform_source
            ,seller_id
            ,seller_name
            ,created_time Created_Time
            ,created_date
            ,audit_time
            ,audit_date
            ,pick_time
            ,pack_time
            ,handover_time
            ,delivery_time
            ,created_time_mod
            ,CASE WHEN TYPE ='B2B' THEN concat(date2,substr(created_time_mod,11,9))
            	WHEN platform_source='Shopee' AND substr(created_time_mod,12,2)<16 THEN concat(LEFT(created_time_mod,10),' 23:59:59')
                WHEN platform_source='Shopee' AND substr(created_time_mod,12,2)>=16 THEN concat(date1,' 23:59:59')
                WHEN platform_source='Tik Tok' AND substr(created_time_mod,12,2)<18 THEN concat(LEFT(created_time_mod,10),' 23:59:59')
                WHEN platform_source='Tik Tok' AND substr(created_time_mod,12,2)>=18 THEN concat(date1,' 23:59:59')
                -- when platform_source='LAZADA'  then concat(date1,substr(created_time_mod,11,9))
                ELSE concat(date1,substr(created_time_mod,11,9)) END pack_deadline

            ,CASE WHEN TYPE ='B2B' THEN concat(date2,substr(created_time_mod,11,9))
                WHEN platform_source='Shopee' AND substr(created_time_mod,12,2)<16 THEN concat(LEFT(created_time_mod,10),' 23:59:59')
                WHEN platform_source='Shopee' AND substr(created_time_mod,12,2)>=16 THEN concat(date1,' 23:59:59')
                WHEN platform_source='Tik Tok' AND substr(created_time_mod,12,2)<18 THEN concat(LEFT(created_time_mod,10),' 23:59:59')
                WHEN platform_source='Tik Tok' AND substr(created_time_mod,12,2)>=18 THEN concat(date1,' 23:59:59')
                WHEN platform_source='LAZADA' THEN concat(date2,substr(created_time_mod,11,9))
                ELSE concat(date1,substr(created_time_mod,11,9)) END deadline
            ,date_add(now(),interval -60 minute) ETL
            ,is_time
            ,no_istime_type
            ,audit_type
            ,express_name
            ,out_operator
             ,status
			,is_visible
            ,express_sn
			,express_name_fixup
        FROM
        (
            select
                do.TYPE
                ,do.delivery_sn
                ,do.goods_num 
                ,do.warehouse_id
                ,do.warehouse_name
                ,if(do.is_tiktok is not null, 'Tik Tok',do.platform_source) platform_source
                ,do.seller_name
                ,do.seller_id
                ,do.created_time
                ,do.created_date
                ,do.audit_time
                ,do.audit_date
                ,do.pick_time
                ,do.pack_time
                ,do.handover_time
                ,do.delivery_time
                ,do.is_time
                ,do.no_istime_type
                ,do.audit_type
                ,do.express_name
                ,do.out_operator
                ,calendar.created
                ,calendar.if_day_off
                ,calendar.created_mod
                ,calendar.date1
                ,calendar.date2
                ,calendar.date3
                ,calendar.date4
                ,case when calendar.if_day_off='是' then concat(calendar.created_mod,' 00:00:00') else concat(calendar.created_mod,substr(do.created_time,11,9)) end created_time_mod 
                ,status
				,is_visible
                ,express_sn
				,express_name_fixup
                
            FROM
            (
                -- wms平台
                SELECT 
                    'B2C' TYPE
                    ,if(locate('-1', do.delivery_sn)>0 ,left(do.delivery_sn, 13), do.delivery_sn) delivery_sn
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
                    ,if(locate('DO',do.express_sn)>0 or substring(do.express_sn,1,3)='LBX' or do.is_presale=1 or t0.order_sn is not null or (w.name='BPL-Return Warehouse' and substring(do.express_name,1,3)='SPX'), 0, 1) is_time -- do.is_presale=1 预售单不参与时效考核
                    ,case when do.is_presale=1 then '预售单'
                        when t0.order_sn is not null then '曾缺货订单'
                        when locate('DO',do.express_sn)>0 then 'DO快递单号'
                        when substring(do.express_sn,1,3)='LBX' then 'LBX快递单号'
                        when (w.name='BPL-Return Warehouse' and substring(do.express_name,1,3)='SPX') then 'SPX快递单号'
                        else '正常时效单'
                        end as no_istime_type
                    ,case when do.is_presale=1 then '预售单'
                        when t0.order_sn is not null then '曾缺货订单'
                        end as audit_type
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
                     ,case when do.`status` NOT IN ('1000','1010') and do.`platform_status` != 9 and do.prompt NOT in (1,2,3,4) and sl.name not in ('FFM-TH', 'Flash -Thailand') and locate('-1', do.delivery_sn)=0 then 1
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
                        -- and `created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 90 day), '+07:00', '+08:00')
                        and `created`>='2023-06-01'
                ) t0 on do.delivery_sn = t0.order_sn
                left join (select delivery_order_id,mark_id from wms_production.`delivery_order_mark_relation` where mark_id in (201, 200)) domr on domr.delivery_order_id = do.id
                left join (select id from wms_production.wordbook_detail where `wordbook_id` = 10 and (zh = 'TT3' or zh='TT')) wd on domr.mark_id=wd.id
                WHERE 1=1
                    -- and do.`created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 90 day), '+07:00', '+08:00')
                    and do.`created` >= '2023-06-01'
                    -- AND do.`status` NOT IN ('1000','1010') -- 取消单
                    -- AND do.`platform_status` != 9
                    -- AND do.prompt NOT in (1,2,3,4) -- 剔除拦截
                    -- and sl.name not in ('FFM-TH', 'Flash -Thailand') -- 剔除物料和资产

                UNION
                -- wms平台
                SELECT 
                    'B2B' TYPE
                    ,return_warehouse_sn
                    ,total_goods_num
                    ,do.warehouse_id
                    ,w.name warehouse_name
                    ,'B2B'platform_source
                    ,sl.name seller_name
                    ,do.`seller_id` seller_id
                    ,date_add(do.`created`, interval -60 minute) created_time
                    ,left(date_add(do.`created`, interval -60 minute), 10) created_date
                    ,date_add(do.`verify_time`, interval -60 minute) audit_time
                    ,left(date_add(do.`verify_time`, interval -60 minute), 10) audit_date
                    ,do.picking_end_time
                    ,do.pack_time + interval -1 hour pack_time
                    ,date_add(do.`out_warehouse_time`, interval -60 minute) handover_time
                    ,date_add(do.`out_warehouse_time`, interval -60 minute) delivery_time
                    ,if(locate('DO',do.express_sn)>0 or substring(do.express_sn,1,3)='LBX', 0, 1) is_time
                    ,case 
                        when locate('DO',do.express_sn)>0 then 'DO快递单号'
                        when substring(do.express_sn,1,3)='LBX' then 'LBX快递单号'
                        else '正常时效单'
                        end as no_istime_type
                    ,null as audit_type
                    ,ulc.name
                    ,null is_tiktok
                    ,null out_operator
                    ,case do.status
                         when 1000 then '已作废'
                         when 1005 then '草稿'
                         when 1010 then '待审核'
                         when 1015 then '审核不通过'
                         when 1020 then '已审核'
                         when 2000 then '库存分配暂停'
                         when 2005 then '分配库存失败'
                         when 2010 then '分配库存成功'
                         when 2020 then '已确认'
                         when 2025 then '拣货中'
                         when 2028 then '拣货完成'
                         when 2030 then '已打包'
                         when 2040 then '出库进行中'
                         when 3010 then '已出库'
                         when 3020 then '已完成'
                         else do.status
                  	 end as status
                     ,case when prompt ='0' AND status>='1020' and sl.name not in ('FFM-TH', 'Flash -Thailand') then 1
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
                from  wms_production.return_warehouse do 
                LEFT JOIN wms_production.warehouse w ON do.warehouse_id=w.id
                LEFT JOIN `wms_production`.`seller` sl on do.`seller_id`=sl.`id`
                left join wms_production.logistic_company ul on do.logistic_company_id = ul.id
                left join wms_production.usable_logistic_company ulc on ul.usable_logistic_company_id  = ulc.id
                WHERE 1=1
                    -- and prompt ='0' 
                    -- AND status>='1020'
                    -- and do.`created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 90 day), '+07:00', '+08:00')
                    and do.`created` >= '2023-06-01'
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
                    ,case when  t0.order_sn is not null then '曾缺货订单'
                    	end as audit_type
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
                        -- and `created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 90 day), '+07:00', '+08:00')
                        and `created` >='2023-06-01'
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
                              -- and ol.`created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 90 day), '+07:00', '+08:00')
                              and ol.`created` >='2023-06-01'
                      ) t_in
                      where rn = 1
                ) ol on do.delivery_sn = ol.order_sn
                WHERE 1=1
                	-- and do.`created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 90 day), '+07:00', '+08:00')
                    and do.`created` >='2023-06-01'
                    -- AND do.`status` NOT IN ('1000','1010') -- 取消单
                    -- AND w.name='BPL3-LIVESTREAM'
                    -- and do.status <> '3030'
                    -- and sl.name not in ('FFM-TH', 'Flash -Thailand') -- 剔除物料和资产

                UNION
                -- erp平台
                SELECT 
                    'B2B' TYPE
                    ,outbound_sn
                    ,t0.goods_number
                    ,warehouse_id
                    ,w.name warehouse_name
                    ,'B2B'platform_source
                    ,s.name seller_name
                    ,do.seller_id
                    ,date_add(do.`created`, interval -60 minute) created_time
                    ,left(date_add(do.`created`, interval -60 minute), 10) created_date
                    ,date_add(do.`created`, interval -60 minute) audit_time
                    ,left(date_add(do.confirm_time , interval + 7 hour), 10) audit_date
                    ,do.confirm_time + interval + 7 hour    pick_time
                    ,do.confirm_time + interval + 7 hour    pack_time
                    ,date_add(do.confirm_time , interval + 7 hour) handover_time
                    ,date_add(do.confirm_time , interval + 7 hour) delivery_time
                    ,1 is_time
                    ,null no_istime_type
                    ,null audit_type
                    ,or1.carrier
                    ,null is_tiktok
                    ,null out_operator
                    ,case do.status when 0 then '取消'
                        when 1 then '已创建'
                        when 2 then '分配库存成功'
                        when 3 then '分配库存失败'
                        when 4 then '已出库'
                        when 5 then '已完成'
                        else do.status
                    end as status
                    ,case when do.status > '0' AND w.name='BPL3-LIVESTREAM' and s.name not in ('FFM-TH', 'Flash -Thailand') then 1 
                    else 0 end as is_visible
                    ,null express_sn
					, null express_name_fixup

                from  erp_wms_prod.outbound_order do 
                left join 
                (

                    select
                      outbound_id
                      ,sum(in_num) goods_number
                	from
                	erp_wms_prod.outbound_order_detail
                	group by 1
                ) t0 on do.id = t0.outbound_id
                LEFT JOIN erp_wms_prod.warehouse w ON do.warehouse_id=w.id
                left join `erp_wms_prod`.`seller` s on do.seller_id = s.id
                left join erp_wms_prod.outbound_register or1 on do.id = or1.outbound_id
                WHERE 1=1 
                	-- and do.status > '0'
                    AND w.name='BPL3-LIVESTREAM'
                    -- and do.`created` >= convert_tz(date_sub(date(now() + interval -1 hour),interval 90 day), '+07:00', '+08:00')
                    and do.`created` >='2023-06-01'
                    -- and s.name not in ('FFM-TH', 'Flash -Thailand') -- 剔除物料和资产
            ) do 
            left join 
            -- 日历调整// created_mod 是节假日顺延后首日,date1是节假日顺延后第二天
            dwm.dim_th_default_timeV2 calendar on calendar.created=do.created_date
            where created_date>='2023-09-04'
        )do
    )
    where warehouse_name in ('AGV', 'BPL-Return', 'BPL3', 'BST', 'LAS')