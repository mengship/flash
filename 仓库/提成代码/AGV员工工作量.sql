with a as
(
    select 
        date
        , bau.work_no work_no
        , a.type
        , real_qty 
    from
    (
        -- 上架
        SELECT
            left(rwb.gmt_modified,10) date
            ,rwbd.user_id user_id
            -- ,case when action_type = 'PUTAWAY' and rwb.biz_type IN ( 'RESTORE' ) then '拦截上架'
            --     when action_type = 'PUTAWAY' and rwb.biz_type IN ( 'INBOUND' ) and rwb.source_order_type = '4' then '销退上架'
            --     when action_type = 'PUTAWAY' and rwb.biz_type IN ( 'REPLENISH' ) then '补货上架'
            --     when action_type = 'PUTDOWN' and rwb.biz_type IN ( 'REPLENISH') then '补货下架'
            --     when action_type = 'PUTDOWN' and rwb.biz_type IN ( 'TOC_PICK_REPICK' ) then '复检下架'
            --     when action_type = 'PUTDOWN' and rwb.biz_type IN ( 'TRANSFER' ) then '移库下架'
            --     when action_type = 'PUTAWAY' and rwb.biz_type IN ( 'TRANSFER' ) then '移库上架'
            -- end type
            ,'上架' type
            -- ,count( rwbd.item_id ) Sku_num
            ,sum(real_qty)  real_qty
        FROM was.robot_work_bill rwb
        left join was.robot_work_bill_detail rwbd on work_bill_id=rwb.id
        LEFT JOIN was.base_authority_user bau ON rwbd.user_id = bau.user_id
        LEFT JOIN wms_production.seller sl on rwbd.owner_id=sl.id
        WHERE 1=1
            and rwb.group_id = 1180 -- AGV仓
            AND rwb.del_flag = 0 -- 正常未删除
            AND rwb.status = 'FINISH'-- 已完成
            and  action_type = 'PUTAWAY'
            and  rwb.biz_type IN ( 'INBOUND', 'RESTORE' )
            and sl.name not in ('FFM-TH') -- 剔除物料
            AND left(rwb.gmt_modified,7)=left(date_sub(date_add(now(),interval -60 minute),interval 1 month),7) -- @todo
            -- and bau.work_no='420015'
        GROUP BY 1,2,3

        -- 收货
        union all
        SELECT
            left(ircd.create_time,10)
            ,ircd.scan_person user_id
            --    ,COUNT(DISTINCT ( ircd.item_id )) inboundSKU
            ,'收货' type
            ,sum(ircd.scan_qty) inboundNum
        FROM was.inb_receive_container irc
        join was.inb_receive_container_detail  ircd on irc.inb_receive_container_id = ircd.inb_receive_container_id
        LEFT JOIN wms_production.seller sl on irc.owner_id=sl.id
        -- LEFT JOIN base_authority_user bau
        -- ON ircd.scan_person = bau.user_id
        WHERE 1=1
            and ircd.group_id = 1180
            AND left(ircd.create_time,7)=left(date_sub(date_add(now(),interval -60 minute),interval 1 month),7) -- @todo
            AND ircd.is_deleted = 0
            and sl.name not in ('FFM-TH') -- 剔除物料
        GROUP BY 1,2

        -- 拣选
        union all -- agv 仓拣货
        SELECT
            left(oobt.gmt_modified,10) date
            ,oobt.operator user_id
            ,'拣选'type
            -- ,count(DISTINCT ( oobt.order_code )) pickOrder
            ,sum(case when  wobo.type in(1)  then actual_num end)pickNum -- 2,3是ToB
            -- ,sum(actual_num) pickNum
        FROM was.oub_out_bound_task oobt
        -- LEFT JOIN base_authority_user bau ON oobt.operator = bau.user_id
        LEFT JOIN was.was_out_bound_order wobo  on  wobo.order_code = CONVERT(oobt.order_code using gbk) and oobt.group_id = wobo.group_id
        LEFT JOIN wms_production.seller sl on oobt.owner_id=sl.id
        WHERE 1=1
            and oobt.del_flag = 0 -- 未删除
            AND oobt.status = 1
            AND oobt.group_id = 1180 -- AGV仓
            AND oobt.type in (1,5)
            and sl.name not in ('FFM-TH') -- 剔除物料
            and left(oobt.gmt_modified,7)=left(date_sub(date_add(now(),interval -60 minute),interval 1 month),7) -- @todo
        GROUP BY 1,2

        -- 打包
        union all -- agv 打包
        SELECT
            left(wsg.operation_time,10)date
            ,wsg.creator user_id
            ,'打包' type
            -- ,count(DISTINCT ( wsg.relevance_code )) packOrder
            -- ,sum(item_num) packNum
            ,sum(case when wobo.type in(1) then item_num END)packNum 
        FROM was.was_status_group wsg
        -- LEFT JOIN was.base_authority_user bau ON wsg.creator = bau.user_id
        left join was.was_package wp on wsg.relevance_code = wp.order_code
        left join was.was_out_bound_order wobo  on  wobo.order_code = wp.order_code and wp.group_id = wobo.group_id
        LEFT JOIN wms_production.seller sl on wp.owner_id=sl.id
        WHERE 1=1
            and wsg.status = 'FINISH_PACK'
            AND wsg.del_flag = 0
            AND wsg.type = 1
            and sl.name not in ('FFM-TH') -- 剔除物料
            # AND wsg.group_id = 1180
            AND left(wsg.operation_time,7) = left(date_sub(date_add(now(),interval -60 minute),interval 1 month),7) -- @todo
        GROUP BY 1,2

        



    )a
    LEFT JOIN was.base_authority_user bau ON bau.user_id = a.user_id


    union all -- agv 人工仓 上架
    select
            date(shelf_on_end)
            ,m.job_number
            ,'上架'
            ,sum(number)
        from wms_production.shelf_on_order so
        left join wms_production.shelf_on_order_goods soog on so.id=soog.shelf_on_order_id
        left join wms_production.seller s on so.seller_id =s.id 
        left join wms_production.warehouse w on so.warehouse_id=w.id
        left join wms_production.`member` m on so.shelf_on_man_id = m.id
        where w.name='AutoWarehouse-人工仓'
        and left(shelf_on_end, 7)=left(date_sub(date_add(now(),interval -60 minute),interval 1 month),7) -- @todo
        group by 1,2,3
    
    union all -- agv 人工仓 收货
    select
    date(complete_time) complete_date
    ,m.job_number
    ,'收货'
    ,sum(goods_in_num)
    from
    wms_production.arrival_notice an
    left join wms_production.seller s on an.seller_id =s.id 
    left join wms_production.warehouse w on an.warehouse_id=w.id
    left join wms_production.`member` m on an.complete_id = m.id
    where w.name='AutoWarehouse-人工仓'
    and left(complete_time, 7)=left(date_sub(date_add(now(),interval -60 minute),interval 1 month),7) -- @todo
    group by 1,2,3

    

    union all -- agv 人工仓拣货
        select
            date(完成时间) 完成日期
            -- ,虚拟仓
            ,员工ID
            ,'拣选' type
            ,sum(商品数量)
        from
            dwm.dwd_th_ffm_picking_detail1
        where left(完成时间, 7)=left(date_sub(date_add(now(),interval -60 minute),interval 1 month),7)
        and 虚拟仓='AutoWarehouse-人工仓'
        group by 1,2,3

    union all -- agv 人工仓 打包

        select
            t_out.now_date
            ,t_out.creator_id
            ,t_out.title_name
            ,sum(t_out.packnum)
        from
        (
            select
                '打包' title_name
                ,case when w.name in ('AutoWarehouse', 'AutoWarehouse-人工仓')    then 'AGV'
                    when w.name='BPL-Return Warehouse'  then 'BPL-Return'
                    when w.name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
                    when w.name='BangsaoThong'  then 'BST'
                    when w.name IN ('BKK-WH-LAS2电商仓')    then 'LAS'
                    when w.name ='LCP Warehouse' then 'LCP' end warehouse_name
                ,db.creator_id
                ,db.real_name
                ,date(do.pack_time) now_date
                ,hour(do.pack_time) now_hour
                ,sum(do.goods_num) packnum
            from
            (
                select
                    db.delivery_order_id,
                    m.job_number pack_id,
                    db.warehouse_id,
                    m.job_number creator_id,
                    m.real_name,
                    count(1) as boxNum
                from
                    wms_production.delivery_box db
                    LEFT JOIN `wms_production`.`member` m on db.creator_id = m.id
                    where 1=1 
                    -- date(db.created)>= 
                    -- date(db.created + interval -1 hour) >= CURRENT_DATE
                    and date(db.created + interval -1 hour)>='2024-09-01'
                group by
                    delivery_order_id,
                    db.warehouse_id,
                    db.pack_id,
                    db.creator_id
            ) db
            left join `wms_production`.`delivery_order` do on do.id = db.delivery_order_id
            left join `wms_production`.`warehouse` w on db.warehouse_id = w.id
            where left(do.pack_time, 7)=left(date_sub(date_add(now(),interval -60 minute),interval 1 month),7) -- @todo
                and w.name = 'AutoWarehouse-人工仓'
            group by 1,2,3,4,5,6

        )t_out where 1=1
        group by 1,2,3
)

select 
    left(date,7)
    ,work_no
    ,type
    ,sum(real_qty)
from a
where 1=1
    and left(date,7)=left(date_sub(date_add(now(),interval -60 minute),interval 1 month),7)  -- @todo
group by 1,2,3,4;

# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

# outbound2
select 
    job_number
    ,count (distinct delivery_sn)
from wms_production.delivery_order do
left join wms_production.delivery_receipt_order_delivery_ref dror on do.id=dror.delivery_order_id
left join wms_production.delivery_receipt_order dro on dro.id=dror.delivery_receipt_order_id
LEFT JOIN `wms_production`.`member` mb on do.operator_id=mb.`id`
LEFT JOIN wms_production.seller sl on do.seller_id=sl.id
where 1=1
    and do.warehouse_id in('36', '78')
    and do.`status` NOT IN ('1000','1010') 
    and do.`platform_status` != 9 
    and do.prompt NOT in (1,2,3,4) 
    and sl.name not in ('FFM-TH', 'Flash -Thailand')
    and left(do.delivery_time,7)=left(date_sub(date_add(now(),interval -60 minute),interval 1 month),7) -- @todo
group by 1