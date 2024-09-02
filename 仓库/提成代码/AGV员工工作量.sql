with a as
(
    select 
        date
        , bau.work_no work_no
        , user_name
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
        WHERE 1=1
            and rwb.group_id = 1180 -- AGV仓
            AND rwb.del_flag = 0 -- 正常未删除
            AND rwb.status = 'FINISH'-- 已完成
            and  action_type = 'PUTAWAY'
            and  rwb.biz_type IN ( 'INBOUND', 'RESTORE' )
            AND left(rwb.gmt_modified,7)=left(date_sub(date_add(now(),interval -60 minute),interval 1 month),7) -- @todo
            -- and bau.work_no='420015'
        GROUP BY 1,2,3


        -- 收货
        union all
        SELECT
            left(ircd.create_time,10)
            ,scan_person user_id
            --    ,COUNT(DISTINCT ( ircd.item_id )) inboundSKU
            ,'收货' type
            ,sum(scan_qty) inboundNum
        FROM was.inb_receive_container_detail  ircd
        -- LEFT JOIN base_authority_user bau
        -- ON ircd.scan_person = bau.user_id
        WHERE 1=1
            and ircd.group_id = 1180
            AND left(ircd.create_time,7)=left(date_sub(date_add(now(),interval -60 minute),interval 1 month),7) -- @todo
            AND ircd.is_deleted = 0
        GROUP BY 1,2


        -- 拣选
        union all
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
        WHERE 1=1
            and oobt.del_flag = 0 -- 未删除
            AND oobt.status = 1
            AND oobt.group_id = 1180 -- AGV仓
            AND oobt.type in (1,5)
            and left(oobt.gmt_modified,7)=left(date_sub(date_add(now(),interval -60 minute),interval 1 month),7) -- @todo
        GROUP BY 1,2

        -- 打包
        union all
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
        WHERE 1=1
            and wsg.status = 'FINISH_PACK'
            AND wsg.del_flag = 0
            AND wsg.type = 1
            # AND wsg.group_id = 1180
            AND left(wsg.operation_time,7)=left(date_sub(date_add(now(),interval -60 minute),interval 1 month),7) -- @todo
        GROUP BY 1,2
    )a
    LEFT JOIN was.base_authority_user bau ON bau.user_id = a.user_id
)

select 
    left(date,7)
    ,work_no
    ,user_name
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
where 1=1
    and do.warehouse_id='36' 
    and left (do.delivery_time,7)=left(date_sub(date_add(now(),interval -60 minute),interval 1 month),7) -- @todo
group by 1