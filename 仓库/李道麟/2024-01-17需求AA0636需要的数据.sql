select
t_out.month
,t_out.发货单量
,t_in.入库单量
from
(
    select
        left(日期, 7) month
        ,sum(B2C出库单量) 发货单量
    from
    (
        SELECT 
            LEFT(delivery_time,10) 日期
            ,warehouse_name
            ,seller_name
            ,sum(if(audit_time is not null and TYPE='B2C', goods_num, 0)) B2C商品数量
            ,COUNT(if(TYPE='B2C', delivery_sn, null)) B2C出库单量
        FROM dwm.dwd_th_ffm_outbound_dayV2
        where 1=1
            and is_visible=1
            and warehouse_name='BST'
            and seller_name='AA0636(Giikin)'
            and LEFT(delivery_time,10) BETWEEN '2024-01-01' AND '2024-12-31'
        GROUP BY 1,2,3
    )
    where 1=1
    group by 1
) t_out

left join
(
    select
        left(日期, 7) month
        ,sum(采购订单入库单量)+sum(销退订单入库单量) 入库单量
    from
    (
        select 
            LEFT(complete_time,10) 日期
            ,仓库名称 仓库
            ,seller_name
            ,'入库单量' 指标
            ,count(if(单据='采购订单', notice_number, null)) 采购订单入库单量
            ,sum(if(单据='采购订单', in_num, 0)) 采购订单入库件量
            ,count(if(单据='销退订单', notice_number, null)) 销退订单入库单量
        FROM dwm.dwd_th_ffm_arrivalnotice_dayV2
        WHERE 1=1
            and seller_name='AA0636(Giikin)'
            and left(complete_time,10) BETWEEN '2024-01-01' AND '2024-12-31'
            and 仓库名称='BST'
        group by 1,2,3,4
    )
    where 1=1
    group by 1
) t_in on t_out.month = t_in.month
order by t_out.month

-- 退件仓
select
    t_out.month
    ,t_out.发货单量
    ,t_in.销退单量
from
(
    select
        left(日期, 7) month
        ,sum(B2C出库单量) 发货单量
    from
    (
        SELECT 
            LEFT(delivery_time,10) 日期
            ,warehouse_name
            ,seller_name
            ,sum(if(audit_time is not null and TYPE='B2C', goods_num, 0)) B2C商品数量
            ,COUNT(if(TYPE='B2C', delivery_sn, null)) B2C出库单量
        FROM dwm.dwd_th_ffm_outbound_dayV2
        where 1=1
            and is_visible=1
            and warehouse_name='BPL-Return'
            and seller_name='AA0636(Giikin)'
            and LEFT(delivery_time,10) BETWEEN '2024-01-01' AND '2024-12-31'
        GROUP BY 1,2,3
    )
    where 1=1
    group by 1
) t_out

left join
(
    select
        left(日期, 7) month
        ,sum(销退订单入库单量) 销退单量
    from
    (
        select 
            LEFT(complete_time,10) 日期
            ,仓库名称 仓库
            ,seller_name
            ,'入库单量' 指标
            ,count(if(单据='采购订单', notice_number, null)) 采购订单入库单量
            ,sum(if(单据='采购订单', in_num, 0)) 采购订单入库件量
            ,count(if(单据='销退订单', notice_number, null)) 销退订单入库单量
        FROM dwm.dwd_th_ffm_arrivalnotice_dayV2
        WHERE 1=1
            and seller_name='AA0636(Giikin)'
            and left(complete_time,10) BETWEEN '2024-01-01' AND '2024-12-31'
            and 仓库名称='BPL-Return'
        group by 1,2,3,4
    )
    where 1=1
    group by 1
) t_in on t_out.month = t_in.month
order by t_out.month