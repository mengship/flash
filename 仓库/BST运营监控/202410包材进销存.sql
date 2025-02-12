-- 包材进销存 后期上到帆软report上
select
    '包材进销存'
    ,thisfir.日期
    ,thisfir.仓库
    ,thisfir.商品条码
    ,thisfir.goods_name
    ,lastfri.实物库存 期初库存
    ,lastfri.可用库存 期初可用库存
    ,thisfir.实物库存 期末库存
    ,thisfir.可用库存 期初可用库存
    ,inbound.in_num 入库
    ,outbound1.out_num 包材出库
    ,outbound2.out_num 调拨出库
    ,usedetail.数量 收费包材
from
(
    -- 本周五的数据
    select 
        sgss.date '日期',
        s.name '货主',
        case when w.name='AutoWarehouse'   then 'AGV'
                when w.name='BPL-Return Warehouse'  then 'BPL-Return'
                when w.name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
                when w.name='BangsaoThong'  then 'BST'
                when w.name IN ('BKK-WH-LAS2电商仓')    then 'LAS'
                when w.name ='LCP Warehouse' then 'LCP' end '仓库',
        sgss.bar_code '商品条码',
        sgss.goods_name ,
        sum(sgss.total_inventory+sgss.scrap_inventory) '实物库存',
        sum(sgss.total_inventory)  '正品实物库存',
        sum(sgss.scrap_inventory)  '残品实物库存',
        sum(sgss.inventory) 可用库存
    from wms_production.seller_goods_stock_snapshot sgss
    left join wms_production.seller s on s.id = sgss.seller_id
    left join wms_production.warehouse w on w.id = sgss.warehouse_id
    where 1=1
        -- and sgss.`date` = subdate(curdate(),if(date_format(curdate(),'%w')=0,7,date_format(curdate(),'%w'))-5) -- 本周五
        and sgss.`date` = '2024-10-09'
        and s.name='FFM-TH'
    group by 1,2,3,4
) thisfir
left join
(
    -- 上周五的数据
    select 
        sgss.date '日期',
        s.name '货主',
        case when w.name='AutoWarehouse'   then 'AGV'
                when w.name='BPL-Return Warehouse'  then 'BPL-Return'
                when w.name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
                when w.name='BangsaoThong'  then 'BST'
                when w.name IN ('BKK-WH-LAS2电商仓')    then 'LAS'
                when w.name ='LCP Warehouse' then 'LCP' end '仓库',
        sgss.bar_code '商品条码',
        sgss.goods_name ,
        sum(sgss.total_inventory+sgss.scrap_inventory) '实物库存',
        sum(sgss.total_inventory)  '正品实物库存',
        sum(sgss.scrap_inventory)  '残品实物库存',
        sum(sgss.inventory) 可用库存
    from wms_production.seller_goods_stock_snapshot sgss
    left join wms_production.seller s on s.id = sgss.seller_id
    left join wms_production.warehouse w on w.id = sgss.warehouse_id
    where 1=1
        and sgss.`date` = SUBDATE(subdate(curdate(),date_format(curdate(),'%w') - 1),3) -- 上周五
        and s.name='FFM-TH'
    group by 1,2,3,4
) lastfri on lastfri.仓库 = thisfir.仓库 and lastfri.商品条码 = thisfir.商品条码
left join
(
    -- 入库
    select
        '入库'
        ,仓库名称 仓库
        ,complete_date
        ,bar_code 商品条码
        ,sum(in_num) in_num
    from
    (
        SELECT 
            notice_number
            ,"采购订单" 单据
            ,warehouse_id
            ,case when w.name='AutoWarehouse' then 'AGV'
                when w.name='BPL-Return Warehouse' then 'BPL-Return'
                when w.name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
                when w.name='BangsaoThong' then 'BST'
                when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS' # ,'PMD-WH','BKK-WH-Ecommerce','BKK-WH-LAS物料仓'
                when w.name='LCP Warehouse' then 'LCP' end 仓库名称
            ,reg_time - interval 1 hour reg_time
            ,left(reg_time - interval 1 hour,10) reg_date
            ,complete_time
            ,date(complete_time) complete_date
            ,if(w.name='AutoWarehouse', irb.finish_date, shelf_complete_time) shelf_complete_time
            ,ang.in_num
            ,ang.number
            ,sg.bar_code
            ,case an.`from_order_type`
                    when 1 then '采购入库'
                    when 2 then '调拨入库'
                    when 3 then '退货入库'
                    when 4 then '其他入库'
                    else an.`from_order_type`
            end 入库类型
        FROM wms_production.arrival_notice an
        left join wms_production.arrival_notice_goods ang on an.id = ang.arrival_notice_id
        left join wms_production.seller_goods sg on sg.id =ang.seller_goods_id 
        left join (select receive_external_no,finish_date from was.inb_receive_bill where is_deleted=0 and create_time >= convert_tz(date_sub(date(now() + interval -1 hour),interval 130 day), '+07:00', '+08:00') )irb on an.notice_number = irb.receive_external_no
        left join wms_production.warehouse w ON an.warehouse_id=w.id
        left join wms_production.seller s on s.id = an.seller_id
            WHERE an.reg_time IS NOT NULL 
            AND an.status>='30'
            AND an.complete_time >= SUBDATE(subdate(curdate(),date_format(curdate(),'%w') - 1),2) -- 上周五
            and an.complete_time < subdate(curdate(),if(date_format(curdate(),'%w')=0,7,date_format(curdate(),'%w'))-5)
            and s.name='FFM-TH'
    ) group by 1,2,3,4
) inbound on inbound.仓库 = thisfir.仓库 and inbound.商品条码 = thisfir.商品条码

left join
(
    -- 包材出库
    select
        '包材出库'
        ,仓库名称 仓库
        ,verify_date
        ,bar_code 商品条码
        ,sum(out_num) out_num
    from
    (
        select
            rw.return_warehouse_sn
            ,case when w.name='AutoWarehouse' then 'AGV'
                when w.name='BPL-Return Warehouse' then 'BPL-Return'
                when w.name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
                when w.name='BangsaoThong' then 'BST'
                when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS' # ,'PMD-WH','BKK-WH-Ecommerce','BKK-WH-LAS物料仓'
                when w.name='LCP Warehouse' then 'LCP' end 仓库名称
            ,rw.out_warehouse_time
            ,rwg.out_num
            ,sg.bar_code
            ,rw.verify_time 
            ,date(rw.verify_time ) verify_date
        from
        wms_production.return_warehouse rw
        left join wms_production.return_warehouse_goods rwg on rw.id = rwg.return_warehouse_id
        left join wms_production.seller_goods sg on sg.id =rwg.seller_goods_id 
        left join wms_production.seller s on rw.seller_id = s.id
        left join wms_production.warehouse w ON rw.`warehouse_id` = w.`id`
        where 1=1
            and rw.verify_time >= SUBDATE(subdate(curdate(),date_format(curdate(),'%w') - 1),2) -- 上周五
            and rw.verify_time < subdate(curdate(),if(date_format(curdate(),'%w')=0,7,date_format(curdate(),'%w'))-6)
            and rw.type=3
            and s.name='FFM-TH'
    )
    group by 1,2,3,4
    
) outbound1 on outbound1.仓库 = thisfir.仓库 and outbound1.商品条码 = thisfir.商品条码
left join
(
    -- 调拨出库
    select
        '调拨出库'
        ,仓库名称 仓库
        ,bar_code 商品条码
        ,sum(out_num) out_num
    from
    (
        select
            rw.return_warehouse_sn
            ,case when w.name='AutoWarehouse' then 'AGV'
                when w.name='BPL-Return Warehouse' then 'BPL-Return'
                when w.name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
                when w.name='BangsaoThong' then 'BST'
                when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS' # ,'PMD-WH','BKK-WH-Ecommerce','BKK-WH-LAS物料仓'
                when w.name='LCP Warehouse' then 'LCP' end 仓库名称
            ,rw.out_warehouse_time
            ,rwg.out_num
            ,sg.bar_code
        from
        wms_production.return_warehouse rw
        left join wms_production.return_warehouse_goods rwg on rw.id = rwg.return_warehouse_id
        left join wms_production.seller_goods sg on sg.id =rwg.seller_goods_id 
        left join wms_production.seller s on rw.seller_id = s.id
        left join wms_production.warehouse w ON rw.`warehouse_id` = w.`id`
        where 1=1
            and rw.verify_time >= SUBDATE(subdate(curdate(),date_format(curdate(),'%w') - 1),2) -- 上周五
            and rw.verify_time < subdate(curdate(),if(date_format(curdate(),'%w')=0,7,date_format(curdate(),'%w'))-5)
            and rw.type=2
            and s.name='FFM-TH'
    )
    group by 1,2,3
)outbound2 on outbound2.仓库 = thisfir.仓库 and outbound2.商品条码 = thisfir.商品条码

left join
(
    -- 收费包材明细数据
    select
        week(left(bd.业务日期,10)+ interval 1 day) 周
        ,bd.仓库
        ,bd.业务日期
        ,bd.包材映射商品条码 商品条码
        ,sum(bd.条码数量)   数量
    from
    (
        select 
            -- a.business_sn     as                         '包材单号',
            a.business_date   as                         '业务日期',
            -- s.name            as                         '货主',
            -- w.name            as                         '仓库',
            case when w.name='AutoWarehouse' then 'AGV'
                when w.name='BPL-Return Warehouse' then 'BPL-Return'
                when w.name='BPL3-LIVESTREAM' then 'BPL3'
                when w.name='BangsaoThong' then 'BST'
                when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS'
                when w.name='LCP Warehouse' then 'LCP' end 仓库,
            -- 0 + convert(a.settlement_amount / 100, char) '结算金额',
            -- c.container_id    as                         '包材ID',
            d.name            as                          包材名称,
            d.bar_code        as                         包材条码,
            c.number          as                         '条码数量',
            -- e.seller_goods_id as                         '包材映射商品ID',
            f.bar_code        as                         '包材映射商品条码'
        from wms_production.billing_detail a
            join wms_production.billing_projects b on a.billing_projects_id = b.id
            join wms_production.container_inventory_log c on a.business_id = c.container_order_id
            join wms_production.container d on c.container_id = d.id
            left join wms_production.container_goods_mapping e on e.container_id = c.container_id
            left join wms_production.seller_goods f on e.seller_goods_id = f.id
            join wms_production.seller s on a.seller_id = s.id
            join wms_production.warehouse w on a.warehouse_id = w.id
        where b.documents = 6
        and a.business_date >= SUBDATE(subdate(curdate(),date_format(curdate(),'%w') - 1),2) -- 上周五
        and a.business_date < subdate(curdate(),if(date_format(curdate(),'%w')=0,7,date_format(curdate(),'%w'))-5)
        and d.seller_id = 0
        and s.disabled = 0
    ) bd
  group by 1,2,3,4
) usedetail on usedetail.仓库 = thisfir.仓库 and usedetail.商品条码 = thisfir.商品条码



-- erp还没做

-- 收费包材明细数据 erp wms
select
     bd.业务日期
    ,week(left(bd.业务日期,10)+ interval 1 day) 周
    ,bd.仓库
    ,bd.包材条码
    ,sum(bd.条码数量)   数量
from
(
    select 
        -- a.business_sn     as                         '包材单号',
        a.business_date   as                         '业务日期',
        -- s.name            as                         '货主',
        -- w.name            as                         '仓库',
        case when w.name='AutoWarehouse' then 'AGV'
            when w.name='BPL-Return Warehouse' then 'BPL-Return'
            when w.name='BPL3-LIVESTREAM' then 'BPL3'
            when w.name='BangsaoThong' then 'BST'
            when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS'
            when w.name='LCP Warehouse' then 'LCP' end 仓库,
        -- 0 + convert(a.settlement_amount / 100, char) '结算金额',
        -- c.container_id    as                         '包材ID',
        d.name            as                          包材名称,
        d.bar_code        as                         包材条码,
        c.number          as                         '条码数量',
        -- e.seller_goods_id as                         '包材映射商品ID',
        f.bar_code        as                         '包材映射商品条码'
    from wms_production.billing_detail a
        join wms_production.billing_projects b on a.billing_projects_id = b.id
        join wms_production.container_inventory_log c on a.business_id = c.container_order_id
        join wms_production.container d on c.container_id = d.id
        left join wms_production.container_goods_mapping e on e.container_id = c.container_id
        left join wms_production.seller_goods f on e.seller_goods_id = f.id
        join wms_production.seller s on a.seller_id = s.id
        join wms_production.warehouse w on a.warehouse_id = w.id
    where b.documents = 6
    and a.business_date >= date_sub(date(now() + interval -1 hour),interval 70 day)
    and d.seller_id = 0
    and s.disabled = 0

    union all
    -- erp
    select
        cubd.date
        ,case when w.name='AutoWarehouse'   then 'AGV'
            when w.name='BPL-Return Warehouse'  then 'BPL-Return'
            when w.name='BPL3-LIVESTREAM'   then 'BPL3'
            when w.name='BangsaoThong'  then 'BST'
            when w.name IN ('BKK-WH-LAS2电商仓')    then 'LAS'
            when w.name ='LCP Warehouse' then 'LCP' end 仓库
        ,c.name 包材名称
        ,c.bar_code
        ,cubd.actual_num+0 条码数量
        ,c.external_code 包材映射商品条码
    from 
    erp_wms_prod.container_use_billing_detail cubd
    left join 
    erp_wms_prod.container c on if(cubd.container_id=0, actual_container_id, cubd.container_id) = c.id
    left join
    erp_wms_prod.seller sl on cubd.seller_id = sl.id
    LEFT JOIN 
    erp_wms_prod.warehouse w ON cubd.warehouse_id = w.id
    where w.name='BPL3-LIVESTREAM'
    and cubd.date >= date_sub(date(now() + interval -1 hour),interval 70 day)
) bd
group by    bd.业务日期
    ,week(left(bd.业务日期,10)+ interval 1 day)
    ,bd.仓库
    ,bd.包材名称
order by bd.业务日期 desc
    ,bd.仓库
    ,bd.包材名称
