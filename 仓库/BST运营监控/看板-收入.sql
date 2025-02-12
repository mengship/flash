


-- erp 
WITH a AS(
    SELECT 
    分类,seller_id,warehouse_id,jf_date,SUM(amount) 收入
    FROM(
        SELECT 
        '操作费' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,date(ji_fei_time) jf_date
        ,SUM(amount) amount
        FROM erp_wms_prod.order_operation_billing_detail 
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4

    UNION

    SELECT 
    分类,seller_id,warehouse_id,jf_date,SUM(amount) 收入
    FROM(
        SELECT 
        '组装拆卸费用' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,date(ji_fei_time) jf_date
        ,SUM(amount) amount
        FROM erp_wms_prod.ad_task_fee_billing_detail 
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4

    UNION

    SELECT 
    分类,seller_id,warehouse_id,jf_date,SUM(amount) 收入
    FROM(
        SELECT 
        '用车申请任务' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,date(ji_fei_time) jf_date
        ,SUM(amount) amount
        FROM erp_wms_prod.car_task_fee_billing_detail         
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4

    UNION

    SELECT 
    分类,seller_id,warehouse_id,jf_date,SUM(amount) 收入
    FROM(
        SELECT 
        '换包装作业' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,date(ji_fei_time) jf_date
        ,SUM(amount) amount
        FROM erp_wms_prod.change_package_fee_billing_detail         
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4

    UNION

    SELECT 
    分类,seller_id,warehouse_id,jf_date,SUM(amount) 收入
    FROM(
        SELECT 
        '消耗品使用' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,date(ji_fei_time) jf_date
        ,SUM(amount) amount
        FROM erp_wms_prod.consumables_use_billing_detail         
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4

    UNION

    SELECT 
    分类,seller_id,warehouse_id,jf_date,SUM(amount) 收入
    FROM(
        SELECT 
        '包材费' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,date(ji_fei_time) jf_date
        ,SUM(amount) amount
        FROM erp_wms_prod.container_use_billing_detail         
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4

    UNION

    SELECT 
    分类,seller_id,warehouse_id,jf_date,SUM(amount) 收入
    FROM(
        SELECT 
        '入库费' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,date(ji_fei_time) jf_date
        ,SUM(amount) amount
        FROM erp_wms_prod.inventory_fee_billing_detail         
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4

    UNION

    SELECT 
    分类,seller_id,warehouse_id,jf_date,SUM(amount) 收入
    FROM(
        SELECT 
        '退货入库费' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,date(ji_fei_time) jf_date
        ,SUM(amount) amount
        FROM erp_wms_prod.inventory_return_fee_billing_detail         
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4

    UNION

    SELECT 
    分类,seller_id,warehouse_id,jf_date,SUM(amount) 收入
    FROM(
        SELECT 
        '贴码费用' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,date(ji_fei_time) jf_date
        ,SUM(amount) amount
        FROM erp_wms_prod.label_service_order_fee_billing_detail                 
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4

    UNION

    SELECT 
    分类,seller_id,warehouse_id,jf_date,SUM(amount) 收入
    FROM(
        SELECT 
        '拦截任务费' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,date(ji_fei_time) jf_date
        ,SUM(amount) amount
        FROM erp_wms_prod.order_clear_fee_billing_detail         
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4

    UNION

    SELECT 
    分类,seller_id,warehouse_id,jf_date,SUM(amount) 收入
    FROM(
        SELECT 
        '退供出库费' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,date(ji_fei_time) jf_date
        ,SUM(amount) amount
        FROM erp_wms_prod.provider_return_fee_billing_detail         
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4

    UNION

    SELECT 
    分类,seller_id,warehouse_id,jf_date,SUM(amount) 收入
    FROM(
        SELECT 
        '出库费' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,date(ji_fei_time) jf_date
        ,SUM(amount) amount
        FROM erp_wms_prod.stock_out_fee_billing_detail                 
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4

    UNION

    SELECT 
    分类,seller_id,warehouse_id,jf_date,SUM(amount) 收入
    FROM(
        SELECT 
        '仓储费' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,date(ji_fei_time) jf_date
        ,SUM(amount) amount
        FROM erp_wms_prod.storage_fee_billing_detail                 
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4

    UNION

    SELECT 
    分类,seller_id,warehouse_id,jf_date,SUM(amount) 收入
    FROM(
        SELECT 
        '卸货费' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,date(ji_fei_time) jf_date
        ,SUM(amount) amount
        FROM erp_wms_prod.unloading_fee_billing_detail                 
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4

    UNION

    SELECT 
    分类,seller_id,warehouse_id,jf_date,SUM(amount) 收入
    FROM(
        SELECT 
        '效期管理费' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,date(ji_fei_time) jf_date
        ,SUM(amount) amount
        FROM erp_wms_prod.validity_fee_billing_detail                 
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4

    union

    SELECT 
    分类,seller_id,warehouse_id,jf_date,SUM(amount) 收入
    FROM(
        SELECT 
        '组装拆卸费用流水(订单来源)' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,date(ji_fei_time) jf_date
        ,SUM(amount) amount
        FROM erp_wms_prod.ad_task_order_new_fee_billing_detail                 
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4

    UNION

    SELECT 
    分类,seller_id,warehouse_id,jf_date,SUM(amount) 收入
    FROM(
        SELECT 
        '销毁任务费用' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,date(ji_fei_time) jf_date
        ,SUM(amount) amount
        FROM erp_wms_prod.destroy_task_fee_billing_detail                 
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4

    UNION
    
    SELECT 
    分类,seller_id,warehouse_id,jf_date,SUM(amount) 收入
    FROM(
        SELECT 
        '贴码服务流水' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,date(ji_fei_time) jf_date
        ,SUM(amount) amount
        FROM erp_wms_prod.label_service_fee_billing_detail                 
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4
    
    UNION
    
    SELECT 
    分类,seller_id,warehouse_id,jf_date,SUM(amount) 收入
    FROM(
        SELECT 
        '手动流水' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        -- ,left(business_time,7) 收入月份
        ,date(business_time) jf_date
        ,SUM(amount) amount
        FROM erp_wms_prod.manual_add_billing_detail                 
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4
    
    UNION
    
    SELECT 
    分类,seller_id,warehouse_id,jf_date,SUM(amount) 收入
    FROM(
        SELECT 
        '用工加班任务流水' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,date(ji_fei_time) jf_date
        ,SUM(amount) amount
        FROM erp_wms_prod.overtime_task_fee_billing_detail                 
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4
)
select 
    blp.billing_name_zh 收入类型,
    CASE WHEN LEFT(blp.billing_name_zh,3) IN ('仓储费','入库费','出库费','包材费','卸货费') THEN LEFT(blp.billing_name_zh,3)
        ELSE blp.billing_name_zh END 收入类型2,
    left(bld.business_date,10) 日期,
    week(left(bld.business_date,10)+ interval 1 day) 周,
    case when  w.name='AutoWarehouse' then 'AGV'
        when w.name='BPL-Return Warehouse' then 'BPL-Return'
        when w.name='BPL3-LIVESTREAM' then 'BPL3'
        when w.name='BangsaoThong' then 'BST'
        when w.name IN ('BKK-WH-LAS2电商仓') then 'LAS'
        when w.name='LCP Warehouse' then 'LCP' end 仓库名称,
    sum(bld.settlement_amount)/100 amount -- 结算金额
from wms_production.billing_detail bld
left join wms_production.billing_projects blp on bld.billing_projects_id= blp.id
left join wms_production.warehouse w on bld.warehouse_id=w.id
where 1=1
    -- and bl.type='1'
    -- and billing_name_zh='操作费'
    and left(bld.business_date,10) >=left(now() - interval 70 day,10)
    and LEFT(blp.billing_name_zh,2) <> '快递'
group by 1,2,3,4,5 
having 仓库名称 is not null
  union
SELECT 
  分类
  ,分类
  ,jf_date
  ,week(jf_date+ interval 1 day) 周
  ,left(w.name, 4) 仓库
  ,sum(收入) FROM a
LEFT JOIN `erp_wms_prod`.`seller` sl on a.`seller_id`=sl.`id`
LEFT JOIN `erp_wms_prod`.`warehouse` w on a.`warehouse_id`=w.`id`
WHERE 收入 != 0
and w.name='BPL3-LIVESTREAM'
and jf_date >=left(now() - interval 70 day,10)
group by 1,2,3,4,5