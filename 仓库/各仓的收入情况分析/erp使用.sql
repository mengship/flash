-- erp 
WITH a AS(
    SELECT 
    分类,seller_id,warehouse_id,收入月份,SUM(amount) 收入
    FROM(
        SELECT 
        '操作费' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,left(ji_fei_time,7) 收入月份
        ,SUM(amount) amount
        FROM erp_wms_prod.order_operation_billing_detail 
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4

    UNION

    SELECT 
    分类,seller_id,warehouse_id,收入月份,SUM(amount) 收入
    FROM(
        SELECT 
        '组装拆卸费用' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,left(ji_fei_time,7) 收入月份
        ,SUM(amount) amount
        FROM erp_wms_prod.ad_task_fee_billing_detail 
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4

    UNION

    SELECT 
    分类,seller_id,warehouse_id,收入月份,SUM(amount) 收入
    FROM(
        SELECT 
        '用车申请任务' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,left(ji_fei_time,7) 收入月份
        ,SUM(amount) amount
        FROM erp_wms_prod.car_task_fee_billing_detail         
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4

    UNION

    SELECT 
    分类,seller_id,warehouse_id,收入月份,SUM(amount) 收入
    FROM(
        SELECT 
        '换包装作业' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,left(ji_fei_time,7) 收入月份
        ,SUM(amount) amount
        FROM erp_wms_prod.change_package_fee_billing_detail         
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4

    UNION

    SELECT 
    分类,seller_id,warehouse_id,收入月份,SUM(amount) 收入
    FROM(
        SELECT 
        '消耗品使用' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,left(ji_fei_time,7) 收入月份
        ,SUM(amount) amount
        FROM erp_wms_prod.consumables_use_billing_detail         
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4

    UNION

    SELECT 
    分类,seller_id,warehouse_id,收入月份,SUM(amount) 收入
    FROM(
        SELECT 
        '包材使用费' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,left(ji_fei_time,7) 收入月份
        ,SUM(amount) amount
        FROM erp_wms_prod.container_use_billing_detail         
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4

    UNION

    SELECT 
    分类,seller_id,warehouse_id,收入月份,SUM(amount) 收入
    FROM(
        SELECT 
        '入库费' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,left(ji_fei_time,7) 收入月份
        ,SUM(amount) amount
        FROM erp_wms_prod.inventory_fee_billing_detail         
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4

    UNION

    SELECT 
    分类,seller_id,warehouse_id,收入月份,SUM(amount) 收入
    FROM(
        SELECT 
        '退货入库费' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,left(ji_fei_time,7) 收入月份
        ,SUM(amount) amount
        FROM erp_wms_prod.inventory_return_fee_billing_detail         
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4

    UNION

    SELECT 
    分类,seller_id,warehouse_id,收入月份,SUM(amount) 收入
    FROM(
        SELECT 
        '贴码费用' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,left(ji_fei_time,7) 收入月份
        ,SUM(amount) amount
        FROM erp_wms_prod.label_service_order_fee_billing_detail                 
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4

    UNION

    SELECT 
    分类,seller_id,warehouse_id,收入月份,SUM(amount) 收入
    FROM(
        SELECT 
        '拦截任务费' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,left(ji_fei_time,7) 收入月份
        ,SUM(amount) amount
        FROM erp_wms_prod.order_clear_fee_billing_detail         
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4

    UNION

    SELECT 
    分类,seller_id,warehouse_id,收入月份,SUM(amount) 收入
    FROM(
        SELECT 
        '退供出库费' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,left(ji_fei_time,7) 收入月份
        ,SUM(amount) amount
        FROM erp_wms_prod.provider_return_fee_billing_detail         
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4

    UNION

    SELECT 
    分类,seller_id,warehouse_id,收入月份,SUM(amount) 收入
    FROM(
        SELECT 
        '出库费' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,left(ji_fei_time,7) 收入月份
        ,SUM(amount) amount
        FROM erp_wms_prod.stock_out_fee_billing_detail                 
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4

    UNION

    SELECT 
    分类,seller_id,warehouse_id,收入月份,SUM(amount) 收入
    FROM(
        SELECT 
        '存储费' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,left(ji_fei_time,7) 收入月份
        ,SUM(amount) amount
        FROM erp_wms_prod.storage_fee_billing_detail                 
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4

    UNION

    SELECT 
    分类,seller_id,warehouse_id,收入月份,SUM(amount) 收入
    FROM(
        SELECT 
        '卸货费' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,left(ji_fei_time,7) 收入月份
        ,SUM(amount) amount
        FROM erp_wms_prod.unloading_fee_billing_detail                 
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4

    UNION

    SELECT 
    分类,seller_id,warehouse_id,收入月份,SUM(amount) 收入
    FROM(
        SELECT 
        '效期管理费' AS 分类
        ,seller_id
        ,warehouse_id
        ,sn
        ,left(ji_fei_time,7) 收入月份
        ,SUM(amount) amount
        FROM erp_wms_prod.validity_fee_billing_detail                 
        GROUP BY 2,3,4,5
        )
    GROUP BY 1,2,3,4
)

SELECT w.name 仓库,sl.name 客户,分类,收入月份,收入 FROM a
LEFT JOIN `erp_wms_prod`.`seller` sl on a.`seller_id`=sl.`id`
LEFT JOIN `erp_wms_prod`.`warehouse` w on a.`warehouse_id`=w.`id`
WHERE 收入 != 0
