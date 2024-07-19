-- 仓储费计费规则
SELECT
     s.name '货主'
     ,w.name '仓库'
     ,wbr.billing_code '规则编号'
     ,wbr.billing_name '规则名称'
     ,case wbr.status 
               when 0 then '删除'
               when 1 then '停用'
               when 2 then '存盘'
               when 3 then '启用'
     else wbr.status end '状态'
     ,wbr.created '创建时间'
     ,wbr.modified  '修改时间'
     ,wbrd.billing_projects_id '序号'
     ,case wbrd.billing_projects_id
                when 1 then '仓储费'
                when 2 then '操作费'
                when 3 then '出库费'
                when 4 then '装货费'
                when 5 then '卸货费'
                when 6 then '入库费'
                when 7 then '销退入库费'
                when 8 then '增值服务（全部）'
                when 9 then '盘点费'
                when 10 then '包材费'
                when 12 then '拦截费'
                when 14 then '短信费'
                when 18 then '仓储费-按天/件'
                when 19 then '入库费（有效期）'
                when 21 then '出库费2'
                when 22 then '理赔费'
                when 23 then '租车费用'
                when 24 then '发货包材费'
                when 25 then '敏货操作费加价'
                when 27 then '组装拆卸费'
                when 28 then '仓储费（恒温仓）'
                when 29 then '退款服务费用'
                when 30 then '入库费（赠品）'
                when 31 then '出库费（退仓）'
                when 32 then '粘贴条码、标签费用'
                when 33 then '商品包装费用'
                when 34 then '销毁费'
                when 35 then '条码打印费用'
                when 36 then '商品换码费用'
                when 37 then '商品组装费用'
                when 39 then 'QC&商品加工费用'
                when 40 then '商品附加费'
                when 41 then '贵品保管费'
                when 43 then '卸货费（按货柜）'
                when 44 then '装货费（按货柜）'
                when 45 then '入库费（SN）'
                when 46 then '合并发货单'
                when 47 then '包材费（合并发货）'
                when 48 then '包材费（分摊）'
                when 49 then '出库单（折扣商品）'
        else billing_projects_id end '计费名称'
    ,wbrd.billing_rules_code '计费规则代码'
    ,case wbrd.billing_order_type
            when 1 then '销售订单'
            when 2 then '出库单（普通出库）'
                when 3 then '入库单（全部）'
                when 4 then '销退订单'
                when 5 then '增值服务单（全部）'
                when 6 then '包材单'
                when 7 then '装货单'
                when 8 then '卸货单'
                when 9 then '盘点单'
                when 10 then '仓库租赁单'
                when 11 then '拦截归位单'
                when 12 then '组装拆卸单'
                when 13 then '短信'
                when 14 then '退款单'
                when 15 then '服务申请单'
                when 16 then '理赔单'
                when 21 then '默认包材单'
                when 35 then '报废单'
                when 39 then '出库单（退仓出库）'
                when 41 then '增值服务单（商品条码打印）'
                when 42 then '增值服务单（商品标签贴码）'
                when 43 then '增值服务单（商品加工）'
                when 44 then '增值服务单（商品包装）'
                when 45 then '增值服务单（商品组装）'
                when 46 then '增值服务单（商品换码）'
                when 47 then '合并发货单'
                when 48 then '包材单（合并发货）'
                when 49 then '包材单（合并发货）-按货主均摊'
        else wbrd.billing_order_type end '计费单据'
        ,case wbrd.billing_data 
                when 'orderNum' then '订单数'
                when 'orderGoodsNum' then '商品件数'
                when 'orderTotalPrice' then '订单总金额'
                when 'orderGoodsByShelf' then '商品件数（保质期商品）'
                when 'orderGoodsNumBySN' then '商品件数（SN商品）'
                when 'goodsDeclaredValue' then '商品声明价值'
                when 'orderTotalVolume' then '订单总体积'
                when 'affixedCodeNum' then '增值服务数量'
                when 'goodsVolumeByConstantTemperature' then '商品体积（恒温仓）'
                when 'goodsVolumeByZZDays' then '商品体积（周转天数）'
                when 'orderGoodsNumByMu' then '商品件数（母件）'
                when 'messageNum' then '短信条数'
                when 'orderGoodsNumByUnitConversion' then '商品件数（单位换算）'
                when 'goodsVolume' then '商品体积'
                when 'rentedArea' then '租用面积'
                when 'locationAreaNoLocation' then '货位面积（按公摊系数-不区分货位）㎡'
                when 'orderContainerSize' then '订单货柜尺寸'
                when 'orderGoodsNumByUnit' then '商品件数（指定单位）'
                when 'locationArea' then '货位面积（按公摊系数-区分货位）㎡'
                when 'orderGoodsNumByUnitConversionZk' then '商品件数（单位换算）-折扣'
                when 'orderGoodsByMaxWeightSize' then '商品件数（最大重量尺寸）'
                when 'goodsVolumeByShare' then '商品体积（含公摊）'
                when 'orderGoodsNumByDays' then '商品件数'
                when 'orderGoodsNumByDaysVolume' then '商品库龄体积'
                when 'orderGoodsNumByZi' then '商品件数（子件）'
                when 'orderGoodsByWeightSize' then '商品件数（平均重量尺寸）'
                when 'goodsVolumeByRoomTemperature' then '商品体积（常温仓）'
                when 'locationAreaNormal' then '货位面积（不含公摊系数）㎡'
                when 'hwRentedArea' then '恒温仓面积'
                when 'boxNum' then '箱单数'
                when 'orderGoodsTypeNum' then '商品品种数'
                when 'locationAreaByZZRate' then '货位面积（周转率）㎡'
                when 'orderGoodsNumByPrimary' then '商品件数（普通商品）'
                when 'goodsVolumeByZZDaysForDo' then '商品体积（周转天数-仅发货单）'
        else wbrd.billing_data end '计费数据'
        ,case wbrd.billing_rule
                when 'ladder' then '阶梯价'
                when 'unionLadder' then '组合阶梯价'
                when 'unitBySpec' then '指定计费单位'
                when 'num' then '按数值计费'
                when 'free' then '不计费'
                when 'rate' then '费率计费'
        else wbrd.billing_rule end '计费规则'
        ,wbrd.cost_price '单价'
        ,wbrd.special_cost '阶梯价参数'
        ,wbrd.is_partial_discount '是否部分优惠'
        ,wbrd.days '免租期'
        ,wbrd.min_fee '最低收费 两位小数'
        ,wbrd.billing_projects_min_fee '计费项维度最低收费'
        ,wbrd.min_number '最低数值'
        ,wbrd.modified '修改时间'
from wms_production.warehouse_billing_rules wbr
left join wms_production.warehouse_billing_rules_detail wbrd on wbr.id = wbrd.warehouse_billing_rules_id
left join wms_production.warehouse_billing_rules_ref wbrr on wbrr.warehouse_billing_rules_id = wbr.id
left join wms_production.seller s on s.id = wbrr.seller_id
left join wms_production.warehouse w on w.id = wbrr.warehouse_id
-- where wbr.billing_code = 'R215'
where date(wbrd.modified) >= '2024-06-01' # 从6月到现在有哪些合同规则发生过变更
order by 3,8





-- 包材价格表查询
SELECT 
    crd.container_rule_id '规则编号',
    cr.billing_name  '规则名称',
    se.name  '货主',
    ct.id '包材序号',
    ct.bar_code 'bar_code',
    ct.name '包材名称',
    ct.`length`  '长',
    ct.width '宽',
    ct.height '高',
    ct.weight '重量',
    ct.volume/1000000 '体积',
    crd.packing_charge/100 '价格'
from wms_production.container_rule_detail crd
left join wms_production.container_top ct on ct.id =crd.container_top_id 
left join wms_production.container_rule cr on cr.id = crd.container_rule_id 
left join wms_production.container_rule_ref crr on crr.container_rule_id =crd.container_rule_id 
left join wms_production.seller se on se.id =crr.seller_id
-- where se.name  = 'Life Extension'