CREATE TABLE `delivery_order` (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '',
  `delivery_sn` varchar NOT NULL DEFAULT '' COMMENT '发货单号',
  `order_sn` varchar DEFAULT '' COMMENT '外部单号',
  `third_id` varchar NOT NULL DEFAULT '' COMMENT '第三方ID',
  `platform_source_id` int DEFAULT '0' COMMENT '平台店铺ID(seller_platform_source主键)',
  `platform_source_user` varchar NOT NULL DEFAULT '' COMMENT '店铺用户ID',
  `platform_source_email` varchar NOT NULL DEFAULT '' COMMENT '店铺用户Email',
  `order_source_type` tinyint NOT NULL DEFAULT '1' COMMENT '[1]人工录入 [2]批量导入 [3]接口获取 [4]自动生成',
  `order_source_id` int COMMENT '订单来源',
  `order_source_sn` varchar NOT NULL DEFAULT '' COMMENT '来源单号',
  `warehouse_id` int COMMENT '仓库主键ID',
  `is_smart` tinyint NOT NULL DEFAULT '1' COMMENT '[0]货主分仓 [1]智能分仓',
  `open_smart` tinyint NOT NULL DEFAULT '0' COMMENT '[0]正常 [1]关闭智能分仓 [2]开启智能分仓',
  `is_replace_goods` smallint NOT NULL DEFAULT '0' COMMENT '是否已整单商品替换',
  `smart_audit` tinyint NOT NULL DEFAULT '0' COMMENT '[0]未智能审单 [1]已智能审单',
  `seller_id` int NOT NULL COMMENT '货主主键ID',
  `salesman` int NOT NULL DEFAULT '0' COMMENT '业务员ID',
  `prompt_urgent` tinyint NOT NULL DEFAULT '0' COMMENT '是否加急',
  `system_short` tinyint NOT NULL DEFAULT '0' COMMENT '系统报缺提示状态1系统报缺 2 待补货 3 待上架',
  `manual_short` tinyint NOT NULL DEFAULT '0' COMMENT '0没有标记人工报缺  1 标记人工报缺',
  `seller_goods_id` int COMMENT '商品主键ID',
  `pack_order_id` int COMMENT '打包单主键Id',
  `auditor_id` int COMMENT '审核人ID',
  `status` int NOT NULL COMMENT '订单状态',
  `other_warehouse_status` tinyint DEFAULT '0' COMMENT '[0]未下发 [1]已下发 [2]下发失败(新增) [3]下发失败(修改) [10]待拣选 [20]拣选中 [30]拣选完成 [40]打包中 [50]待复核 [60]复核中 [70]复核完成 [80]打包完成',
  `is_presale` tinyint NOT NULL DEFAULT '0' COMMENT '是否是预售订单 默认为0，1是预售订单',
  `prompt` tinyint NOT NULL DEFAULT '0' COMMENT '[1]撤回等待 [2]生成拦截归位单 [3]生成上架单(拦截成功) [4]拦截未成功',
  `times` smallint NOT NULL DEFAULT '1' COMMENT '默认第1次，每次撤回到待审核状态时加1',
  `back_type` int NOT NULL DEFAULT '0' COMMENT '撤回类型',
  `back_text` varchar COMMENT '撤回文本',
  `type` tinyint NOT NULL DEFAULT '1' COMMENT '订单类型 [1]销售订单 [2]补货订单 [3]换货订单',
  `container_id` int COMMENT '包装箱主键ID',
  `length` int DEFAULT '0' COMMENT '长(单位:mm)',
  `width` int DEFAULT '0' COMMENT '宽(单位:mm)',
  `height` int DEFAULT '0' COMMENT '高(单位:mm)',
  `volume` varchar DEFAULT '' COMMENT '体积(单位:mm³)',
  `total_weight` int NOT NULL DEFAULT '0' COMMENT '总重量(单位:克)',
  `printable` tinyint NOT NULL DEFAULT '0' COMMENT '可打印发货单状态',
  `is_prepack` tinyint NOT NULL DEFAULT '0' COMMENT '预包装',
  `prepacker` int COMMENT '预打包人',
  `pack_time` datetime COMMENT '打包时间',
  `express_code` varchar NOT NULL DEFAULT '' COMMENT '事先确定的物流公司CODE',
  `logistic_company_id` int COMMENT '物流公司ID',
  `express_name` varchar NOT NULL DEFAULT '' COMMENT '快递公司名称',
  `own_express` tinyint NOT NULL DEFAULT '0' COMMENT '[0]通用快递 [1]自己快递',
  `distribution` varchar NOT NULL DEFAULT 'express' COMMENT '配送方式',
  `pre_express` tinyint NOT NULL DEFAULT '0' COMMENT '是否事先确定的快递单号',
  `express_sn` varchar COMMENT '快递单号',
  `multi_express` tinyint NOT NULL DEFAULT '0' COMMENT '[1]多运单号 [2]忽略多运单',
  `rts_express_sn` varchar NOT NULL DEFAULT '' COMMENT '设置RTS时的运单号',
  `rts_shipment_provider` varchar NOT NULL DEFAULT '' COMMENT 'RTS承运商',
  `replenishment` tinyint NOT NULL DEFAULT '0' COMMENT '[1]是否已自动补货',
  `rts_status` tinyint NOT NULL DEFAULT '0' COMMENT '[1]已设置RTS',
  `express_type` tinyint NOT NULL DEFAULT '1' COMMENT '获取快递类型',
  `routes_status` tinyint NOT NULL DEFAULT '10' COMMENT '第三方物流状态 [10]创建',
  `invoice_required` tinyint NOT NULL DEFAULT '0' COMMENT '是否需要发票',
  `tax_code` varchar NOT NULL DEFAULT '' COMMENT '发票编码',
  `branch_number` varchar NOT NULL DEFAULT '' COMMENT '开票人代码',
  `tax_invoice_requested` tinyint NOT NULL DEFAULT '0' COMMENT '是否需要发票tax',
  `invoice_info` varchar NOT NULL DEFAULT '[]' COMMENT '发票信息',
  `consignee_name` varchar NOT NULL DEFAULT '' COMMENT '收货人',
  `countries` varchar NOT NULL DEFAULT 'th' COMMENT '国家',
  `province` varchar NOT NULL DEFAULT '' COMMENT '省',
  `city` varchar NOT NULL DEFAULT '' COMMENT '市',
  `district` varchar NOT NULL DEFAULT '' COMMENT '区',
  `code` varchar NOT NULL DEFAULT '' COMMENT '编号',
  `postal_code` varchar NOT NULL DEFAULT '' COMMENT '邮编',
  `consignee_address` varchar NOT NULL DEFAULT '' COMMENT '详细地址',
  `phone_number` varchar NOT NULL DEFAULT '' COMMENT '电话号码',
  `comment` varchar DEFAULT '' COMMENT '留言',
  `buyer_message` varchar DEFAULT '' COMMENT '买家留言',
  `is_export` tinyint NOT NULL DEFAULT '0' COMMENT '已导出',
  `delivery_print_id` int COMMENT '发货单打印人ID',
  `delivery_print_time` datetime COMMENT '发货单打印日期',
  `print_id` int COMMENT '制单人ID',
  `is_printed` tinyint NOT NULL DEFAULT '0' COMMENT '已打印快递面单',
  `print_time` datetime COMMENT '打印日期',
  `show_price` tinyint NOT NULL DEFAULT '1' COMMENT '[0]不印价格 [1]打印价格',
  `total_price` decimal(20, 0) NOT NULL DEFAULT '0' COMMENT '总价格',
  `cod_amount` bigint NOT NULL DEFAULT '0' COMMENT 'COD金额(单位:萨当)',
  `order_discount_total_money` bigint NOT NULL DEFAULT '0' COMMENT '整单优惠金额',
  `goods_discount_total_money` bigint NOT NULL DEFAULT '0' COMMENT '商品优惠总金额',
  `packing_charge` bigint COMMENT '包装费',
  `logistic_charge` bigint COMMENT '运费',
  `insure_declare_value` int NOT NULL DEFAULT '0' COMMENT '保价金额',
  `hot` varchar COMMENT 'ABC分类',
  `repository_id` int COMMENT '库区ID',
  `heteromorphism` tinyint NOT NULL DEFAULT '0' COMMENT '是否异形 [0]否 [1]是',
  `max_kinds` bigint NOT NULL DEFAULT '1' COMMENT '单品最大购买数量',
  `combo_kinds_num` int NOT NULL DEFAULT '0' COMMENT '套装品种数量',
  `combo_goods_num` bigint NOT NULL DEFAULT '0' COMMENT '套装商品数量',
  `kinds_num` int NOT NULL DEFAULT '0' COMMENT '品种数量',
  `goods_num` bigint NOT NULL DEFAULT '0' COMMENT '商品数量',
  `cancel_id` int NOT NULL DEFAULT '0' COMMENT '取消原因ID',
  `cancel_reason` varchar NOT NULL DEFAULT '' COMMENT '取消原因',
  `split_merge` varchar COMMENT '拆分合并信息',
  `is_split_merge` tinyint NOT NULL DEFAULT '0' COMMENT '[1]拆分 [2]合并 [3]拆分合并',
  `is_cancel` tinyint NOT NULL DEFAULT '0' COMMENT '是否取消',
  `is_update` tinyint NOT NULL DEFAULT '0' COMMENT '[0]没修改 [1]有修改',
  `master_order` tinyint NOT NULL DEFAULT '1' COMMENT '是否主单',
  `auto_split` tinyint NOT NULL DEFAULT '0' COMMENT '[0]未自动拆分 [1]已自动拆分',
  `express_process` tinyint NOT NULL DEFAULT '0' COMMENT '[20]打包接口完成 [30]获取面单号码 [40]设置RTS成功 [50]设置发票成功 [60]获取发票文件',
  `express_error` tinyint NOT NULL DEFAULT '0' COMMENT '获取电子面单错误类型',
  `error_prompt` varchar NOT NULL DEFAULT '' COMMENT '失败提示信息',
  `sap_response` tinyint NOT NULL DEFAULT '0' COMMENT 'SAP请求结果',
  `sap_id` varchar DEFAULT '' COMMENT '最大单品重量(单位:g)',
  `sap_time` datetime COMMENT 'SAP成功时间',
  `sap_error` varchar NOT NULL DEFAULT '' COMMENT '推送SAP失败原因',
  `pay_status` int COMMENT '支付状态 [0]未支付 [1]已支付',
  `pay_mode` int COMMENT '支付方式 [1]货到付款  [2]银行转账 [3]在线支付',
  `buy_time` datetime COMMENT '下单时间',
  `payment_time` datetime COMMENT '付款时间',
  `out_province` tinyint NOT NULL DEFAULT '0' COMMENT '[0]省内 [1]省外',
  `in_remote` tinyint NOT NULL DEFAULT '0' COMMENT '[0]正常 [1]偏远',
  `distance` int NOT NULL DEFAULT '0' COMMENT '距离(单位:m)',
  `points_time` datetime COMMENT '分仓时间',
  `wait_verify` datetime COMMENT '等待审核时间',
  `audit_time` timestamp COMMENT '审核时间',
  `can_audit_time` datetime COMMENT '能智能审核的时间',
  `express_time` datetime COMMENT '获取电子面单时间',
  `allocation_stop` datetime COMMENT '分配库存停止时间',
  `allocation_time` datetime COMMENT '分配库存时间',
  `wave_time` datetime COMMENT '生成波次时间',
  `wait_pick` datetime COMMENT '等待拣货时间',
  `start_pick` datetime COMMENT '开始拣货时间',
  `succ_pick` datetime COMMENT '完成拣货时间',
  `picked_id` int NOT NULL DEFAULT '0' COMMENT '拣选完成人',
  `start_receipt` datetime COMMENT '开始交接',
  `operator_id` bigint NOT NULL DEFAULT '0' COMMENT '扫描运单号操作人id',
  `delivery_time` timestamp COMMENT '发货时间',
  `lanShou_time` datetime COMMENT '揽收时间',
  `delivery_out_id` int COMMENT '发货交接人',
  `confirm_time` timestamp COMMENT '完成时间',
  `virtual_dispatch_status` tinyint COMMENT '虚拟签收状态 [1] 第三方发货未签收 [2] 标记平台签收成功 [3] 标记平台签收失败',
  `creator_id` int COMMENT '创建人',
  `created` timestamp DEFAULT CURRENT_TIMESTAMP COMMENT '',
  `modified` timestamp DEFAULT CURRENT_TIMESTAMP COMMENT '',
  `region` varchar NOT NULL DEFAULT '' COMMENT '大区,用于计费',
  `platform_status` tinyint DEFAULT '0' COMMENT '平台订单状态 默认0 1待处理 2准备发货 3已发货 4已签收 5交付失败 6被快递丢失 7退货 8被快递损坏',
  `platform_status_raw` varchar DEFAULT '' COMMENT '订单状态 平台接口返回的原始状态',
  `out_time` datetime COMMENT '预计出库时间',
  `is_conso` smallint DEFAULT '0' COMMENT '是否天猫集运 0 否 1 是',
  `check` tinyint DEFAULT '0' COMMENT '[1]自动审核',
  `platform_package_id` varchar DEFAULT '' COMMENT '平台包裹id',
  `lanShou_time_last` datetime COMMENT '末端揽收时间',
  `delivery_type` smallint DEFAULT '0' COMMENT '下发仓库类型',
  `channel_source` varchar DEFAULT '' COMMENT '渠道来源',
  PRIMARY KEY (`id`)
) DISTRIBUTE BY HASH(`id`) INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}'

CREATE TABLE `delivery_order_goods` (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '',
  `delivery_order_id` int NOT NULL COMMENT '发货单主键ID',
  `delivery_combo_id` int COMMENT '订单套装ID',
  `is_gift` int NOT NULL DEFAULT '0' COMMENT '是否赠品',
  `warehouse_id` int COMMENT '仓库ID',
  `seller_goods_id` int NOT NULL COMMENT '商品主键ID',
  `goods_number` int NOT NULL COMMENT '商品数量',
  `preoccupied_inventory` int NOT NULL DEFAULT '0' COMMENT '预占用数量',
  `lack_inventory` int NOT NULL DEFAULT '0' COMMENT '缺货数量',
  `max_lack_inventory` int NOT NULL DEFAULT '0' COMMENT '历史缺货数量',
  `lack_time` int COMMENT '缺货时间',
  `status` tinyint NOT NULL DEFAULT '0' COMMENT '[1]缺库存 [2]待补货',
  `goods_price` bigint DEFAULT '0' COMMENT '销售单价(单位:萨当)',
  `total_price` bigint DEFAULT '0' COMMENT '销售总价(单位:萨当)',
  `replenishment` tinyint NOT NULL DEFAULT '0' COMMENT '[1]是否已自动补货',
  `created` timestamp DEFAULT CURRENT_TIMESTAMP COMMENT '',
  `modified` timestamp DEFAULT CURRENT_TIMESTAMP COMMENT '',
  `seller_id` bigint DEFAULT '0' COMMENT '货主ID',
  PRIMARY KEY (`id`)
) DISTRIBUTE BY HASH(`id`) INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}'

CREATE TABLE `seller_goods` (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '',
  `seller_id` int NOT NULL COMMENT '货主ID',
  `bar_code` varchar NOT NULL COMMENT '条形码',
  `two_bar_code` varchar NOT NULL DEFAULT '' COMMENT '二级条形码',
  `three_bar_code` varchar NOT NULL DEFAULT '' COMMENT '三级条形码',
  `goods_code` varchar NOT NULL DEFAULT '' COMMENT '商品货号',
  `order_source_type` tinyint NOT NULL DEFAULT '1' COMMENT '[1]人工录入 [2]批量导入 [3]接口获取',
  `order_source_id` int COMMENT '订单来源',
  `channel_source` varchar NOT NULL DEFAULT '' COMMENT '渠道来源',
  `encode_type` varchar NOT NULL COMMENT '',
  `sn_reg` varchar NOT NULL DEFAULT '' COMMENT 'SN码正则',
  `is_asset` tinyint NOT NULL DEFAULT '0' COMMENT '',
  `name` varchar NOT NULL COMMENT '商品名称',
  `abbr_name` varchar NOT NULL DEFAULT '' COMMENT '简称',
  `parent_one` int COMMENT '一级类目ID',
  `parent_two` int COMMENT '二级类目ID',
  `category_id` int COMMENT '类目ID',
  `brand_id` int COMMENT '品牌ID',
  `ent_category_pid` int COMMENT '企业类目父ID',
  `ent_category_id` int COMMENT '企业类目ID',
  `one_unit` int COMMENT '基础单位',
  `in_unit` varchar COMMENT '指定入库计费单位',
  `out_unit` varchar COMMENT '指定出库计费单位',
  `packing_unit` varchar COMMENT '商品带包装单位',
  `two_unit` int COMMENT '二级单位',
  `two_conversion` int COMMENT '二级单位转换',
  `three_unit` int COMMENT '三级单位',
  `three_conversion` int COMMENT '三级单位转换',
  `is_combo` tinyint NOT NULL DEFAULT '0' COMMENT '是否套装',
  `is_imei` tinyint NOT NULL DEFAULT '0' COMMENT '是否IMEI码',
  `hot` varchar NOT NULL DEFAULT '' COMMENT 'ABC分类',
  `length` int COMMENT '长(单位:mm)',
  `width` int COMMENT '宽(单位:mm)',
  `height` int COMMENT '高(单位:mm)',
  `volume` varchar DEFAULT '' COMMENT '体积(单位:mm³)',
  `weight` int COMMENT '重(单位:g)',
  `prepack_weight` int COMMENT '预打包重量(单位:g)',
  `logistic_require` varchar NOT NULL DEFAULT '[]' COMMENT '特殊商品属性',
  `store_type` varchar DEFAULT 'full' COMMENT '存储规则',
  `image` varchar NOT NULL DEFAULT '' COMMENT '图片',
  `price` bigint DEFAULT '0' COMMENT '统一售价(单位:萨当)',
  `cost_price` bigint DEFAULT '0' COMMENT '成本价(单位:萨当)',
  `specification` varchar NOT NULL DEFAULT '' COMMENT '规格',
  `remark` varchar NOT NULL DEFAULT '' COMMENT '备注',
  `is_valuable` tinyint NOT NULL DEFAULT '0' COMMENT '[0]不是贵重物品 [1]是贵重物品',
  `is_locked` tinyint NOT NULL DEFAULT '0' COMMENT '[0]未新品维护 [1]已新品维护',
  `is_shelf_life` tinyint NOT NULL DEFAULT '0' COMMENT '[0]非保质期商品 [1]是保质期商品',
  `shelf_life_day` int COMMENT '保质期天数',
  `shelf_life_warning` int COMMENT '临期预警天数',
  `shelf_life_lock_up` int DEFAULT '0' COMMENT '临期禁售天数',
  `is_unpacked_delivery` tinyint NOT NULL DEFAULT '0' COMMENT '是否可不包装直接发货',
  `introduction` varchar NOT NULL DEFAULT '' COMMENT '商品简介',
  `replenishment_batch_num` int DEFAULT '0' COMMENT '商品补货批量',
  `thirty_sales_num` int NOT NULL DEFAULT '0' COMMENT '30天销售量',
  `status` tinyint NOT NULL DEFAULT '2' COMMENT '[0]删除 [1]停用 [2]存盘 [3]启用',
  `is_mix` tinyint NOT NULL DEFAULT '0' COMMENT '是否商品结构母件',
  `mix_status` tinyint NOT NULL DEFAULT '0' COMMENT '商品结构状态 2存盘 3启用 1停用 0删除',
  `mix_creater` int NOT NULL DEFAULT '0' COMMENT '商品结构创建人',
  `mix_created` timestamp DEFAULT CURRENT_TIMESTAMP COMMENT '商品结构创建时间',
  `created_name` varchar NOT NULL DEFAULT '' COMMENT '创建人',
  `modified_name` varchar NOT NULL DEFAULT '' COMMENT '最后修改人',
  `created` timestamp DEFAULT CURRENT_TIMESTAMP COMMENT '',
  `modified` timestamp DEFAULT CURRENT_TIMESTAMP COMMENT '',
  `rollback_mark` tinyint NOT NULL DEFAULT '0' COMMENT '是否退件入仓 默认否',
  `review_status` int DEFAULT '1' COMMENT '商品复核状态 1 未复核 2 已复核',
  `is_default_container` smallint NOT NULL DEFAULT '1' COMMENT '是否维护了默认包材 默认1 没有  2 有',
  `self_pack_code` smallint NOT NULL DEFAULT '0' COMMENT '是否启用自带包材0 未启用 1启用 默认不启用',
  `check_status` tinyint DEFAULT '0' COMMENT '0 未核对, 10 执行中(核对中), 20 待审核, 30 驳回, 40 已审核',
  `one_unit_is_packed` tinyint DEFAULT '2' COMMENT '一级单位是否自带包装。【1】自带包装【2】不自带',
  `reserve_bar_code` varchar DEFAULT '' COMMENT '备用条码',
  `one_unit_category_id` int DEFAULT '0' COMMENT '仓储分类id 数据字典SC01 对应的明细ID',
  `goods_shape` tinyint DEFAULT '1' COMMENT '【1】标准立方体【2】近似立方体【3】三角【4】可折叠【5】可叠套【6】可压缩【7】不可折叠',
  `two_unit_is_packed` tinyint DEFAULT '2' COMMENT '二级单位是否自带包装。【1】自带包装【2】不自带',
  `two_unit_length` int DEFAULT '0' COMMENT '长(单位：mm)',
  `two_unit_width` int DEFAULT '0' COMMENT '宽(单位：mm)',
  `two_unit_height` int DEFAULT '0' COMMENT '高(单位：mm)',
  `two_unit_volume` varchar DEFAULT '0' COMMENT '二级单位体积,单位mm³',
  `two_unit_weight` int DEFAULT '0' COMMENT '重量(单位：g)',
  `three_unit_is_packed` tinyint DEFAULT '2' COMMENT '二级单位是否自带包装。【1】自带包装【2】不自带',
  `three_unit_length` int DEFAULT '0' COMMENT '长(单位：mm)',
  `three_unit_width` int DEFAULT '0' COMMENT '宽(单位：mm)',
  `three_unit_height` int DEFAULT '0' COMMENT '高(单位：mm)',
  `three_unit_volume` varchar DEFAULT '0' COMMENT '三级单位体积,单位mm³',
  `three_unit_weight` int DEFAULT '0' COMMENT '重量(单位：g)',
  `is_belong_container` varchar DEFAULT 'n' COMMENT '是否属于包材: y(是),n(否)',
  `charging_type` tinyint DEFAULT '1' COMMENT '特殊商品计费属性 1 标准 2 折扣 3 高价',
  `is_exist_code` smallint DEFAULT '3' COMMENT '是否存在条码[2]否 [3]是',
  `shelf_life_type` smallint DEFAULT '1' COMMENT '保质期数类型1日，2月，3年',
  `multi_weight` tinyint DEFAULT '0' COMMENT '是否存在多个重量',
  `declared_value` bigint DEFAULT '0' COMMENT '声明价值',
  `spu_id` bigint DEFAULT '0' COMMENT '商品品类ID 默认0',
  `lwh_image` varchar COMMENT '长宽高图片',
  PRIMARY KEY (`id`)
) DISTRIBUTE BY HASH(`id`) INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}'



CREATE TABLE `attendance_data_v2` (
  `staff_info_id` bigint NOT NULL DEFAULT '0' COMMENT '员工ID',
  `stat_date` date COMMENT '统计日期',
  `attendance_time` tinyint NOT NULL DEFAULT '0' COMMENT '有效出勤时间 (0 缺勤 5:出勤半天 10:出勤全天)',
  `display_data` varchar COMMENT '页面展示的数据',
  `display_data_detail` varchar COMMENT '页面显示的数据详情',
  `attendance_started_at` datetime COMMENT '上班打卡时间',
  `attendance_end_at` datetime COMMENT '下班打卡时间',
  `leave_time_type` smallint NOT NULL DEFAULT '0' COMMENT '(1:上午半天假 2:下午半天假，3:全天请假)',
  `lh_valid` tinyint NOT NULL DEFAULT '0' COMMENT 'LH是否有效(0:无效 1:有效)',
  `is_trial` tinyint NOT NULL DEFAULT '0' COMMENT '是否过了试用期 0:未过试用期 1:正式员工',
  `AL` tinyint NOT NULL DEFAULT '0' COMMENT '年假 ',
  `SL` tinyint NOT NULL DEFAULT '0' COMMENT '病假',
  `PL` tinyint NOT NULL DEFAULT '0' COMMENT '带薪事假',
  `OFF` tinyint NOT NULL DEFAULT '0' COMMENT '调休或休息日',
  `LW` tinyint NOT NULL DEFAULT '0' COMMENT '不带薪假期',
  `AB` tinyint NOT NULL DEFAULT '0' COMMENT '缺勤日期',
  `PH` tinyint NOT NULL DEFAULT '0' COMMENT '公休日起',
  `times1` smallint NOT NULL DEFAULT '0' COMMENT '1倍加班时长',
  `times1_5` smallint NOT NULL DEFAULT '0' COMMENT '1.5倍加班时长',
  `times3` smallint NOT NULL DEFAULT '0' COMMENT '3倍加班时长',
  `operator_count` int NOT NULL DEFAULT '0' COMMENT '员工业务操作量',
  `pickup_count` int NOT NULL DEFAULT '0' COMMENT '员工日揽件量',
  `delivery_count` int NOT NULL DEFAULT '0' COMMENT '员工日派件量',
  `leave_type` varchar DEFAULT '' COMMENT '请假类型',
  `CT` tinyint NOT NULL DEFAULT '0' COMMENT '公司培训假',
  `BT` tinyint NOT NULL DEFAULT '0' COMMENT '出差',
  `shift_start` varchar COMMENT '班次开始时间',
  `shift_end` varchar COMMENT '班次结束时间',
  `lh_date` date COMMENT 'lh日期',
  `lh_time` varchar DEFAULT '' COMMENT 'lh时间',
  `lh_plate_number` varchar DEFAULT '' COMMENT '',
  `job_title` int COMMENT '员工当日的job_title',
  `USL` tinyint NOT NULL DEFAULT '0' COMMENT '无薪病假',
  `sys_department_id` int COMMENT '员工当日部门信息',
  `sys_store_id` varchar COMMENT '员工当日网点',
  `other_UL` tinyint NOT NULL DEFAULT '0' COMMENT '不带薪产假',
  `BT_Y` tinyint DEFAULT '0' COMMENT '黄牌出差',
  `C19SL` tinyint DEFAULT '0' COMMENT '带薪病假(新冠治疗)',
  `ISL` tinyint DEFAULT '0' COMMENT '隔离假',
  `BT_FEEDER_A` tinyint DEFAULT '0' COMMENT 'Feeder A 出差',
  `logic_enum` varchar DEFAULT '' COMMENT '逻辑标识',
  `node_department_id` int COMMENT '员工当日所属部门信息',
  `state` smallint DEFAULT '1' COMMENT '员工状态 1 在职 2 离职 3 停职',
  PRIMARY KEY (`staff_info_id`,`stat_date`)
) DISTRIBUTE BY HASH(`staff_info_id`) INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}';


CREATE TABLE `hr_overtime` (
  `overtime_id` bigint NOT NULL AUTO_INCREMENT COMMENT '加班id',
  `serial_no` varchar COMMENT '序列号',
  `staff_id` int COMMENT '员工id',
  `type` int COMMENT '加班类型1=工作日，2=节假日加班，3=晚班 4-节假日正常上班',
  `start_time` datetime COMMENT '开始时间',
  `end_time` datetime COMMENT '结束时间',
  `reason` varchar COMMENT '原因',
  `state` int COMMENT '状态：1=待审批，2=已同意，3=驳回 4=撤销',
  `higher_staff_id` varchar DEFAULT '0' COMMENT '上级id',
  `created_at` timestamp DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp DEFAULT CURRENT_TIMESTAMP,
  `duration` decimal(4, 2) COMMENT '时长',
  `reject_reason` varchar COMMENT '驳回原因',
  `is_push` int DEFAULT '0' COMMENT '是否推送，默认0=未推送，1=已推送',
  `is_anticipate` tinyint NOT NULL DEFAULT '1' COMMENT '0- 非预申请（后补申请） 1- 预申请',
  `date_at` date COMMENT '申请日期',
  `sub_type` tinyint DEFAULT '0' COMMENT '0-默认 1--1倍日薪 2--1.5倍日薪 3--3倍日薪 4--可调休',
  `wf_role` varchar COMMENT '审批流rolename',
  `references` varchar COMMENT '参考数据',
  `is_delay` tinyint DEFAULT '0' COMMENT '是否延时审批 1是 0 否',
  `approver_id` bigint COMMENT '审批人',
  `approver_name` varchar COMMENT '审批人名称',
  `time_type` tinyint DEFAULT '0' COMMENT '多班次打卡 默认0 没用到 1 前半天 2 后半天 3 全天',
  `detail_data` varchar DEFAULT '' COMMENT '详情页展示数据用',
  `salary_state` tinyint DEFAULT '0' COMMENT '薪酬计算规则是否符合条件 关联',
  PRIMARY KEY (`overtime_id`)
) DISTRIBUTE BY HASH(`overtime_id`) INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}'

CREATE TABLE `delivery_rollback_order` (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '销退单主键',
  `external_order_sn` varchar DEFAULT '' COMMENT '外部订单编号',
  `external_user_info` varchar DEFAULT '' COMMENT '用户在电商平台的账号(用户ID)',
  `back_sn` varchar NOT NULL DEFAULT '' COMMENT '销退订单号',
  `order_source_type` tinyint NOT NULL DEFAULT '1' COMMENT '[1]人工录入 [2]批量导入 [3]接口获取 [4]系统生成',
  `delivery_order_id` int NOT NULL COMMENT '发货单ID',
  `delivery_box_id` int NOT NULL DEFAULT '0' COMMENT '箱单ID',
  `seller_id` int NOT NULL COMMENT '货主ID',
  `store_id` int NOT NULL DEFAULT '0' COMMENT '店铺ID',
  `creator_id` int NOT NULL COMMENT '创建者ID',
  `auditor_id` int COMMENT '审核人ID',
  `register_id` int COMMENT '登记人ID',
  `delivery_sn` varchar COMMENT '发货单单号delivery_order表',
  `express_sn` varchar COMMENT '运单单号delivery_order表',
  `back_type` varchar DEFAULT 'primary' COMMENT '[primary]普通退货 [backgoods]退货换货 [allRejected]全部拒收[package]包裹销退,[crossBorder]跨境订单,[interceptCrossBorder]拦截跨境销退',
  `warehouse_id` int NOT NULL COMMENT '退回仓库ID 默认原发货单仓库 可改',
  `backpay_mode` varchar DEFAULT 'bank' COMMENT '付款方式退款方式---bank-银行转账--online-在线支付--cod-货到付款',
  `bank_name` varchar COMMENT '退款银行name',
  `back_status` int DEFAULT '1' COMMENT '退款状态 默认1  1：待退款 2：已退款 3：不退款',
  `payee` varchar COMMENT '退款的收款人',
  `bank_id` varchar COMMENT '退款银行的银行账号',
  `back_man` varchar COMMENT '退货人',
  `back_man_phone` varchar COMMENT '退货人电话',
  `countries` varchar NOT NULL DEFAULT 'th' COMMENT '国家',
  `province` varchar NOT NULL DEFAULT '' COMMENT '省',
  `city` varchar NOT NULL DEFAULT '' COMMENT '市',
  `district` varchar NOT NULL DEFAULT '' COMMENT '区',
  `code` varchar NOT NULL DEFAULT '' COMMENT '编号',
  `postal_code` varchar NOT NULL DEFAULT '' COMMENT '邮编',
  `consignee_address` varchar NOT NULL DEFAULT '' COMMENT '详细地址',
  `back_reason` int DEFAULT '1' COMMENT '退货原因 1：7天无理由 2：质量问题 3：过期 4：破损 5：其他',
  `back_price` bigint DEFAULT '0' COMMENT '退货金额',
  `back_express_price` bigint DEFAULT '0' COMMENT '退货运费金额',
  `real_express_price` bigint DEFAULT '0' COMMENT '用于计费的退货运费',
  `kinds_num` int NOT NULL DEFAULT '0' COMMENT '品种数量',
  `goods_num` bigint NOT NULL DEFAULT '0' COMMENT '商品数量',
  `goods_in_num` bigint NOT NULL DEFAULT '0' COMMENT '商品收货数量',
  `seller_remark` varchar COMMENT '货主备注',
  `buyer_remark` varchar DEFAULT '' COMMENT '买家备注',
  `warehouse_remark` varchar COMMENT '仓库备注',
  `back_express_sn` varchar COMMENT '退货快递单号 货主填',
  `back_express_complete_status` varchar NOT NULL DEFAULT 'n' COMMENT '销退单快递信息是否完成状态',
  `back_express_status` tinyint NOT NULL DEFAULT '0' COMMENT '销退单路由跟踪状态[0]没有状态 [10]配送中 [20]异常 [30]拒收 [40]签收',
  `back_express_status_remark` varchar NOT NULL DEFAULT '' COMMENT '修改物流状态为异常的时候，需要补充备注',
  `ori_express_sn` varchar DEFAULT '' COMMENT '原运单号',
  `total_weight` int DEFAULT '0' COMMENT '订单总量(g)',
  `total_size` int DEFAULT '0' COMMENT '订单尺寸(mm)',
  `logistic_company_id` int DEFAULT '0' COMMENT '物流公司ID',
  `back_express_name` varchar COMMENT '退货快递公司名字 货主填',
  `status` int DEFAULT '1020' COMMENT '销退单状态',
  `audit_time` datetime COMMENT '货主审核时间',
  `delivery_back_time` datetime COMMENT '货主寄回时间',
  `complete_time` datetime COMMENT '收货完成时间',
  `complete_id` int COMMENT '收货完成人',
  `created` timestamp DEFAULT CURRENT_TIMESTAMP COMMENT '',
  `modified` timestamp DEFAULT CURRENT_TIMESTAMP COMMENT '',
  `check_in_id` int NOT NULL DEFAULT '0' COMMENT '登记人ID',
  `check_in_time` timestamp DEFAULT CURRENT_TIMESTAMP COMMENT '登记时间',
  `api_source_from` tinyint NOT NULL DEFAULT '1' COMMENT '调用api的来源 默认1 正常调用 2 仓库端',
  `from_order_type` tinyint DEFAULT '1' COMMENT '来源类型 1发货单 2 出库单',
  `goods_error` tinyint NOT NULL DEFAULT '0' COMMENT '商品异常 1：商品异常',
  `other_warehouse_status` smallint NOT NULL DEFAULT '0' COMMENT '外部仓库状态 0 未下发 1下发成功 2 下发失败 3取消下发失败',
  `cancel_order_reason` varchar COMMENT '取消原因',
  `shelf_status` bigint DEFAULT '0' COMMENT '1070 上架中 1080 上架完成',
  `is_fast_inspection` tinyint DEFAULT '0' COMMENT '是否快速质检增加：[1]是',
  `close_reason` varchar DEFAULT '' COMMENT '销退单异常关闭原因',
  `arrival_time` datetime COMMENT '到货时间',
  `arrival_user` bigint DEFAULT '0' COMMENT '到货人',
  `channel_source` varchar DEFAULT '' COMMENT '渠道来源',
  `post_back_status` int DEFAULT '0' COMMENT '记录下已回传的状态只有正向',
  `add_source` tinyint DEFAULT '1' COMMENT '添加来源,[1]scm,[2]wms',
  `transport_mode` smallint DEFAULT '1' COMMENT '1:送货到仓 2:上门揽收',
  `shelf_end_time` datetime COMMENT '上架时间',
  `goods_status` varchar DEFAULT '' COMMENT 'normal:正品, bad:残品',
  `url` varchar DEFAULT '' COMMENT '附件地址',
  `close_time` datetime COMMENT '关闭时间',
  `close_name` varchar DEFAULT '' COMMENT '关闭人姓名',
  PRIMARY KEY (`id`)
) DISTRIBUTE BY HASH(`id`) INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}'


CREATE TABLE `delivery_rollback_order_register` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `warehouse_id` int NOT NULL COMMENT 'FK:warehouse_id,仓库ID',
  `status` int NOT NULL COMMENT '[1045]已到货,[1060]收货完成,[1061]异常标记',
  `express_sn` varchar NOT NULL COMMENT '退货快递单号',
  `seller_id` int NOT NULL DEFAULT '0' COMMENT '货主ID',
  `delivery_rollback_order_id` bigint NOT NULL DEFAULT '0' COMMENT '销退单id',
  `back_sn` varchar NOT NULL DEFAULT '' COMMENT '销退单SN',
  `delivery_order_id` bigint NOT NULL DEFAULT '0' COMMENT '发货单ID',
  `delivery_sn` varchar NOT NULL DEFAULT '' COMMENT '发货单SN',
  `abnormal_desc` varchar NOT NULL DEFAULT '' COMMENT '异常描述',
  `creator` int COMMENT '收货完成人',
  `created` datetime DEFAULT CURRENT_TIMESTAMP,
  `completer` int DEFAULT '0' COMMENT '完成时间',
  `completed` datetime COMMENT '完成时间',
  `modified` datetime DEFAULT CURRENT_TIMESTAMP,
  `remark` varchar DEFAULT '' COMMENT '备注',
  `type` tinyint DEFAULT '0' COMMENT '业务类型,[0]未知：默认未知,[1]普通销退,[2]包裹销退,[3]认证仓退件;这个逻辑不对等销退表，产品需要展示使用',
  `shop_id` varchar DEFAULT '' COMMENT '产品定义的shopID透传使用',
  `seller_platform_source_id` bigint DEFAULT '0' COMMENT 'shop_id 对应的平台ID',
  `order_sn` varchar DEFAULT '' COMMENT 'TT外部导入的单号',
  `register_sn` varchar DEFAULT '' COMMENT '登记表的SN',
  `adopt_status` tinyint DEFAULT '0' COMMENT ' 认领状态,[0]未认领，[1]确认中,[2] 已认领 ,[3]认领失败',
  `is_close_warning` tinyint DEFAULT '0' COMMENT '是否关闭预警,[0]否,[1]是',
  PRIMARY KEY (`id`)
) DISTRIBUTE BY HASH(`id`) INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}'

CREATE TABLE `rollback_order` (
  `id` bigint AUTO_INCREMENT,
  `warehouse_id` int COMMENT '仓库id',
  `seller_id` int COMMENT '客户id',
  `rollback_sn` varchar COMMENT '销退单号',
  `total_package_num` int DEFAULT '0' COMMENT '总包裹数',
  `source_type` smallint DEFAULT '3' COMMENT '单据来源 [1]接口获取[2]人工录入[3]仓库创建',
  `order_sn` varchar DEFAULT '' COMMENT '外部单号',
  `type` smallint DEFAULT '1' COMMENT '销退类型 [1]普通退货[2]退货换货[3]全部拒收[4]包裹销退',
  `status` int DEFAULT '1045' COMMENT '状态 [1030]等待买家寄回[1040]买家已寄回[1045]已到货[1050]收货中[1055]待客户审核[1056]已审核[1060]收货完成[1070]上架中[1080]上架完成[9000]异常关闭',
  `return_reason` smallint DEFAULT '6' COMMENT '退货原因[1]七天无理由[2]质量问题[3]过期[4]商品损坏[5]其他[6]买家拒收',
  `complete_time` datetime COMMENT '完成时间',
  `complete_id` int DEFAULT '0' COMMENT '完成人',
  `creator_id` int COMMENT '创建人id',
  `created` timestamp COMMENT '创建时间',
  `modified` timestamp COMMENT '更新时间',
  `receive_time` datetime COMMENT '收货完成时间',
  `receiver_id` int DEFAULT '0' COMMENT '收货人',
  `arrival_time` datetime COMMENT '到货时间',
  `arrival_id` int DEFAULT '0' COMMENT '到货人',
  `platform_rollback_sn` varchar DEFAULT '' COMMENT '三方系统单号，oms平台销退单号',
  PRIMARY KEY (`id`)
) DISTRIBUTE BY HASH(`id`) INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}' COMMENT='销退单'


CREATE TABLE `seller_goods_stock_snapshot` (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '',
  `date` date COMMENT '日期',
  `seller_id` int NOT NULL COMMENT 'FK:seller_id,货主ID',
  `seller_name` varchar DEFAULT '' COMMENT '货主名字',
  `seller_goods_id` int NOT NULL COMMENT 'FK:seller_goods id，商品主键ID',
  `warehouse_id` int NOT NULL COMMENT '仓库ID',
  `warehouse_name` varchar DEFAULT '' COMMENT '仓库名字',
  `total_inventory` bigint NOT NULL DEFAULT '0' COMMENT '实物库存',
  `inventory` bigint NOT NULL DEFAULT '0' COMMENT '可用库存',
  `occupy_inventory` bigint NOT NULL DEFAULT '0' COMMENT '占用库存',
  `wait_delivery_inventory` bigint NOT NULL DEFAULT '0' COMMENT '等待发货库存/出库占用量',
  `available_inventory` int NOT NULL DEFAULT '0' COMMENT '可售库存',
  `real_available` int NOT NULL DEFAULT '0' COMMENT '预占用分配时用到的可售库存',
  `preoccupied_inventory` bigint NOT NULL DEFAULT '0' COMMENT '预占用库存',
  `transfer_loading_num` bigint NOT NULL DEFAULT '0' COMMENT '正品调拨在途数量',
  `scrap_inventory` int NOT NULL DEFAULT '0' COMMENT '残品实物库存',
  `scrap_occupy_inventory` bigint NOT NULL DEFAULT '0' COMMENT '残品出库占用量',
  `scrap_available_inventory` bigint NOT NULL DEFAULT '0' COMMENT '残品可用库存',
  `scrap_transfer_loading_num` bigint DEFAULT '0' COMMENT '残品调拨在途数量',
  `created` timestamp DEFAULT CURRENT_TIMESTAMP COMMENT '',
  `modified` timestamp DEFAULT CURRENT_TIMESTAMP COMMENT '',
  `goods_name` varchar DEFAULT '' COMMENT '商品名称',
  `bar_code` varchar COMMENT '商品条码',
  `specification` varchar COMMENT '商品规格',
  `goods_code` varchar COMMENT '商品货号',
  `price` bigint COMMENT '售价',
  `volume` varchar DEFAULT '' COMMENT '商品体积(单位:mm³)',
  `is_shelf_life` smallint COMMENT '是否保质期',
  `is_sn` smallint COMMENT '是否sn',
  `sales_num` bigint DEFAULT '0' COMMENT '销售数量',
  `do_sales_num` bigint DEFAULT '0' COMMENT '发货单销量',
  PRIMARY KEY (`id`)
) DISTRIBUTE BY HASH(`id`) INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}'