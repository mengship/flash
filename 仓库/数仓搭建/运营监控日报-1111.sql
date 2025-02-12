
-- drop table if exists dwm.dws_th_ffm_operateMonitor_day;
-- create table dwm.dws_th_ffm_operateMonitor_day as
delete from dwm.dws_th_ffm_operateMonitor_day where 日期 = date_sub(date(now() + interval -1 hour),interval 1 day); -- 先删除数据
insert into dwm.dws_th_ffm_operateMonitor_day -- 再插入数据
select
    '' type
    ,date(t0.日期) 日期
    ,t0.仓库
    ,case t0.仓库 
		when 'BST' then 1	
		when 'AGV' then 2	
		when 'LAS' then 3	
		when 'BPL3' then 4	
		when 'BPL-Return' then 5
	end as asc1
    ,t0.在职人数
    ,t0.应出勤
    ,t0.实际出勤
    ,round(t0.实际出勤/nullif(t0.在职人数, 0), 4)  在职出勤率
    ,round(t0.实际出勤/NULLIF(t0.应出勤, 0), 4)  应出勤率
    ,t5.临时工时
    ,t0.加班时长
    ,t25.B2C出库单量
    ,NULLIF((((t0.实际出勤-t0.B2B实际出勤人数)*8 + ifnull(t0.加班时长,0) + ifnull(t5.临时工时,0))/8), 0)
	,cast(t25.B2C出库单量/NULLIF((((t0.在职人数-t0.B2B在职人数)*8 + ifnull(t0.加班时长,0) + ifnull(t5.临时工时,0))/8), 0) as DECIMAL(26,4)) as 在职人效单天人
	,cast(t25.B2C出库单量/NULLIF((((t0.实际出勤-t0.B2B实际出勤人数)*8 + ifnull(t0.加班时长,0) + ifnull(t5.临时工时,0))/8), 0)  as DECIMAL(26,4)) as 出勤人效单天人
	,cast(t25.B2C商品数量/NULLIF((((t0.在职人数-t0.B2B在职人数)*8 + ifnull(t0.加班时长,0) + ifnull(t5.临时工时,0))/8), 0)  as DECIMAL(26,4)) as 在职人效件天人
	,cast(t25.B2C商品数量/NULLIF((((t0.实际出勤-t0.B2B实际出勤人数)*8 + ifnull(t0.加班时长,0) + ifnull(t5.临时工时,0))/8), 0)  as DECIMAL(26,4)) as 出勤人效件天人

    ,t7.采购订单到货单量
    ,t7.销退订单到货单量
    ,t9.采购订单入库单量
    ,t9.销退订单入库单量
    ,t11.采购订单及时入库
    ,t11.销退订单及时入库
    ,t11.采购订单应入库
    ,t11.销退订单应入库
    ,t11.采购订单及时入库 / NULLIF(t11.采购订单应入库, 0) 采购订单入库及时率
    ,t11.销退订单及时入库 / NULLIF(t11.销退订单应入库, 0) 销退订单入库及时率
    ,t11.采购订单未及时入库
    ,t11.销退订单未及时入库
    ,t17.采购订单及时上架
    ,t17.销退订单及时上架
    ,t17.采购订单应上架
    ,t17.销退订单应上架
    ,t17.采购订单及时上架 / NULLIF(t17.采购订单应上架, 0) 采购订单上架及时率
    ,t17.销退订单及时上架 / NULLIF(t17.销退订单应上架, 0) 销退订单上架及时率
    ,t17.采购订单未及时上架
    ,t17.销退订单未及时上架
	,t23.B2C流入单量
    ,t23.B2C已审核单量
    ,t23.B2C未审核单量
	,t23.B2C预售单量
	,t23.B2C缺货单量
	,t23.B2B流入单量
    ,t23.B2B已审核单量
    ,t23.B2B未审核单量
    ,t25.B2C商品数量
    ,t25.B2C出库单量
    ,t25.B2C商品数量 / NULLIF(t25.B2C出库单量, 0) B2C件单比
    ,t26.B2B出库单量
    ,(t27.B2CShopee及时发货 + t27.B2CTikTok及时发货 + t27.B2CLAZADA及时发货 +t27.B2COther及时发货 ) / NULLIF((t27.B2CShopee应发货 + t27.B2CTikTok应发货 + t27.B2CLAZADA应发货 + t27.B2COther应发货), 0) B2C发货及时率
    ,t27.B2CShopee及时发货
    ,t27.B2CShopee应发货
    ,t27.B2CShopee未及时发货
    ,t27.B2CShopee及时发货 / NULLIF(t27.B2CShopee应发货, 0) B2CShopee发货及时率
    ,t27.B2CTikTok及时发货
    ,t27.B2CTikTok应发货
    ,t27.B2CTikTok未及时发货
    ,t27.B2CTikTok及时发货 / NULLIF(t27.B2CTikTok应发货, 0) B2CTikTok发货及时率
    ,t27.B2CLAZADA及时发货
    ,t27.B2CLAZADA应发货
    ,t27.B2CLAZADA未及时发货
    ,t27.B2CLAZADA及时发货 / NULLIF(t27.B2CLAZADA应发货, 0) B2CLAZADA发货及时率
    ,t27.B2COther及时发货
    ,t27.B2COther应发货
    ,t27.B2COther未及时发货
    ,t27.B2COther及时发货 / NULLIF(t27.B2COther应发货, 0) B2COther发货及时率
    ,t27.B2CShopee未及时发货 + t27.B2CTikTok未及时发货 + t27.B2CLAZADA未及时发货 + t27.B2COther未及时发货 未及时发货
    ,(t28.B2CShopee及时打包 + t28.B2CTikTok及时打包 + t28.B2CLAZADA及时打包 + t28.B2COther及时打包) / NULLIF((t28.B2CShopee应打包 + t28.B2CTikTok应打包 + t28.B2CLAZADA应打包 + t28.B2COther应打包), 0) B2C打包及时率
    ,t28.B2CShopee及时打包
    ,t28.B2CShopee应打包
    ,t28.B2CShopee未及时打包
    ,t28.B2CShopee及时打包 / NULLIF(t28.B2CShopee应打包, 0) B2CShopee打包及时率
    ,t28.B2CTikTok及时打包
    ,t28.B2CTikTok应打包
    ,t28.B2CTikTok未及时打包
    ,t28.B2CTikTok及时打包 / NULLIF(t28.B2CTikTok应打包, 0) B2CTikTok打包及时率
    ,t28.B2CLAZADA及时打包
    ,t28.B2CLAZADA应打包
    ,t28.B2CLAZADA未及时打包
    ,t28.B2CLAZADA及时打包 / NULLIF(t28.B2CLAZADA应打包, 0) B2CLAZADA打包及时率
    ,t28.B2COther及时打包
    ,t28.B2COther应打包
    ,t28.B2COther未及时打包
    ,t28.B2COther及时打包 / NULLIF(t28.B2COther应打包, 0) B2COther打包及时率
    ,t28.B2CShopee未及时打包 + t28.B2CTikTok未及时打包 + t28.B2CLAZADA未及时打包 + t28.B2COther未及时打包 未及时打包
    ,t28.B2B及时发货
    ,t28.B2B应发货
    ,t28.B2B未及时发货
    ,t28.B2B未及时打包	
    ,t28.B2B及时打包 / NULLIF(t28.B2B应打包, 0) B2B打包及时率	
    ,t28.B2B及时发货 / NULLIF(t28.B2B应发货, 0) B2B发货及时率
	,t30.货主数
	,t30.7D活跃货主
	,t30.14D活跃货主
	,t30.SKU数
	,t30.小件
	,t30.中件
	,t30.大件
	,t30.超大件
	,t30.信息不全
	,t30.其他
	,t30.正品库存
	,t30.小件库存
	,t30.中件库存
	,t30.大件库存
	,t30.超大件库存
	,t30.信息不全库存
	,t30.其他库存	
	,t30.残品库存
	,t30.拣选区一品一位
	,t30.拣选区一位一品	
	,t30.拣选区SKU覆盖率	
	,t30.规划库位
	,t30.规划库位_轻型货架
	,t30.规划库位_地堆
	,t30.规划库位_高位货架
	,t30.总使用库位
	,t30.高位货架使用库位
	,t30.轻型货架使用库位
	,t30.地堆使用库位	
	,t30.库位利用率
	,t30.轻型货架库位利用率
	,t30.地堆库位利用率
	,t30.高位货架库位利用率
	,t30.规划库容
	,t30.规划库容_轻型货架
	,t30.规划库容_地堆
	,t30.规划库容_高位货架
	,t30.总使用库容
	,t30.高位货架使用库容
	,t30.轻型货架使用库容
	,t30.地堆使用库容
	,t30.库容利用率
	,t30.轻型货架库容利用率
	,t30.地堆库容利用率	
	,t30.高位货架库容利用率	
	,t31.生成拦截单量
	,t31.完成拦截单量
	,t31.拦截归位及时率（24H）
	,t31.拦截单超时未完结
	,t32.生成异常单量
	,t32.完成异常单量
	,t32.及时率（24H）
	,t32.异常单超时未完结
	,t33.生成工单
	,t33.有责客诉量
	,t33.无责客诉量
	,t33.有责客诉量/t25.B2C出库单量 as 客诉成立率
	,t33.处理及时率
    ,t34.amount / NULLIF(t35.B2Chandovercnt, 0) 单均成本
    ,t36.amount / NULLIF(t25.B2C出库单量, 0) 单均操作费
    ,(t36.amount / NULLIF(t25.B2C出库单量, 0) - t34.amount / NULLIF(t35.B2Chandovercnt, 0)) / NULLIF((t34.amount / NULLIF(t35.B2Chandovercnt, 0)), 0) 人力损溢率

from
-- 出勤情况 已固化到dwm
dwm.dwm_th_ffm_staff_day t0
left join
( -- 临时工时
    select 
        left(dt,10) 日期
        ,warehouse 仓库
        ,'临时工工时' type
        ,sum(num_people*8+num_ot) 临时工时
    FROM dwm.th_ffm_tempworker_input WHERE left(dt,10)>=left(NOW() - interval 7 day,10)
        and warehouse is not null
    GROUP BY 1,2,3
) t5 on t0.日期 = t5.日期 and t0.仓库 = t5.仓库

-- 入库 已经固化到dwm层
-- 采购订单 销退订单 到货单量
left join dwm.dwm_th_ffm_arrivalnoticereg_day t7 on t0.日期 = t7.日期 and t0.仓库 = t7.仓库

-- 采购订单 销退订单 入库单量
left join dwm.dwm_th_ffm_arrivalnoticein_day t9 on t0.日期 = t9.日期 and t0.仓库 = t9.仓库
-- 采购订单 销退订单 及时入库 应入库 未及时入库
left join 
(
    select
        日期
        ,仓库
        ,TYPE
        ,采购订单及时入库
        ,销退订单及时入库
        ,采购订单应入库
        ,销退订单应入库
        ,采购订单未及时入库
        ,销退订单未及时入库
    from
    dwm.dwm_th_ffm_arrivalnoticetimely_day 
    where '24h及时入库'=TYPE
)t11 on t0.日期 = t11.日期 and t0.仓库 = t11.仓库

-- 采购订单 销退订单 及时上架 应上架 未及时上架
left join 
(
    select
        日期
        ,仓库
        ,TYPE
        ,采购订单及时入库 采购订单及时上架
        ,销退订单及时入库 销退订单及时上架
        ,采购订单应入库 采购订单应上架
        ,销退订单应入库 销退订单应上架
        ,采购订单未及时入库 采购订单未及时上架
        ,销退订单未及时入库 销退订单未及时上架
    from
    dwm.dwm_th_ffm_arrivalnoticetimely_day 
    where '48h及时上架'=TYPE
)t17 on t0.日期 = t17.日期 and t0.仓库 = t17.仓库

-- 出库
-- B2C  已审核单量 未审核单量
left join dwm.dwm_th_ffm_orderaudit_day t23 on t0.日期 = t23.日期 and t0.仓库 = t23.warehouse_name

-- B2C 商品数量 出库单量 固化到 dwm
left join dwm.dwm_th_ffm_orderout_day t25 on t0.日期 = t25.日期 and t0.仓库 = t25.warehouse_name

-- B2B 打包 单量
left join dwm.dwm_th_ffm_orderpack_day t26 on t0.日期 = t26.日期 and t0.仓库 = t26.warehouse_name

-- shopee TikTok LAZADA Other B2C 及时发货 应发货 未及时发货
left join 
(
    select
        日期
        ,warehouse_name
        ,指标
        ,B2CShopee及时打包 B2CShopee及时发货
        ,B2CShopee应打包 B2CShopee应发货
        ,B2CShopee未及时打包 B2CShopee未及时发货
        ,B2CTikTok及时打包 B2CTikTok及时发货
        ,B2CTikTok应打包 B2CTikTok应发货
        ,B2CTikTok未及时打包 B2CTikTok未及时发货
        ,B2CLAZADA及时打包 B2CLAZADA及时发货
        ,B2CLAZADA应打包 B2CLAZADA应发货
        ,B2CLAZADA未及时打包 B2CLAZADA未及时发货
        ,B2COther及时打包 B2COther及时发货
        ,B2COther应打包 B2COther应发货
        ,B2COther未及时打包 B2COther未及时发货
        ,B2B及时打包
        ,B2B应打包
        ,B2B未及时打包
        ,B2B及时发货
        ,B2B应发货
        ,B2B未及时发货
    from
    dwm.dwm_th_ffm_ordertimelyout_day
    where '及时发货'=指标
) t27 on t0.日期 = t27.日期 and t0.仓库 = t27.warehouse_name

-- shopee TikTok LAZADA Other B2C 及时打包 应打包 未及时打包
left join 
(
    select
        日期
        ,warehouse_name
        ,指标
        ,B2CShopee及时打包
        ,B2CShopee应打包
        ,B2CShopee未及时打包
        ,B2CTikTok及时打包
        ,B2CTikTok应打包
        ,B2CTikTok未及时打包
        ,B2CLAZADA及时打包
        ,B2CLAZADA应打包
        ,B2CLAZADA未及时打包
        ,B2COther及时打包
        ,B2COther应打包
        ,B2COther未及时打包
        ,B2B及时打包
        ,B2B应打包
        ,B2B未及时打包
        ,B2B及时发货
        ,B2B应发货
        ,B2B未及时发货
    from
    dwm.dwm_th_ffm_ordertimelyout_day
    where '及时打包'=指标
) t28 on t0.日期 = t28.日期 and t0.仓库 = t28.warehouse_name

-- B2B 及时打包 应打包 未及时打包
left join dwm.dwm_th_ffm_sellergoodslocation_day t30 on t0.日期 = t30.statc_date and t0.仓库 = t30.warehouse_name

-- 拦截单
left join 
(
    select
    statc_date
    ,warehouse_name
    ,title
    ,生成拦截单量
    ,完成拦截单量
    ,拦截归位及时率（24H）
    ,超时未完结 拦截单超时未完结
    from
    dwm.dwm_th_ffm_intercept_abnormaltimely_day
    where '拦截单'=title
) t31 on t0.日期 = t31.statc_date and t0.仓库 = t31.warehouse_name

-- 异常单
left join 
(
    select
    statc_date
    ,warehouse_name
    ,title
    ,生成拦截单量 生成异常单量
    ,完成拦截单量   完成异常单量
    ,拦截归位及时率（24H） 及时率（24H）
    ,超时未完结 异常单超时未完结
    from
    dwm.dwm_th_ffm_intercept_abnormaltimely_day
    where '异常单'=title
) t32 on t0.日期 = t32.statc_date and t0.仓库 = t32.warehouse_name

-- 客诉
left join dwm.dwm_th_ffm_complaint_day t33 on t0.日期 = t33.日期 and t0.仓库 = t33.仓库名称

-- 成本
left join 
(
    select
        日期
        ,周
        ,仓库名称
        ,sum(amount) amount
    from
    dwm.dwm_th_ffm_cost_day 
    where 收入类型2 <> '管理成本'
    group by 1,2,3
)
t34 on t0.日期 = t34.日期 and t0.仓库 = t34.仓库名称

-- 出库 交接日期汇总
left join dwm.dwm_th_ffm_orderhandover_day t35 on t0.日期 = t35.dt and t0.仓库 = t35.warehouse_name

-- 收入 操作费
left join dwm.dwm_th_ffm_income_day t36 on t0.日期 = t36.日期 and t0.仓库 = t36.仓库名称

where t0.日期 = date_sub(date(now() + interval -1 hour),interval 1 day)