-- 账单审核情况商务已审 数据未审

select 
    distinct billing_sn  
    ,s.name
    ,dd.billing_name 账单名称
    ,a.business_audit_time as 商务审核时间
    ,a.accounts_receivable/100 as 账单金额
    ,if( type=1,"仓储费","快递费") as type 
    ,date(a.business_audit_time) dd,date(a.data_audit_time)
from wms_production.billing  as a
left join 
(
    select 
        a.billing_name
        ,status
        ,b.seller_id 
    from warehouse_billing_rules as a 
    left join warehouse_billing_rules_ref  as b on a.id=b.warehouse_billing_rules_id
    where a.status =3
) as dd on dd.seller_id=a.seller_id
left join wms_production.seller s on s.id=a.seller_id 
where 1=1
    and a.billing_end between date ('2024-09-01') and  date('2024-09-30')
    and data_auditor_id=0
    and a.accounts_receivable/100>=0
    and date(a.business_audit_time)>='2024-05-01' 
    and a.status not in (0,50);

-- 整体业务单量
select 
    a.warehouse_name
    ,a.seller_name
    ,a.comment
    ,a. months
    ,a.type
    ,a.count_id as 本期单量
    ,number as 本期件量
from 
(
    select 
        b.name as warehouse_name
        ,a.name as seller_name
        ,kk.months
        ,kk.type
        ,count(distinct kk.id) as count_id
        ,a.comment
        ,sum( number) number
    from 
    (
        select 
            a.seller_id,
            a.warehouse_id,
            a.id ,
            '入库单' as type,
            date_format(a.complete_time,'%y#%m') as months, ba.in_num  number
        from wms_production.arrival_notice as a 
        left join wms_production.arrival_notice_goods ba  on a.id=ba.`arrival_notice_id` 
        where DATE(a.complete_time) between date('2024-09-01') and date('2024-09-30')
        union all
        (
            select 
                ba.seller_id,
                ba.warehouse_id,
                ba.id ,
                '销退单'as type,
                date_format(ba.complete_time,'%y#%m') as months,bb.`back_goods_in_number` number
            from wms_production.delivery_rollback_order ba
            left join `wms_production`.`delivery_rollback_order_goods` bb on ba.`id` =bb.`delivery_rollback_order_id` 
            where date(ba.complete_time) between date('2024-09-01') and date('2024-09-30')
        ) 
        union all 
        (
            select 
                d.seller_id,
                d.warehouse_id,
                d.id,
                '发货单'as type, 
                date_format(d.delivery_time,'%y#%m')as months,dd.`goods_number` as number
            from wms_production.delivery_order d
            left join `wms_production`.`delivery_order_goods` dd on  d.id=dd.`delivery_order_id` 
            where date(d.delivery_time) between date('2024-09-01') and date('2024-09-30')
        )
        union  all
        (
            select
                r.seller_id,
                r.warehouse_id,
                r.id,
                '出库单'as type,
                date_format(r.out_warehouse_time,'%y#%m')as months,rr.`out_num` as number
            from wms_production.return_warehouse  r
            left join `wms_production`.`return_warehouse_goods` as rr on r.id=rr.`return_warehouse_id` 
            where date(r.out_warehouse_time) between date('2024-09-01') and date('2024-09-30')  and type=1 
        )
        union all
        (
            select
                ip.seller_id,
                ip.warehouse_id,
                ip.id,
                '拦截单'as type,
                date_format(ip.shelf_on_end_time,'%y#%m')as months,ipp.had_on_num as number 
            from wms_production.intercept_place ip 
            left join `wms_production`.`intercept_place_goods`as ipp on ip.id=ipp.intercept_place_id
            where date(ip.shelf_on_end_time) between date('2024-09-01') and date('2024-09-30')
        )
        union all
        (
            select
                ac.seller_id,
                ac.warehouse_id,
                ac.id,
                '贴码单'as type,
                date_format(ac.mark_time,'%y#%m')as months,acc.`affixed_code_num`  as number 
            from wms_production.affixed_code ac 
            left join `wms_production`.`affixed_code_goods` as acc on ac.`id` =acc.`affixed_code_id`  
            where date(ac.mark_time) between date('2024-09-01') and date('2024-09-30')
        )
        union all
        (
            select
                seller_id,
                warehouse_id,
                id,
                '卸货费'as type,
                date_format(FROM_UNIXTIME(audit_time),'%y#%m')as months,volume/1000/1000/1000 as number
            from wms_production.load_unload_order 
            where date(FROM_UNIXTIME(audit_time))between date('2024-09-01') and date('2024-09-30') and type=2 
        )
        union all
        (
            select
                seller_id,
                warehouse_id,
                id,
                '装货费'as type,
                date_format(FROM_UNIXTIME(audit_time),'%y#%m')as months,volume/1000/1000/1000 as number
            from wms_production.load_unload_order 
            where date(FROM_UNIXTIME(audit_time)) between date('2024-09-01') and date('2024-09-30')  and type=1
        )
        union all 
        (
            select 
                ba.from_seller_id,
                ba.warehouse_id,
                ba.id ,
                '货权转出'as type,
                date_format(ba.complete_time,'%y#%m') as months,ba.actual_num number
            from wms_production.transfer_order ba
            where date(ba.complete_time) between date('2024-09-01') and date('2024-09-30')
        )
        union all 
        (
            select 
                ba.to_seller_id,
                ba.warehouse_id,
                ba.id ,
                '货权转入'as type,
                date_format(ba.complete_time,'%y#%m') as months,ba.actual_num number
            from wms_production.transfer_order ba
            where date(ba.complete_time) between date('2024-09-01') and date('2024-09-30')
        ) 
        union all 
        (
            select 
                ba.seller_id,
                ba.warehouse_id,
                ba.id ,
                '报废单'as type,
                date_format(ba.complete_time,'%y#%m') as months,ba.goods_num number
            from wms_production.destroy_order ba
            where date(ba.complete_time) between date('2024-09-01') and date('2024-09-30')
        )
    )  as kk
    left join wms_production.seller as a on kk.seller_id=a.id
    left join wms_production.warehouse as b on kk.warehouse_id=b.id
    group by 
    kk.months,
    kk.warehouse_id,
    kk.seller_id,
    kk.type,
    a.comment
) as a 
where 1=1 
and seller_name ='SMUZAPP'
order by months ,warehouse_name,seller_name,type asc

-- 退仓数据检查 暂时没用到
select 
    s.name
    ,a.seller_goods_id 
    ,l.location_code 
    ,sg.name as goods_name
    ,a.total_inventory
    ,a.date
    ,sg.length/1000*sg.width/1000*sg.height /1000*a.total_inventory  as volume
    ,sg.length/1000
    ,sg.width/1000
    ,sg.height /100
from  seller_goods_location_ref_snapshot  as a
left join seller s on a.seller_id =s.id
left join location l on a.location_id=l.id        
left join seller_goods sg on sg.id =a.seller_goods_id 
where a.date=date('2024-09-01')   and s.name ='${seller_name}';

-- 核对包材
select
    gg.delivery_sn,
    gg.good_name,
    num,
    b.container_sn,
    d.settlement_amount as 包材费,
    h.settlement_amount as 手工包材,
    d.billing_name_zh,
    f.name as 包材名,
    h.billing_name_zh,
    b.number,
    b.check_time
from
(
    select 
        gg.delivery_sn
        ,sum(gg.goods_number) num
        ,GROUP_CONCAT(distinct gg.name  , gg.goods_number separator '#' ) as good_name
        ,gg.id
        ,gg.seller_name
    from 
    (
        select 
            a.delivery_sn
            ,c.goods_number
            ,g.name
            ,s.name as seller_name
            ,a.id
            ,g.weight/1000 
        from wms_production.delivery_order as a
        left join wms_production.delivery_order_goods as c on c.delivery_order_id = a.id
        left join wms_production.seller_goods as g on g.id = c.seller_goods_id
        left join wms_production.seller s on a.seller_id=s.id
        where 1=1
            and s.name = 'SMUZAPP'
            and date(delivery_time) between date('2024-09-01') and date('2024-09-30')
        /* order by a.delivery_sn,g.name,c.goods_number */
    )gg 
    group by gg.delivery_sn,gg.id,gg.seller_name
)as gg
left join wms_production.container_order as b on gg.id = b.business_id
left join wms_production.container_inventory_log as g on g.container_order_id=b.id 
left join wms_production.container as f on f.id=g.container_id
left join 
(
    select 
        d.from_order_sn
        ,e.billing_name_zh 
        ,sum(settlement_amount/100) as settlement_amount
    from wms_production.billing_detail as d  -- 账单明细表
    left join billing_projects as e on e.id = d.billing_projects_id  
    where e.id  =10 -- 10 包材费 
        /* and d.charge_sn ='AR2409015451' */
    group by  d.from_order_sn,e.billing_name_zh
)as d on d.from_order_sn=gg.delivery_sn
left join  
(
    select 
        d.`business_sn`
        ,e.billing_name_zh 
        ,sum(settlement_amount/100) as settlement_amount
    from wms_production.billing_detail as d  -- 账单明细表
    left join billing_projects as e on e.id  =d.billing_projects_id  
    where e.id  =17 -- 手工包材
        /* and d.charge_sn ='AR2409015451' */
    group by  d.`business_sn` ,e.billing_name_zh
)as h on h.`business_sn`=gg.delivery_sn
order by gg.good_name


-- 仓库费按照库龄收费
select 
    seller_name 
    ,sum(charge)
    ,sum(volume )
    ,sum(volume_1)
from 
(
    select 
        seller_name
        ,age
        ,case when age='61-120' then sum(volume)*60
            when age='121-180' then sum(volume)*90
            when age='180以上' then sum(volume)*120
            else 0
            end charge
        ,sum(volume) volume 
        ,sum(volume)*1.3 volume_1
    from 
    (
        select 
            s.name seller_name
            ,a.seller_goods_id 
            ,a.date
            ,a.in_days
            ,sg.length/1000*sg.width/1000*sg.height /1000*a.inventory  as volume 
            ,case when a.in_days <=60 then '0-60天'
                when a.in_days between 61 and 120 then '61-120'
                when a.in_days between 121 and 180 then '121-180'
                else '180以上'
                end age
        from seller_goods_days_stock_snapshot  as a
        left join seller s on a.seller_id =s.id
        left join seller_goods sg on sg.id =a.seller_goods_id 
        left join wms_production.warehouse w on a.warehouse_id =w.id 
        where 1=1
        and a.date between date('2024-09-01') and  date('2024-09-30') 
        and s.name='LGF TiTe 提特' 
    )
    group by age,seller_name
)
group by seller_name;

-- 仓储费按照货位体积 货位面积 商品体积收费
select 
    a.seller_name
    ,a.mon
    ,a.goods_volume
    ,b.total_area as 货位面积
    ,b.total_volume as 货位体积
from 
(
    select 
        seller_name
        ,date as mon
        ,sum(volume) as goods_volume
    from 
    (
        select 
            s.name seller_name
            ,a.seller_goods_id 
            ,l.location_code 
            ,sg.name as goods_name
            ,a.total_inventory
            ,a.date
            ,sg.length/1000*sg.width/1000*sg.height /1000*a.total_inventory  as volume
            ,sg.length/1000
            ,sg.width/1000
            ,sg.height /100
        from  seller_goods_location_ref_snapshot  as a
        left join seller s on a.seller_id =s.id
        left join location l on a.location_id=l.id        
        left join seller_goods sg on sg.id =a.seller_goods_id 
        where a.date between date('2024-09-01') and date('2024-09-30')
            and s.name ='XiShengJi 喜苼记'
            and (sg.`length`is not null and  sg.width is not null and sg.height is not null )
    )
    group by seller_name,date
) as a
left join 
(
    select 
        a.date  mon
        ,s.name
        ,decode(l.location_attribute,  1 ,'小货架',2, '中货架',3 ,'大货架',4, '落地货架',5, '其他',location_attribute)货位属性
        ,sum(l.length / 1000 * l.width / 1000 * ( 100 + r.share_ratio ) / 100) AS total_area
        ,sum(l.length / 1000 * l.width / 1000 *l.height/ 1000 * ( 100 + r.share_ratio ) / 100) AS total_volume
    FROM
    (
        select 
            distinct date 
            ,location_id 
            ,seller_id 
        from 
        seller_goods_location_ref_snapshot 
        where date between date('2024-09-01') and date('2024-09-30') 
    ) as a
    left join         location l    on a.location_id=l.id        
    left join  wms_production.warehouse w on w.id=l.warehouse_id 
    left join         repository r  on l.repository_id = r.id 
    left join         seller as s   on s.id=a.seller_id 
    where r.use_attribute NOT IN ( 'temporary', 'waitingTemporary' )   
      and s.name ='XiShengJi 喜苼记'
    group by a.date,s.name
) as b
on a.mon=b.mon and a.seller_name=b.name
order by a.mon 
;

-- 货主合同发生变更逻辑
select
*
from
(
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
)

-- 虾米盒子 Xiami Box 操作费
select 
    sum(price)
from 
(
    select 
        a.delivery_sn
        ,num
        ,case when weight<1 and (length<100 and height<100 and height <100 ) then 1
            when weight<5 and (length<100 and height<100 and height <100 ) then 1.5
            when weight<30 and (length<100 and height<100 and height <100 ) then 4
            when weight<50 and (length<150 and height<150 and height <150 ) then 8
            else 0 end price 
    from 
    (
        select
            a.delivery_sn
            ,sum(c.goods_number) num
            , max(g.length/10) length 
            ,max(g.width/10) width
            , max(g.height /10)height
            ,SUM(g.weight*c.goods_number)  /1000 weight
        from
        wms_production.delivery_order as a
        left join wms_production.delivery_order_goods as c on c.delivery_order_id = a.id
        left join wms_production.seller_goods as g on c.seller_goods_id =g.id
        left join `wms_production`.seller s on s.id=a.`seller_id` 
        where  1=1
            and s.name ='LGF DuoJing 多镜'
            and date(a.delivery_time) 
        between date_sub(date_sub(date_format(now(),'%y-%m-%d'),interval extract(day from now())-1 day),interval 1 month) 
              and date_sub(date_sub(date_format(now(),'%y-%m-%d'),interval extract(day from now()) day),interval 0 month)
        group by a.delivery_sn
    )
)


-- LGF 京蛙JingWa 操作费
select 
    sum(price)
from 
(
    select 
        a.delivery_sn
        ,num
        ,case when weight<1 and (length<100 and height<100 and height <100 ) then 1
            when weight<5 and (length<100 and height<100 and height <100 ) then 1.5
            when weight<30 and (length<100 and height<100 and height <100 ) then 4
            when weight<50 and (length<150 and height<150 and height <150 ) then 8
            else 0 end price 
    from 
    (
        select
            a.delivery_sn
            ,sum(c.goods_number) num
            , max(g.length/10) length 
            ,max(g.width/10) width
            , max(g.height /10)height
            ,SUM(g.weight*c.goods_number)  /1000 weight
        from
        wms_production.delivery_order as a
        left join wms_production.delivery_order_goods as c on c.delivery_order_id = a.id
        left join wms_production.seller_goods as g on c.seller_goods_id =g.id
        left join `wms_production`.seller s on s.id=a.`seller_id` 
        where  s.name ='Local TiTe 提特'and date(a.delivery_time) between date('2024-09-01') and date('2024-09-30')
        group by a.delivery_sn
    )
)



-- lazada 计费卡账单核对 （包材另外核对）
-- 业务单量和账单关系
select 
    s.name
    ,be.type as 业务类型
    ,be.count_id 系统业务量 
    ,null 
    ,bill.*
    ,warehouse.charge
    ,decode(bill.billing_name_zh,'仓储费',if(abs(bill.amount-warehouse.charge)<=1,0,bill.amount-warehouse.charge),0) check_w
    ,bill.c_sn - be.count_id check_b
from wms_production.seller  s 
left join  
(
    select 
        kk.seller_id 
        ,kk.months
        ,kk.type
        ,count(kk.id) as count_id -- 件数
    from 
    (
        select 
            a.seller_id,
            a.id ,
            '入库费' as type,
            date_format(complete_time,'%y#%m') as months
        from wms_production.arrival_notice a 
        where DATE(complete_time) between date('2024-10-01')  and date('2024-10-31')
        union all
        (
            select 
                seller_id,
                id ,
                '销退入库费'as type,
                date_format(complete_time,'%y#%m') as months
            from wms_production.delivery_rollback_order 
            where date(complete_time) between date('2024-10-01')  and date('2024-10-31')
        ) 
        union all 
        (
            select 
            do.seller_id,
            do.id,
            '操作费'as type,
            date_format(do.delivery_time,'%y#%m')as months
            from wms_production.delivery_order do
            where date(delivery_time) between date('2024-10-01')  and date('2024-10-31')
        )
        union  all
        (
            select
            rw.seller_id,
            rw.id,
            '出库费'as type,
            date_format(out_warehouse_time,'%y#%m')as months
            from wms_production.return_warehouse rw
            where date(out_warehouse_time) between date('2024-10-01')  and date('2024-10-31') --  and type=1 -- 退供出库
        )
        union all
        (
            select
            seller_id,
            id,
            '拦截费'as type,
            date_format(shelf_on_end_time,'%y#%m')as months
            from wms_production.intercept_place-- 拦截
            where date(shelf_on_end_time) between date('2024-10-01')  and date('2024-10-31')
        )
        union all
        (
            select
            ac.seller_id,
            ac.id,
            '贴码单'as type,
            date_format(mark_time,'%y#%m')as months
            from wms_production.affixed_code ac -- 贴码单
            where date(mark_time) between date('2024-10-01')  and date('2024-10-31')
        )
        union all 
        (
            select 
            do.seller_id,
            do.id,
            '包材费'as type,
            date_format(do.delivery_time,'%y#%m')as months
            from wms_production.delivery_order do
            where date(delivery_time) between date('2024-10-01')  and date('2024-10-31')
        )
        union all  
        (
            select 
            ba.seller_id,
            ba.id ,
            '报废单'as type,
            date_format(ba.complete_time,'%y#%m') as months
            from wms_production.destroy_order ba
            where date(ba.complete_time) between date('2024-08-01') and date('2024-08-31'))
        ) as kk
    group by
    kk.seller_id,
    kk.months,
    kk.type
) as be    -- 收费业务单量
on s.id=be.seller_id
left join 
(
    select 
        a.seller_id 
        , sum(charge*volume *1.3) as charge
    from 
    (
        select 
            a.date
            ,a.seller_id,
            a.seller_goods_id 
            ,a.in_days
            ,sg.length/1000*sg.width/1000*sg.height /1000*a.inventory  as volume ,
            case when a.in_days <=60 then 0
            when a.in_days >60 and a.in_days <=120 then 2
            when a.in_days >120 and a.in_days <=180 then 3
            else 4
            end charge 
        from seller_goods_days_stock_snapshot  as a
        left join wms_production.seller_goods sg on sg.id =a.seller_goods_id 
        where a.date between  date('2024-10-01')  and date('2024-10-31') 
        and (sg.`length`is not null and  sg.width is not null and sg.height is not null )
    )
    group by a.seller_id 
) as warehouse -- lazada系列仓储费
on s.id=warehouse.seller_id
left join 
(
    select
        month (a.business_date) as months
        ,bill.business_audit_time
        ,bill_name.billing_name,
        a.seller_id ,
        bill.billing_sn  as 账单号,
        sum(settlement_amount/100) as amount,
        sum(adjustment_amount/100) as  adjustment_amount,
        count(a.business_sn) as c_sn,
        case when b.billing_name_zh like '%增值%' then '贴码单' 
            when b.billing_name_zh like '%出库%' then '出库费'
            when b.billing_name_zh like '%入库单%' then '入库费'
            when b.billing_name_zh like '%包材费%' then '包材费'
            else b.billing_name_zh  end billing_name_zh,
        bill.data_auditor_id
        from wms_production.billing_detail as a 
        left join wms_production.billing_projects as b on b.id=a.billing_projects_id
        left join wms_production.billing as bill on bill.billing_sn=a.charge_sn
        left join 
        (
            select 
                b.seller_id 
                ,a.billing_name
            from warehouse_billing_rules as a 
            left join warehouse_billing_rules_ref  as b on a.id=b.warehouse_billing_rules_id
            where a.status =3  
        ) as bill_name on a.seller_id=bill_name.seller_id
        where business_date between  date('2024-10-01')  and date('2024-10-31')
        and bill.status not in (0,50)
        and settlement_amount>=0 
        and bill.type=1 -- 仓储费
    group by  b.billing_name_zh,month (a.business_date),a.seller_id,bill_name.billing_name,bill.data_auditor_id 
) as bill on be.seller_id=bill.seller_id  and be.type=bill.billing_name_zh
where billing_name  in ('Lazada客户','上海遥饮信息技术有限公司 （Lazada客户）','安德') and business_audit_time is not null and data_auditor_id=0


-- 账单和业务单量关系
select 
    distinct 
    s.name
    ,be.type
    ,be.count_id
    ,null 
    ,bill.*
    ,warehouse.charge
    ,decode(bill.billing_name_zh,'仓储费',if(abs(bill.amount-warehouse.charge)<=5,0,bill.amount-warehouse.charge),0) check_w
    ,bill.c_sn -be.count_id check_b
from  
(
    select
        month (a.business_date) as months
        ,bill.business_audit_time
        ,bill_name.billing_name,
        a.seller_id ,
        bill.billing_sn  as 账单号,
        sum(settlement_amount/100) as amount,
        sum(adjustment_amount/100) as  adjustment_amount,
        count(a.business_sn) as c_sn,
        case when b.billing_name_zh like '%增值%' then '贴码单' 
        when b.billing_name_zh like '%出库%' then '出库费'
        when b.billing_name_zh like '%入库单%' then '入库费'
        when b.billing_name_zh like '%包材费%' then '包材费'
        else b.billing_name_zh  end billing_name_zh,
        bill.data_auditor_id
    from wms_production.billing_detail as a 
    left join wms_production.billing_projects as b on b.id=a.billing_projects_id
    left join wms_production.billing as bill on bill.billing_sn=a.charge_sn
    left join 
    ( 
        select 
            b.seller_id 
            ,a.billing_name
        from warehouse_billing_rules as a 
        left join warehouse_billing_rules_ref  as b on a.id=b.warehouse_billing_rules_id
        where a.status =3 
    ) as bill_name -- and  a.billing_name in ('安德','上海遥饮信息技术有限公司 （Lazada客户）','Lazada客户'))
    on a.seller_id=bill_name.seller_id
    where business_date between  date('2024-10-01')  and date('2024-10-31')
        and bill.status not in (0,50)
 -- and settlement_amount>0 
        and bill.type=1 -- 仓储费
    group by  b.billing_name_zh,month (a.business_date),a.seller_id,bill_name.billing_name,bill.data_auditor_id 
) as bill
left join  
(
    select 
        kk.seller_id 
        ,kk.months
        ,kk.type
        ,count(kk.id) as count_id -- 件数
    from 
    (
        select 
            a.seller_id,
            a.id ,
            '入库费' as type,
            date_format(complete_time,'%y#%m') as months
        from wms_production.arrival_notice a 
        where DATE(complete_time) between date('2024-10-01')  and date('2024-10-31')
        union all
        (
            select 
                seller_id,
                id ,
                '销退入库费'as type,
                date_format(complete_time,'%y#%m') as months
            from wms_production.delivery_rollback_order 
            where date(complete_time) between date('2024-10-01')  and date('2024-10-31')
        )
        union all 
        (
            select 
                do.seller_id,
                do.id,
                '操作费'as type,
                date_format(do.delivery_time,'%y#%m')as months
            from wms_production.delivery_order do
            where date(delivery_time) between date('2024-10-01')  and date('2024-10-31')
        )
        union  all
        (
            select
                rw.seller_id,
                rw.id,
                '出库费'as type,
                date_format(out_warehouse_time,'%y#%m')as months
            from wms_production.return_warehouse rw
            where date(out_warehouse_time) between date('2024-10-01')  and date('2024-10-31') --  and type=1 -- 退供出库
        )
        union all
        (
            select
                seller_id,
                id,
                '拦截费'as type,
                date_format(shelf_on_end_time,'%y#%m')as months
            from wms_production.intercept_place-- 拦截
            where date(shelf_on_end_time) between date('2024-10-01')  and date('2024-10-31')
        )
        union all
        (
            select
                ac.seller_id,
                ac.id,
                '贴码单'as type,
                date_format(mark_time,'%y#%m')as months
            from wms_production.affixed_code ac -- 贴码单
            where date(mark_time) between date('2024-10-01')  and date('2024-10-31')
        )
        union all 
        (
            select
                b.seller_id,
                b.id,
                '包材费'as type,
                date_format(b.created,'%y#%m')as months 
            from
            wms_production.container_order as b 
            where date(b.created) between date('2024-10-01')  and date('2024-10-31')
        )
        union all
        (
            select
                ba.seller_id,
                ba.id ,
                '报废单'as type,
                date_format(ba.complete_time,'%y#%m') as months
            from wms_production.destroy_order ba
            where date(ba.complete_time) between date('2024-08-01') and date('2024-08-31')
        )
    ) as kk
    group by
    kk.seller_id,
    kk.months,
    kk.type
) as be    -- 收费业务单量
on bill.seller_id=be.seller_id  and bill.billing_name_zh=be.type
left join 
(
    select 
        a.seller_id 
        , sum(charge*volume *1.3) as charge
    from 
    (
        select 
            a.date
            ,a.seller_id,
            a.seller_goods_id 
            ,a.in_days
            ,sg.length/1000*sg.width/1000*sg.height /1000*a.inventory  as volume ,
            case when a.in_days <=60 then 0
                when a.in_days >60 and a.in_days <=120 then 2
                when a.in_days >120 and a.in_days <=180 then 3
                else 4 end charge 
        from seller_goods_days_stock_snapshot  as a
        left join wms_production.seller_goods sg  on sg.id =a.seller_goods_id 
        where a.date between  date('2024-10-01')  and date('2024-10-31') 
        and (sg.`length`is not null and  sg.width is not null and sg.height is not null )
    )
    group by a.seller_id 
) as warehouse -- lazada系列仓储费
on bill.seller_id=warehouse.seller_id
left join wms_production.seller s on s.id=bill.seller_id
where billing_name  in ('Lazada客户','上海遥饮信息技术有限公司 （Lazada客户）','安德') and business_audit_time is not null and data_auditor_id=0


-- 操作费LZD 续约客户 
select 
    count(delivery_sn)
    ,count(delivery_sn)/(TIMESTAMPDIFF(DAY,'2024-08-01','2024-08-31')+1) 日均单量,
    case when count(delivery_sn)/(TIMESTAMPDIFF(DAY,'2024-08-01','2024-08-31')+1)<=200 then sum(price)
        when count(delivery_sn)/(TIMESTAMPDIFF(DAY,'2024-08-01','2024-08-31')+1)<=500 then sum(price)-count(delivery_sn)*0.7
        when count(delivery_sn)/(TIMESTAMPDIFF(DAY,'2024-08-01','2024-08-31')+1)>500 then sum(price)-count(delivery_sn)*1
    else 0 end charge
from 
(
    select
        a.delivery_sn
        ,sum(c.goods_number) num ,
        case when sum(c.goods_number)<=3 then 2.5 
            else 2.5+(sum(c.goods_number)-3)*0.3 end price 
    from
        wms_production.delivery_order as a
    left join wms_production.delivery_order_goods as c on c.delivery_order_id = a.id
    left join wms_production.seller_goods as g on c.seller_goods_id =g.id
    left join `wms_production`.seller s on s.id=a.`seller_id` 
    where 1=1
    and s.name ='LGF 微米斯WeiMiSi'
    and date(a.delivery_time) between date('2024-10-01') and date('2024-10-31')
    group by a.delivery_sn
);


-- 仓储费LZD 续约客户  
select 
    name,-- date as mon,
    sum(volume) as goods_volume,
    sum(volume) *1.3*2 as charge
from 
(
    select 
        s.name
        ,a.seller_goods_id 
        ,l.location_code 
        ,sg.name as goods_name,
        a.total_inventory
        ,a.date
        ,sg.length/1000*sg.width/1000*sg.height /1000*a.total_inventory  as volume,
        sg.length/1000
        ,sg.width/1000
        ,sg.height /100
    from  seller_goods_location_ref_snapshot  as a
    left join seller s on a.seller_id =s.id
    left join location l on a.location_id=l.id        
    left join seller_goods sg on sg.id =a.seller_goods_id 
    where 1=1
    and a.date between date('2024-10-01') and date('2024-10-31')
    and s.name ='LGF LeMei乐美'
    and (sg.`length`is not null and  sg.width is not null and sg.height is not null )
)
group by name-- ,date
