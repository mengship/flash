
-- 1 入库单量pcs

select
    '入库单量pcs'
    ,left(complete_date, 7) complete_month
    ,仓库名称
    ,TYPE
    ,sum(in_num) in_num
from
(
    -- 入库量   
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
        ,CASE   WHEN greatest(sg.LENGTH, sg.width, sg.height)<=250 AND sg.weight <= 3000 THEN '小件'
                WHEN greatest(sg.LENGTH, sg.width, sg.height)<=500 AND sg.weight <= 5000 THEN '中件'
                WHEN greatest(sg.LENGTH, sg.width, sg.height)<=1000 AND sg.weight <= 15000 THEN '大件'
                WHEN greatest(sg.LENGTH, sg.width, sg.height)>1000 OR sg.weight > 15000 THEN '超大件' 
                ELSE '信息不全' END TYPE
    FROM wms_production.arrival_notice an
    left join wms_production.arrival_notice_goods ang on an.id = ang.arrival_notice_id
    left join wms_production.seller_goods sg on sg.id =ang.seller_goods_id 
    left join (select receive_external_no,finish_date from was.inb_receive_bill where is_deleted=0 and create_time >= convert_tz(date_sub(date(now() + interval -1 hour),interval 130 day), '+07:00', '+08:00') )irb on an.notice_number = irb.receive_external_no
    left join wms_production.warehouse w ON an.warehouse_id=w.id
    left join wms_production.seller s on s.id = an.seller_id
        WHERE an.reg_time IS NOT NULL 
        AND an.status>='30'
        AND an.complete_time >= '2023-10-01'
        AND an.complete_time <  '2024-10-01'
        and s.name not in ('FFM-TH') -- , 'Flash -Thailand' 也算仓储的客户
) t0
  where 仓库名称 is not null
group by 1,2,3,4
order by 1,2,3,4;

-- 2 销退入库
select
    '销退入库单量pcs'
    ,left(收货完成日期, 7) complete_month
    ,仓库名称
    ,count(distinct 销退入库单号) order_num
    ,sum(goods_in_num_fixup) goods_in_num
    ,round(sum(goods_in_num_fixup) / count(distinct 销退入库单号), 2) pcsorder
from
(
    select 
        '销退入库pcs'
        ,case when w.name='AutoWarehouse' then 'AGV'
            when w.name='BPL-Return Warehouse' then 'BPL-Return'
            when w.name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
            when w.name='BangsaoThong' then 'BST'
            when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS' # ,'PMD-WH','BKK-WH-Ecommerce','BKK-WH-LAS物料仓'
            when w.name='LCP Warehouse' then 'LCP' end 仓库名称
        ,case dr.status 
            when 1000 then '取消退货'
            when 1001 then '已删除此销退单'
            when 1010 then '无需寄回直接退款'
            when 1020 then '等待审核'
            when 1030 then '审核完成'
            when 1040 then '买家已寄回'
            when 1045 then '已到货'
            when 1050 then '收货中'
            when 1060 then '收货完成'
            when 1070 then '上架中'
            when 1080 then '上架完成' 
            else dr.status end 状态
        ,case dr.shelf_status when 1070 then '上架中' when 1080 then '上架完成' end 上架状态
        ,dr.back_sn 销退入库单号
        ,dr.delivery_sn 原订单号
        ,dr.express_sn 原运单号
        ,case dr.order_source_type 
        when 1 then '人工录入' 
        when 2 then '批量导入' 
        when 3 then '接口获取' 
        when 4 then '系统生成'
        else dr.order_source_type end 订单来源
        ,dr.external_order_sn 外部单号
        ,s.name 货主
        ,case dr.back_type        
            when 'primary' then '普通退货' 
            when 'backgoods' then '退货换货' 
            when 'allRejected' then '全部拒收'
            when 'package' then '包裹销退'
            when 'crossBorder' then '跨境订单'
            when 'interceptCrossBorder' then '拦截跨境销退'
        else dr.back_type end 销退单类型
        ,dr.complete_time 收货完成时间
        ,date(dr.complete_time) 收货完成日期
        ,dr.goods_in_num
        ,if(dr.back_type='package', 1, goods_in_num) goods_in_num_fixup
    from wms_production.delivery_rollback_order dr
    left join wms_production.seller s on dr.seller_id = s.id
    left join wms_production.member m on dr.creator_id =m.id
    left join wms_production.warehouse w ON dr.warehouse_id=w.id
    where dr.complete_time>='2023-10-01'
        and dr.complete_time<'2024-10-01'
) t0
  where 仓库名称 is not null
group by 1,2,3
order by 1,2,3;

-- 3 2C 单
-- wms平台
select
    complete_month
    ,仓库名称
    ,order_num
    ,round(order_num / month_day_cnt, 2) avg_day
    ,round(goods_num/order_num, 2) pcsorder
from
(
    select
        '2C发货单'
        ,left(delivery_date, 7) complete_month
        ,仓库名称
        ,count(distinct delivery_sn) order_num
        ,sum(goods_num) goods_num
    from
    (
        SELECT 
            'B2C' TYPE
            ,do.delivery_sn
            ,goods_num 
            ,warehouse_id
            ,w.name warehouse_name
            ,case when w.name='AutoWarehouse' then 'AGV'
                when w.name='BPL-Return Warehouse' then 'BPL-Return'
                when w.name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
                when w.name='BangsaoThong' then 'BST'
                when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS' # ,'PMD-WH','BKK-WH-Ecommerce','BKK-WH-LAS物料仓'
                when w.name='LCP Warehouse' then 'LCP' end 仓库名称
            ,ps.`name` platform_source
            ,sl.`name` seller_name
            ,do.seller_id
            ,date_add(do.`created`, interval -60 minute) created_time
            ,left(date_add(do.`created`, interval -60 minute), 10) created_date
            ,date_add(do.`audit_time`, interval -60 minute) audit_time
            ,left(date_add(do.`audit_time`, interval -60 minute), 10) audit_date
            ,do.`succ_pick` pick_time
            ,do.`pack_time` pack_time
            ,do.`start_receipt ` handover_time
            ,date_add(do.`delivery_time`, interval -60 minute) delivery_time
            ,date(date_add(do.`delivery_time`, interval -60 minute)) delivery_date
            ,do.express_name
            ,do.operator_id out_operator
            ,case when do.`status` NOT IN ('1000','1010') and do.`platform_status` != 9 and do.prompt NOT in (1,2,3,4) and sl.name not in ('FFM-TH', 'Flash -Thailand') then 1
                else 0
                end as is_visible
            ,do.express_sn
        FROM `wms_production`.`delivery_order` do 
        LEFT JOIN `wms_production`.`seller_platform_source` sps on do.`platform_source_id`=sps.`id`
        LEFT JOIN `wms_production`.`platform_source` ps on sps.`platform_source_id`=ps.`id` 
        LEFT JOIN `wms_production`.`seller` sl on do.`seller_id`=sl.`id`
        LEFT JOIN wms_production.warehouse w ON do.warehouse_id=w.id
        WHERE 1=1
            and do.`delivery_time` >= convert_tz('2023-10-01', '+07:00', '+08:00')
            and sl.name not in ('FFM-TH') -- 剔除物料和资产
    )
    where 仓库名称 is not null
    group by 1,2,3
    order by 1,2,3
) t0
left join 
(
    select 
        left(date, 7) month 
        ,month_day_cnt 
    from 
    tmpale.ods_th_dim_date 
    where date>='2023-10-01'
        and date<'2024-10-01'
    group by 1,2
) date_month on t0.complete_month = date_month.month

-- 4 2C 件
select
    complete_month
    ,仓库名称
    ,TYPE
    ,out_num
    ,round(out_num / month_day_cnt, 2) avg_day
from
(
    select
        '2C单量pcs'
        ,left(delivery_date, 7) complete_month
        ,仓库名称
        ,TYPE
        ,sum(goods_number) out_num
    from
        (
            SELECT 
                'B2C' TYPE01
                ,do.delivery_sn
                ,goods_num 
                ,do.warehouse_id
                ,w.name warehouse_name
                ,case when w.name='AutoWarehouse' then 'AGV'
                    when w.name='BPL-Return Warehouse' then 'BPL-Return'
                    when w.name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
                    when w.name='BangsaoThong' then 'BST'
                    when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS' # ,'PMD-WH','BKK-WH-Ecommerce','BKK-WH-LAS物料仓'
                    when w.name='LCP Warehouse' then 'LCP' end 仓库名称
                ,ps.`name` platform_source
                ,sl.`name` seller_name
                ,do.seller_id
                ,date_add(do.`created`, interval -60 minute) created_time
                ,left(date_add(do.`created`, interval -60 minute), 10) created_date
                ,date_add(do.`audit_time`, interval -60 minute) audit_time
                ,left(date_add(do.`audit_time`, interval -60 minute), 10) audit_date
                ,do.`succ_pick` pick_time
                ,do.`pack_time` pack_time
                ,do.`start_receipt ` handover_time
                ,date_add(do.`delivery_time`, interval -60 minute) delivery_time
                ,date(date_add(do.`delivery_time`, interval -60 minute)) delivery_date
                ,do.express_name
                ,do.operator_id out_operator
                ,case when do.`status` NOT IN ('1000','1010') and do.`platform_status` != 9 and do.prompt NOT in (1,2,3,4) and sl.name not in ('FFM-TH', 'Flash -Thailand') then 1
                    else 0
                    end as is_visible
                ,do.express_sn
                ,CASE   WHEN greatest(sg.LENGTH, sg.width, sg.height)<=250 AND sg.weight <= 3000 THEN '小件'
                        WHEN greatest(sg.LENGTH, sg.width, sg.height)<=500 AND sg.weight <= 5000 THEN '中件'
                        WHEN greatest(sg.LENGTH, sg.width, sg.height)<=1000 AND sg.weight <= 15000 THEN '大件'
                        WHEN greatest(sg.LENGTH, sg.width, sg.height)>1000 OR sg.weight > 15000 THEN '超大件' 
                        ELSE '信息不全' END TYPE
                ,dog.goods_number
            FROM `wms_production`.`delivery_order` do 
            left join wms_production.delivery_order_goods dog on do.id = dog.delivery_order_id
            left join wms_production.seller_goods as sg on sg.id = dog.seller_goods_id
            LEFT JOIN `wms_production`.`seller_platform_source` sps on do.`platform_source_id`=sps.`id`
            LEFT JOIN `wms_production`.`platform_source` ps on sps.`platform_source_id`=ps.`id` 
            LEFT JOIN `wms_production`.`seller` sl on do.`seller_id`=sl.`id`
            LEFT JOIN wms_production.warehouse w ON do.warehouse_id=w.id
            WHERE 1=1
                and do.`delivery_time` >= convert_tz('2023-10-01', '+07:00', '+08:00')
                and sl.name not in ('FFM-TH') -- 剔除物料和资产
        ) t0
        where 仓库名称 is not null
    group by 1,2,3,4

) t0
left join 
(
    select 
        left(date, 7) month 
        ,month_day_cnt 
    from 
    tmpale.ods_th_dim_date 
    where date>='2023-10-01'
        and date<'2024-10-01'
    group by 1,2
) date_month on t0.complete_month = date_month.month
order by 1,2,3,4

-- 5 2B 件
select
    complete_month
    ,仓库名称
    ,TYPE
    ,out_num
    ,round(out_num / month_day_cnt, 2) avg_day
from
(
    select
        '入库单量pcs'
        ,left(delivery_date, 7) complete_month
        ,仓库名称
        ,TYPE
        ,sum(out_num) out_num
    from
    (
        SELECT 
            'B2B' TYPE0
            ,return_warehouse_sn
            ,total_goods_num
            ,do.warehouse_id
            ,case when w.name='AutoWarehouse' then 'AGV'
                when w.name='BPL-Return Warehouse' then 'BPL-Return'
                when w.name in ('BPL3-LIVESTREAM', 'BPL3- Bangphli3 Livestream Warehouse') then 'BPL3'
                when w.name='BangsaoThong' then 'BST'
                when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS' # ,'PMD-WH','BKK-WH-Ecommerce','BKK-WH-LAS物料仓'
                when w.name='LCP Warehouse' then 'LCP' end 仓库名称
            ,w.name warehouse_name
            ,'B2B'platform_source
            ,sl.name seller_name
            ,''seller_id
            ,date_add(do.`created`, interval -60 minute) created_time
            ,left(date_add(do.`created`, interval -60 minute), 10) created_date
            ,date_add(do.`verify_time`, interval -60 minute) audit_time
            ,left(date_add(do.`verify_time`, interval -60 minute), 10) audit_date
            ,do.picking_end_time
            ,do.pack_time + interval -1 hour pack_time
            ,date_add(do.`out_warehouse_time`, interval -60 minute) handover_time
            ,date_add(do.`out_warehouse_time`, interval -60 minute) delivery_time
            ,date(date_add(do.`out_warehouse_time`, interval -60 minute)) delivery_date
            ,if(locate('DO',do.express_sn)>0 or substring(do.express_sn,1,3)='LBX', 0, 1) is_time
            ,case 
                when locate('DO',do.express_sn)>0 then 'DO快递单号'
                when substring(do.express_sn,1,3)='LBX' then 'LBX快递单号'
                else '正常时效单'
                end as no_istime_type
            ,null is_tiktok
            ,null out_operator
            ,do.express_sn
            ,CASE   WHEN greatest(sg.LENGTH, sg.width, sg.height)<=250 AND sg.weight <= 3000 THEN '小件'
                    WHEN greatest(sg.LENGTH, sg.width, sg.height)<=500 AND sg.weight <= 5000 THEN '中件'
                    WHEN greatest(sg.LENGTH, sg.width, sg.height)<=1000 AND sg.weight <= 15000 THEN '大件'
                    WHEN greatest(sg.LENGTH, sg.width, sg.height)>1000 OR sg.weight > 15000 THEN '超大件' 
                    ELSE '信息不全' END TYPE
            ,rwg.out_num
        from  wms_production.return_warehouse do 
        left join wms_production.return_warehouse_goods rwg on do.id = rwg.return_warehouse_id
        left join wms_production.seller_goods sg on sg.id =rwg.seller_goods_id 
        LEFT JOIN wms_production.warehouse w ON do.warehouse_id=w.id
        LEFT JOIN `wms_production`.`seller` sl on do.`seller_id`=sl.`id`
        WHERE 1=1
            and do.`out_warehouse_time` >= convert_tz('2023-10-01', '+07:00', '+08:00')
            and sl.name not in ('FFM-TH') -- 剔除物料和资产
    ) t0
    where 仓库名称 is not null
    group by 1,2,3,4
) t0
left join 
(
    select 
        left(date, 7) month 
        ,month_day_cnt 
    from 
    tmpale.ods_th_dim_date 
    where date>='2023-10-01'
        and date<'2024-10-01'
    group by 1,2
) date_month on t0.complete_month = date_month.month
order by 1,2,3,4;

-- 6 人力成本 1 总成本 提成
select
    仓库名称,
    月份,
    sum(人力总成本) 人力总成本,
    sum(incentive) incentive
from
    (
        SELECT
            gz.一级部门,
            gz.二级部门,
            case when gz.二级部门='AGV Warehouse' then 'AGV'
              when gz.二级部门='Bangphli Livestream Warehouse' then 'BPL3'
              when gz.二级部门='BPL2-Bangphli Return Warehouse' then 'BPL-Return'
              when gz.二级部门='BST-Bang Sao Thong Warehouse' then 'BST'
              when gz.二级部门='LAS-Lasalle Material Warehouse' then 'LAS' 
              end 仓库名称,
            gz.excel_month as 月份,
            sum(gz.total_income) + sum(gz.social) + sum(gz.bonus) +sum(incentive) as 人力总成本,
            sum(incentive) incentive
        FROM
            (
                SELECT
                    sg.`excel_month`,
                    sg.staff_info_id,
                    sd.一级部门,
                    sd.二级部门,
                    sg.`bonus`, #年终奖 人力总成本的一部分
                    sg.`total_income`, -- 人力总成本的一部分
                    sg.`social` -- 人力总成本的一部分
                    ,ifnull(sg01.incentive, 0) incentive
                    ,left(date_add(concat(sg.excel_month, '-01'), interval 1 month), 7) excel_next_month
                FROM
                    `backyard_pro`.`salary_gongzi` sg
                    LEFT JOIN `backyard_pro`.`hr_staff_info` hsi on hsi.`staff_info_id` = sg.`staff_info_id`
                    LEFT JOIN dwm.`dwd_hr_organizational_structure_detail` sd on sd.`id` = hsi.`node_department_id`
                    left join (
                      select
                          excel_month
                          ,incentive
                          ,staff_info_id
                        FROM
                    `backyard_pro`.`salary_gongzi` sg
                    ) sg01 on sg.left(date_add(concat(sg.excel_month, '-01'), interval 1 month), 7) = sg01.excel_month and sg.staff_info_id = sg01.staff_info_id
                WHERE
                    sg.excel_month >= '2023-10'
                    AND sg.`company_id` = 2
                    AND sd.一级部门 = 'Thailand Fulfillment'
            ) gz
        GROUP BY
            1,
            2,
            3,
            4
    ) t0
  where 仓库名称 is not null
group by
    1,
    2
order by 1,2;


-- 6 人力成本 2 OT
select
  'OT成本'
,left(申请日期, 7) 月份
,仓库
,avg(OT类型) OT平均倍数
,sum(加班时长) 加班时长
from
(
    SELECT 
        ho.date_at 申请日期
        ,ho.staff_id 员工ID
        ,hsi.name 员工姓名
        ,CASE hsi.state 
                when 1 then '在职'
                when 2 then '离职'
                when 3 then '停职'
                else ht.state 
        end 在职状态
        ,case when 二级部门='AGV Warehouse' then 'AGV'
            when 二级部门='BPL2-Bangphli Return Warehouse' then 'BPL-Return'
            when 二级部门='Bangphli Livestream Warehouse' then 'BPL3'
            when 二级部门='BST-Bang Sao Thong Warehouse' then 'BST'
            when 二级部门='LAS-Lasalle Material Warehouse' then 'LAS'
            -- when 二级部门='LCP Warehouse' then 'LCP'
        end 仓库
        ,case 
            when left(三级部门,4)='Pack' then 'Packing'
            when left(三级部门,4)='Pick' then 'Picking'
            when left(三级部门,3)='Out' then 'Outbound'
            when left(三级部门,3)='Inb' then 'Inbound'
            when left(三级部门,3)='B2B' then 'B2B'
                else 'HO'
        end 职位类别
        ,sub_type
        ,hjt.job_name 职位
        ,sd2.name 部门
        ,sd.一级部门
        ,sd.二级部门
        ,sd.三级部门
        ,sd.四级部门
        ,if(hsi.sys_store_id ='-1','Head Office',ss.name) 网点
        ,CASE ho.`type` 
                when 1 then 1.5
                when 2 then 3
                when 4 then 1
                ELSE 0 
        end OT类型
        ,ho.start_time 开始时间
        ,ho.end_time 结束时间
        ,ho.duration 加班时长
        ,o.day_of_week 周几
        ,o.week_begin_date 周最早日期
        ,o.week_end_date 周最晚日期
        ,CASE when ho.`type` =4 and adv.times1 >0 then '是'
                when ho.`type` =1 and adv.times1_5 >0 then '是'
                when ho.`type` =2 and adv.times3 >0 then '是'
                ELSE '否'
        END  是否给加班费
        ,hsi2.job_title_grade_v2
        ,round((ifnull(gs.daysalary,0) + ifnull(gs.housesubsidy,0) + ifnull(gs.mealsubsidy,0) + ifnull(gs.socialsecurity,0)/30 + ifnull(subsidy,0)/30)/8, 0) salary_sum_hour
    FROM backyard_pro.hr_overtime ho 
    LEFT JOIN bi_pro.hr_staff_info hsi on ho.staff_id =hsi.staff_info_id 
    LEFT JOIN bi_pro.hr_staff_transfer ht on ho.staff_id =ht.staff_info_id and ho.date_at =ht.stat_date 
    left join fle_staging.sys_department sd2 on sd2.id =hsi.node_department_id 
    left join fle_staging.sys_store ss on ss.id =hsi.sys_store_id 
    left join dwm.dwd_hr_organizational_structure_detail sd  on sd.id =hsi.node_department_id 
    left join bi_pro.hr_job_title hjt on hjt.id =hsi.job_title 
    left join tmpale.ods_th_dim_date o on o.`date` =ho.date_at 
    left join bi_pro.attendance_data_v2 adv on adv.stat_date =ho.date_at and adv.staff_info_id =ho.staff_id
    left join backyard_pro.hr_staff_info hsi2 on hsi2.staff_info_id = ho.staff_id
    left join tmpale.tmp_th_ffm_jobsubsidy fjs on hjt.job_name = fjs.job
    left join tmpale.tmp_th_ffm_gradesalary gs on concat('F',hsi2.job_title_grade_v2) = gs.grade
    WHERE ho.state =2
        and sd.一级部门='Thailand Fulfillment'
        and ho.date_at >='2023-10-01'
) t0
  where 仓库 is not null
group by 1,2,3
order by 1,2,3;

-- 6 人力成本 3 在职
select
    title
    ,月份
    ,仓库
    ,三级部门
    ,round(在职/month_day_cnt, 2) 在职
    ,round(应出勤/month_day_cnt, 2) 应出勤
    ,round(出勤/month_day_cnt, 2) 出勤
    ,round(旷工/month_day_cnt, 2) 旷工
from
(
    select
        '考勤' title
        ,left(统计日期, 7) 月份
        ,仓库
        ,三级部门
        ,sum(在职) 在职
        ,sum(应出勤) 应出勤
        ,sum(出勤) 出勤
        ,sum(ABS) + sum(事假) 旷工
    from
    (
        SELECT 
            人员信息
            ,统计日期
            ,一级部门
            ,三级部门
            ,部门
            ,仓库
            ,在职
            ,职位类别
            ,职位
            ,上班打卡时间
            ,班次开始
            ,下班打卡时间
            ,班次结束
            ,公休日
            ,休息日
            ,if(出勤=1,1,应出勤) 应出勤
            ,出勤
        --    ,应出勤-请假-旷工 出勤
            -- ,未出勤
            ,请假
            -- ,请假时段
            ,if(迟到>0,floor(迟到),0) 迟到
            ,旷工
            ,年假
            ,事假
            ,病假
            ,产假
            ,丧假
            ,婚假
            ,公司培训假
            ,跨国探亲假
            ,旷工最晚时间
            ,ABS
            ,now() + interval -1 hour update_time
        FROM
        (
            SELECT
                人员信息
                ,统计日期
                ,一级部门
                ,三级部门
                ,部门
                ,仓库     
                ,职位类别
                ,职位
                ,在职
                ,上班打卡时间
                ,班次开始
                ,下班打卡时间
                ,班次结束
                ,公休日
                ,休息日
                ,应出勤
                ,出勤
                -- ,未出勤
                ,请假
                ,请假时段
                ,case when 应出勤=1 AND (上班打卡时间 is null or 下班打卡时间 is null) AND 请假=0 then 0 
                    when 应出勤=1 AND (上班打卡时间 is null or 下班打卡时间 is null) AND 请假=0.5 then 0
                    when 应出勤=1 AND 请假=1 then 0 
                    when 应出勤=1 AND 请假=0.5 AND 请假时段='上午半天假' AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(旷工最晚时间))/60<=120 then (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(旷工最晚时间))/60 
                    when 应出勤=1 AND 请假=0.5 AND 请假时段='上午半天假' AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(旷工最晚时间))/60>120 then 0
                    when 应出勤=1 AND 请假=0.5 AND 请假时段='下午半天假' AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60<=120 then (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60 
                    when 应出勤=1 AND 请假=0.5 AND 请假时段='下午半天假' AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60>120 then 0
                    when 应出勤=1 AND 请假=0 AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60<=120 then (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60
                    when 应出勤=1 AND 请假=0 AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60>120 then 0
                    when 应出勤=1 AND 请假=0 AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(旷工最晚时间))/60>0 then 0
                    else 0
                    end 迟到
                -- 当天没有上班卡或者下班卡为全天旷工；迟到120分钟内，算作迟到，5泰铢/分钟罚款；迟到超过120分钟，算半天旷工；迟到超过13:00或19:00，算全天旷工
                ,case when 应出勤=1 AND (上班打卡时间 is null or 下班打卡时间 is null) AND 请假=0 then 1 
                    when 应出勤=1 AND (上班打卡时间 is null or 下班打卡时间 is null) AND 请假=0.5 then 0.5
                    when 应出勤=1 AND 请假=1 then 0 
                    when 应出勤=1 AND 请假=0.5 AND 请假时段='上午半天假' AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(旷工最晚时间))/60<=120 then 0 
                    when 应出勤=1 AND 请假=0.5 AND 请假时段='上午半天假' AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(旷工最晚时间))/60>120 then 0.5
                    when 应出勤=1 AND 请假=0.5 AND 请假时段='下午半天假' AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60<=120 then 0 
                    when 应出勤=1 AND 请假=0.5 AND 请假时段='下午半天假' AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60>120 then 0.5
                    when 应出勤=1 AND 请假=0 AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60<=120 then 0
                    when 应出勤=1 AND 请假=0 AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(班次开始))/60>120 then 0.5
                    when 应出勤=1 AND 请假=0 AND (UNIX_TIMESTAMP(上班打卡时间)-UNIX_TIMESTAMP(旷工最晚时间))/60>0 then 1
                    else 0
                    end 旷工
                ,年假
                ,事假
                ,病假
                ,产假
                ,丧假
                ,婚假
                ,公司培训假
                ,跨国探亲假
                ,旷工最晚时间
                ,ABS
            FROM
            (
                SELECT 
                    人员信息
                    ,统计日期
                    ,一级部门
                    ,三级部门
                    ,部门
                    ,仓库
                    ,职位类别
                    ,职位
                    ,在职
                    ,上班打卡时间
                    ,班次开始
                    ,下班打卡时间
                    ,班次结束
                    ,公休日
                    ,休息日
                    ,应出勤
                    ,出勤
                    ,未出勤
                    ,请假
                    ,请假时段
                    ,年假
                    ,事假
                    ,病假
                    ,产假
                    ,丧假
                    ,婚假
                    ,公司培训假
                    ,跨国探亲假
                    ,concat(统计日期,' ',旷工最晚时间,':','00' ) 旷工最晚时间
                    ,ABS
                FROM 
                (
                    SELECT 
                        人员信息
                        ,统计日期
                        ,一级部门
                        ,三级部门
                        ,部门
                        ,仓库
                        ,职位类别
                        ,职位
                        ,在职
                        ,上班打卡时间
                        ,班次开始
                        ,下班打卡时间
                        ,班次结束
                        ,公休日
                        ,休息日
                        ,if(应出勤>0 and 病假<=0 and 请假>0, 0, 应出勤) 应出勤剔除放假
                        ,应出勤
                        ,出勤
                        ,未出勤
                        ,请假
                        ,请假时段
                        ,年假
                        ,事假
                        ,病假
                        ,产假
                        ,丧假
                        ,婚假
                        ,公司培训假
                        ,跨国探亲假
                        ,if(shift_start<'12:00','13:00','19:00') 旷工最晚时间
                        ,ABS
                    FROM 
                    (
                        SELECT 
                            ad.`staff_info_id` 人员信息
                            ,ad.`stat_date` 统计日期
                            ,sd.`一级部门` 一级部门
                            ,sd.`二级部门` 二级部门
                            ,sd.三级部门
                            ,sd.`name` 部门
                            ,case when 二级部门='AGV Warehouse' then 'AGV'
                                when 二级部门='BPL2-Bangphli Return Warehouse' then 'BPL-Return'
                                when 二级部门='Bangphli Livestream Warehouse' then 'BPL3'
                                when 二级部门='BST-Bang Sao Thong Warehouse' then 'BST'
                                when 二级部门='LAS-Lasalle Material Warehouse' then 'LAS'
                                when 二级部门='LCP Warehouse' then 'LCP'
                                end 仓库
                            ,case 
                                when left(三级部门,4)='Pack' then 'Packing'
                                when left(三级部门,4)='Pick' then 'Picking'
                                when left(三级部门,3)='Out' then 'Outbound'
                                when left(三级部门,3)='Inb' then 'Inbound'
                                when left(三级部门,3)='B2B' then 'B2B'
                                else 'HO'
                                end 职位类别
                            ,hjt.`job_name` 职位
                            ,case when ad.state='1' then 1 else 0 end  在职   
                            ,ad.`shift_start`
                            ,ad.`shift_end`
                            ,ad.`attendance_started_at` as 上班打卡时间
                            ,if(ad.`shift_start`<>'',concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ),null) as 班次开始
                            ,ad.`attendance_end_at` as 下班打卡时间
                            ,if(ad.`shift_end`<>'',concat(ad.`stat_date`,' ',ad.`shift_end`,':','00' ),null) as 班次结束
                            -- ,if
                            -- ,UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP((ad.`shift_start`<>'',concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ),null) )
                            -- ,ad.leave_type
                            ,case ad.leave_time_type when 1 then '上午半天'
                                when 2 then '下午半天'
                                when 3 then '全天'
                                end 请假时段
                            ,ad.attendance_time
                            -- ,ad.`AB`
                            ,case when ad.`AB`=10 then 1 when ad.`AB`=5 then 0.5 else 0 end as ABS
                            ,if(ad.PH!=0,1,0) 公休日
                            ,if(ad.OFF!=0,1,0) 休息日
                            -- ,if(ad.PH=0 AND ad.OFF=0,1,0) 应出勤
                            ,case when ad.PH=0 AND ad.OFF=0 then 1 # 当 ph=0 off=0 为当天应该出勤一天
                                when ad.PH=5 then 0.5 # 当 ph=5 或 off=0.5 应出勤半天
                                when ad.OFF=5 then 0.5
                                else 0 end as 应出勤
                            ,CASE WHEN  ad.`attendance_started_at` IS NOT NULL THEN 1 ELSE 0 END 出勤
                            -- ,if(ad.PH=0 AND ad.OFF=0 AND ad.attendance_time=0,1,0) 未出勤天数
                            ,case when ad.PH=0 AND ad.OFF=0 AND ad.attendance_time=0 then 1 
                                when ad.PH=0 AND ad.OFF=0 AND ad.attendance_time=5 then 0.5 
                                when ad.PH=0 AND ad.OFF=0 AND ad.attendance_time=10 then 0
                                end 未出勤
                            ,ad.`AB`
                            -- ,UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' )) 迟到分钟
                            -- ,case when ad.PH=0 AND ad.OFF=0 
                            --      AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))>0
                            --      AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))<=30 then UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))
                            --      else 0
                            --      end 迟到分钟
                            -- ,UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_end`,':','00' ))-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' )) 班次时长
                            -- 当天没有上班卡或者下班卡为全天旷工；迟到30分钟内，算作迟到，5泰铢/分钟罚款；迟到超过30分钟，算半天旷工；迟到超过4小时，算全天旷工
                            ,case when ad.PH=0 AND ad.OFF=0 
                                    AND (ad.`attendance_started_at` is null or ad.`attendance_end_at` is null) 
                                    AND ad.leave_type not in (1,2,12,3,18,4,5,17,7,10,16,19) then 1 
                                when ad.PH=0 AND ad.OFF=0 
                                    AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))>0
                                    AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))<=30  then 0
                                -- when ad.PH=0 AND ad.OFF=0 
                                --  AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))>30
                                --  AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))<=240  then 0.5
                                when ad.PH=0 AND ad.OFF=0 
                                    AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))>30
                                    AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))<=(UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_end`,':','00' ))-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' )))/2  then 0.5
                                -- when ad.PH=0 AND ad.OFF=0 
                                --  AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))>240 then 1
                                when ad.PH=0 AND ad.OFF=0 
                                    AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))>(UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_end`,':','00' ))-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' )))/2 then 1
                                else 0
                                end 旷工
                            ,case when ad.leave_type in (1,2,12,3,18,4,5,17,7,10,16,19) AND ad.leave_time_type=1 then 0.5
                                when ad.leave_type in (1,2,12,3,18,4,5,17,7,10,16,19) AND ad.leave_time_type=2 then 0.5
                                when ad.leave_type in (1,2,12,3,18,4,5,17,7,10,16,19) AND ad.leave_time_type=3 then 1 
                                else 0 
                                end 请假
                            ,case 
                                when ad.leave_type=1 and ad.leave_time_type=1 then 0.5
                                when ad.leave_type=1 and ad.leave_time_type=2 then 0.5
                                when ad.leave_type=1 and ad.leave_time_type=3 then 1
                                else 0
                                end 年假
                            ,case 
                                -- 带薪,不带薪事假
                                when ad.leave_type in (2,12) and ad.leave_time_type=1 then 0.5
                                when ad.leave_type in (2,12) and ad.leave_time_type=2 then 0.5
                                when ad.leave_type in (2,12) and ad.leave_time_type=3 then 1
                                else 0
                                end 事假
                            ,case 
                                -- 带薪,不带薪病假
                                when ad.leave_type in (3,18) and ad.leave_time_type=1 then 0.5
                                when ad.leave_type in (3,18) and ad.leave_time_type=2 then 0.5
                                when ad.leave_type in (3,18) and ad.leave_time_type=3 then 1
                                else 0
                                end 病假
                            ,case 
                                -- 产假,陪产假,产检
                                when ad.leave_type in (4,5,17) and ad.leave_time_type=1 then 0.5
                                when ad.leave_type in (4,5,17) and ad.leave_time_type=2 then 0.5
                                when ad.leave_type in (4,5,17) and ad.leave_time_type=3 then 1
                                else 0
                                end 产假
                            ,case 
                                when ad.leave_type=7 and ad.leave_time_type=1 then 0.5
                                when ad.leave_type=7 and ad.leave_time_type=2 then 0.5
                                when ad.leave_type=7 and ad.leave_time_type=3 then 1
                                else 0
                                end 丧假
                            ,case 
                                when ad.leave_type=10 and ad.leave_time_type=1 then 0.5
                                when ad.leave_type=10 and ad.leave_time_type=2 then 0.5
                                when ad.leave_type=10 and ad.leave_time_type=3 then 1
                                else 0
                                end 婚假
                            ,case 
                                when ad.leave_type=16 and ad.leave_time_type=1 then 0.5
                                when ad.leave_type=16 and ad.leave_time_type=2 then 0.5
                                when ad.leave_type=16 and ad.leave_time_type=3 then 1
                                else 0
                                end 公司培训假
                            ,case 
                                when ad.leave_type=19 and ad.leave_time_type=1 then 0.5
                                when ad.leave_type=19 and ad.leave_time_type=2 then 0.5
                                when ad.leave_type=19 and ad.leave_time_type=3 then 1
                                else 0
                                end 跨国探亲假
                        FROM `bi_pro`.`attendance_data_v2` ad
                        left join bi_pro.hr_staff_transfer hst on ad.staff_info_id = hst.staff_info_id and ad.`stat_date` = hst.stat_date
                        LEFT JOIN `fle_staging`.`staff_info` si on si.`id` =ad.`staff_info_id` 
                        LEFT JOIN `fle_staging`.`sys_store` ss on ss.`id` =si.`organization_id`
                        LEFT JOIN `bi_pro`.`hr_job_title` hjt on hjt.`id` =si.`job_title` 
                        -- LEFT JOIN `fle_staging`.`sys_department` sd on sd.`id` =si.`department_id` 
                        -- LEFT JOIN `dwm`.`dwd_hr_organizational_structure_detail` sd ON sd.`id`=si.`department_id`
                        LEFT JOIN `dwm`.`dwd_hr_organizational_structure_detail` sd ON sd.`id`= ad.node_department_id
                        WHERE sd.`一级部门`='Thailand Fulfillment'
                            AND ad.`stat_date`>= '2023-10-01'
                            -- and ad.`staff_info_id`='668975'
                    ) ad 
                    where 在职=1
                ) ad 
            ) ad 
        ) ad 
    )
    where 仓库 is not null
    group by 1,2,3,4
) t0
left join 
(
    select 
        left(date, 7) month 
        ,month_day_cnt 
    from 
    tmpale.ods_th_dim_date 
    where date>='2023-10-01'
        and date<'2024-10-01'
    group by 1,2
) date_month on t0.月份 = date_month.month
order by 1,2,3;

-- 6 人力成本 4 临时工
select 
    '临时工'
    ,left(费用开始日期,7) 月份
    ,仓库
    ,sum(金额) 金额
from
(
    SELECT 
        *
    FROM 
    (
        -- 普通付款
        SELECT 
            '普通付款' 付款类型
            ,bo.`name_cn` 付款项
            ,op.`apply_no` 付款单号
            -- ,op.`create_id` 
            -- ,op.`create_name`
            ,op.`apply_id` 申请人ID 
            ,op.`apply_name` 申请人名称
            ,op.`apply_company_name` 申请业务线
            -- ,op.`cost_department_name` 
            ,op.`apply_node_department_name` 申请部门
            ,op.`apply_store_name` 申请网点
            ,case when op.`apply_store_name` in ('AGV Warehouse','AGV  Warehouse') then 'AGV'
                when op.`apply_store_name` in ('Fulfillment Bang Sao Thong warehouse','BST- Bang Sao Thong warehouse') then 'BST'
                when op.`apply_store_name` in ('LAS-Lasalle Material Warehouse') then 'LAS'
                when op.`apply_store_name` in ('BPL3-Bangphli Live Stream Warehouse') then 'BPL3'
                when op.`apply_store_name` in ('BPL2-Bangphli Return Warehouse') then 'BPL_return'
                when op.`apply_store_name` in ('LCP Warehouse') then 'LCP'
                when op.`apply_store_name` in ('Head Office','Header Office') then 'Head Office'
                else op.`apply_store_name`
                end 仓库
            ,case op.`currency`
                when 1 then op.`amount_total_actually`
                when 2 then op.`amount_total_actually`*32
                when 3 then op.`amount_total_actually`*5
                end 金额
            -- ,op.`amount_total_actually` 金额
            ,op.`created_at` 创建时间
            ,op.`should_pay_date` 支付时间
            ,left(op.`should_pay_date`, 7) 支付月份
            ,op.`remark` 备注
            ,case op.`approval_status`
                when 1 then '待审核'
                when 2 then '已驳回'
                when 3 then '已通过'
                when 4 then '已撤回'
                end 审核状态
            ,case op.`pay_status`
                when 1 then '待支付'
                when 2 then '已支付'
                when 3 then '未支付'
                end 支付状态
            ,opd.cost_start_date 费用开始日期
        FROM `oa_production`.`ordinary_payment` op 
        LEFT JOIN `oa_production`.`ordinary_payment_detail` opd on opd.`ordinary_payment_id`=op.`id`
        LEFT JOIN `oa_production`.`budget_object` bo on opd.`budget_id`=bo.`id`
        WHERE 1=1
        -- op.`approval_status`=3 and op.`pay_status`=2
            -- and op.`apply_company_name`='Flash Fullfillment'
            and left(opd.cost_start_date, 7) >= '2022-11'
    ) a
)
where 1=1
    and 付款项 like'%劳务%' 
    and left(费用开始日期,7)>='2023-10' 
    and 申请业务线='Flash Fullfillment'
-- and 备注 like'%March%'
group by 1,2,3
order by 月份,仓库 