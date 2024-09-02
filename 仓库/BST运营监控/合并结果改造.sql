select
    '' type
    ,t0.日期
    ,t0.仓库
    ,t0.在职人数
    ,t1.应出勤
    ,t2.实际出勤
    ,round(t2.实际出勤/t0.在职人数, 4)  在职出勤率
    ,round(t2.实际出勤/t1.应出勤, 4)  应出勤率
    ,t5.临时工时
    ,t6.加班时长
    ,t7.采购订单到货单量
    ,t8.销退订单到货单量
    ,t9.采购订单入库单量
    ,t10.销退订单入库单量
    ,t11.采购订单及时入库
    ,t12.销退订单及时入库
    ,t13.采购订单应入库
    ,t14.销退订单应入库
    ,t11.采购订单及时入库 / t13.采购订单应入库 采购订单入库及时率
    ,t12.销退订单及时入库 / t14.销退订单应入库 销退订单入库及时率
    ,t15.采购订单未及时入库
    ,t16.销退订单未及时入库
    ,t17.采购订单及时上架
    ,t18.销退订单及时上架
    ,t19.采购订单应上架
    ,t20.销退订单应上架
    ,t17.采购订单及时上架 / t19.采购订单应上架 采购订单上架及时率
    ,t18.销退订单及时上架 / t20.销退订单应上架 销退订单上架及时率
    ,t21.采购订单未及时上架
    ,t22.销退订单未及时上架
from
(
    select 
        left(统计日期,10)日期
        ,仓库
        ,'在职人数' type
        ,sum(在职) 在职人数
    from 
    dwm.dwd_th_ffm_staff_dayV2
    WHERE left(统计日期,10)>=left(NOW() - interval 7 day,10)
        and 仓库 is not null
    GROUP BY 1,2,3
) t0
left join
(
    select 
        left(统计日期,10) 日期
        ,仓库
        ,'应出勤' type
        ,sum(应出勤) 应出勤
    from 
    dwm.dwd_th_ffm_staff_dayV2
    WHERE left(统计日期,10)>=left(NOW() - interval 7 day,10)
        and 仓库 is not null
        and 在职=1
    GROUP BY 1,2,3
) t1 on t0.日期 = t1.日期 and t0.仓库 = t1.仓库
left join
(
    select 
        left(统计日期,10) 日期
        ,仓库
        ,'实际出勤' type
        ,sum(出勤)  实际出勤
    from 
    dwm.dwd_th_ffm_staff_dayV2
    WHERE left(统计日期,10)>=left(NOW() - interval 7 day,10)
        and 仓库 is not null
        and 在职=1
    GROUP BY 1,2,3
) t2 on t0.日期 = t2.日期 and t0.仓库 = t2.仓库
left join
(
    select 
        left(dt,10) 日期
        ,warehouse 仓库
        ,'临时工工时' type
        ,sum(num_people*8+num_ot) 临时工时
    FROM dwm.th_ffm_tempworker_input WHERE left(dt,10)>=left(NOW() - interval 7 day,10)
        and warehouse is not null
    GROUP BY 1,2,3
) t5 on t0.日期 = t5.日期 and t0.仓库 = t5.仓库
left join
(
    select 
        left(申请日期,10) 日期
        ,仓库
        ,'OT工时' type
        ,nvl(sum(加班时长),0) 加班时长
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
        FROM backyard_pro.hr_overtime ho 
        LEFT JOIN bi_pro.hr_staff_info hsi on ho.staff_id =hsi.staff_info_id 
        LEFT JOIN bi_pro.hr_staff_transfer ht on ho.staff_id =ht.staff_info_id and ho.date_at =ht.stat_date 
        left join fle_staging.sys_department sd2 on sd2.id =hsi.node_department_id 
        left join fle_staging.sys_store ss on ss.id =hsi.sys_store_id 
        left join dwm.dwd_hr_organizational_structure_detail sd  on sd.id =hsi.node_department_id 
        left join bi_pro.hr_job_title hjt on hjt.id =hsi.job_title 
        left join tmpale.ods_th_dim_date o on o.`date` =ho.date_at 
        left join bi_pro.attendance_data_v2 adv on adv.stat_date =ho.date_at and adv.staff_info_id =ho.staff_id 
        WHERE ho.state =2
        and sd.一级部门='Thailand Fulfillment'
        and ho.date_at >= date_sub(date(now() + interval -1 hour),interval 30 day)
    ) OT
    WHERE left(申请日期,10)>=left(NOW() - interval 7 day,10)
        and 仓库 is not null
    GROUP BY 1,2,3
) t6 on t0.日期 = t6.日期 and t0.仓库 = t6.仓库
-- 入库
-- 采购订单 到货单量
left join
(
    select 
        LEFT(reg_time,10) 日期
        ,仓库名称 仓库
        ,单据
        ,'到货单量' 指标
        ,count(notice_number) 采购订单到货单量
    FROM dwm.dwd_th_ffm_arrivalnotice_dayV2
    WHERE 1=1
        and 单据='采购订单'
        and left(reg_time,10) BETWEEN LEFT(now()- INTERVAL 7 DAY ,10) AND LEFT(now()- INTERVAL 1 DAY ,10)
    group by 1,2,3,4
)t7 on t0.日期 = t7.日期 and t0.仓库 = t7.仓库

-- 销退订单 到货单量
left join
(
    select 
        LEFT(reg_time,10) 日期
        ,仓库名称 仓库
        ,单据
        ,'到货单量' 指标
        ,count(notice_number) 销退订单到货单量
    FROM dwm.dwd_th_ffm_arrivalnotice_dayV2
    WHERE 1=1
        and 单据='销退订单'
        and left(reg_time,10) BETWEEN LEFT(now()- INTERVAL 7 DAY ,10) AND LEFT(now()- INTERVAL 1 DAY ,10)
    group by 1,2,3,4
)t8 on t0.日期 = t8.日期 and t0.仓库 = t8.仓库

-- 采购订单 入库单量
left join
(
    select 
        LEFT(complete_time,10) 日期
        ,仓库名称 仓库
        ,单据
        ,'入库单量' 指标
        ,count(notice_number) 采购订单入库单量
    FROM dwm.dwd_th_ffm_arrivalnotice_dayV2
    WHERE 1=1
        and 单据='采购订单'
        and left(complete_time,10) BETWEEN LEFT(now()- INTERVAL 7 DAY ,10) AND LEFT(now()- INTERVAL 1 DAY ,10)
    group by 1,2,3,4
) t9 on t0.日期 = t9.日期 and t0.仓库 = t9.仓库

-- 销退订单 入库单量
left join
(
    select 
        LEFT(complete_time,10) 日期
        ,仓库名称 仓库
        ,单据
        ,'入库单量' 指标
        ,count(notice_number) 销退订单入库单量
    FROM dwm.dwd_th_ffm_arrivalnotice_dayV2
    WHERE 1=1
        and 单据='销退订单'
        and left(complete_time,10) BETWEEN LEFT(now()- INTERVAL 7 DAY ,10) AND LEFT(now()- INTERVAL 1 DAY ,10)
    group by 1,2,3,4
) t10 on t0.日期 = t10.日期 and t0.仓库 = t10.仓库

-- 采购订单 及时入库
left join
(
    SELECT 
        LEFT(receive_deadline_24h,10) 日期
        ,仓库名称 仓库
        ,单据
        ,'及时入库' TYPE
        ,sum(及时入库) 采购订单及时入库
    FROM dwm.dwd_th_ffm_arrivalnotice_dayV2
    WHERE 1=1
        and 单据='采购订单'
        and left(receive_deadline_24h,10) BETWEEN LEFT(now()- INTERVAL 7 DAY ,10) AND LEFT(now()- INTERVAL 1 DAY ,10)
    group by 1,2,3,4
) t11 on t0.日期 = t11.日期 and t0.仓库 = t11.仓库

-- 销退订单 及时入库
left join
(
    SELECT 
        LEFT(receive_deadline_24h,10) 日期
        ,仓库名称 仓库
        ,单据
        ,'及时入库' TYPE
        ,sum(及时入库) 销退订单及时入库
    FROM dwm.dwd_th_ffm_arrivalnotice_dayV2
    WHERE 1=1
        and 单据='销退订单'
        and left(receive_deadline_24h,10) BETWEEN LEFT(now()- INTERVAL 7 DAY ,10) AND LEFT(now()- INTERVAL 1 DAY ,10)
    group by 1,2,3,4
) t12 on t0.日期 = t12.日期 and t0.仓库 = t12.仓库

-- 采购订单 应入库
left join
(
    SELECT 
        LEFT(receive_deadline_24h,10) 日期
        ,仓库名称 仓库
        ,单据
        ,'应入库' TYPE
        ,sum(应入库) 采购订单应入库
    FROM dwm.dwd_th_ffm_arrivalnotice_dayV2
    WHERE 1=1
        and 单据='采购订单'
        and left(receive_deadline_24h,10) BETWEEN LEFT(now()- INTERVAL 7 DAY ,10) AND LEFT(now()- INTERVAL 1 DAY ,10)
    group by 1,2,3,4
) t13 on t0.日期 = t13.日期 and t0.仓库 = t13.仓库

-- 销退订单 应入库
left join
(
    SELECT 
        LEFT(receive_deadline_24h,10) 日期
        ,仓库名称 仓库
        ,单据
        ,'应入库' TYPE
        ,sum(应入库) 销退订单应入库
    FROM dwm.dwd_th_ffm_arrivalnotice_dayV2
    WHERE 1=1
        and 单据='销退订单'
        and left(receive_deadline_24h,10) BETWEEN LEFT(now()- INTERVAL 7 DAY ,10) AND LEFT(now()- INTERVAL 1 DAY ,10)
    group by 1,2,3,4
) t14 on t0.日期 = t14.日期 and t0.仓库 = t14.仓库

-- 采购订单 未及时入库
left join
(
    SELECT 
        LEFT(receive_deadline_24h,10) 日期
        ,仓库名称 仓库
        ,单据
        ,'未及时入库' 未及时入库
        ,count(notice_number) 采购订单未及时入库
    FROM dwm.dwd_th_ffm_arrivalnotice_dayV2
    WHERE 1=1
        and 单据='采购订单'
        and 及时入库='0'
        and  应入库='1'
        and left(receive_deadline_24h,10) BETWEEN LEFT(now()- INTERVAL 7 DAY ,10) AND LEFT(now()- INTERVAL 1 DAY ,10)
    group by 1,2,3,4
) t15 on t0.日期 = t15.日期 and t0.仓库 = t15.仓库

-- 销退订单 未及时入库
left join
(
    SELECT 
        LEFT(receive_deadline_24h,10) 日期
        ,仓库名称 仓库
        ,单据
        ,'未及时入库' 未及时入库
        ,count(notice_number) 销退订单未及时入库
    FROM dwm.dwd_th_ffm_arrivalnotice_dayV2
    WHERE 1=1
        and 单据='销退订单'
        and 及时入库='0'
        and  应入库='1'
        and left(receive_deadline_24h,10) BETWEEN LEFT(now()- INTERVAL 7 DAY ,10) AND LEFT(now()- INTERVAL 1 DAY ,10)
    group by 1,2,3,4
) t16 on t0.日期 = t16.日期 and t0.仓库 = t16.仓库

-- 采购订单 及时上架
left join
(
    SELECT 
        LEFT(putaway_deadline_48h,10) 日期
        ,仓库名称 仓库
        ,单据
        ,'及时上架' TYPE
        ,sum(及时上架) 采购订单及时上架
    FROM dwm.dwd_th_ffm_arrivalnotice_dayV2
    WHERE 1=1
        and 单据='采购订单'
        and left(putaway_deadline_48h,10) BETWEEN LEFT(now()- INTERVAL 7 DAY ,10) AND LEFT(now()- INTERVAL 1 DAY ,10)
    group by 1,2,3,4
) t17 on t0.日期 = t17.日期 and t0.仓库 = t17.仓库

-- 销退订单 及时上架
left join
(
    SELECT 
        LEFT(putaway_deadline_48h,10) 日期
        ,仓库名称 仓库
        ,单据
        ,'及时上架' TYPE
        ,sum(及时上架) 销退订单及时上架
    FROM dwm.dwd_th_ffm_arrivalnotice_dayV2
    WHERE 1=1
        and 单据='销退订单'
        and left(putaway_deadline_48h,10) BETWEEN LEFT(now()- INTERVAL 7 DAY ,10) AND LEFT(now()- INTERVAL 1 DAY ,10)
    group by 1,2,3,4
) t18 on t0.日期 = t18.日期 and t0.仓库 = t18.仓库

-- 采购订单 应上架
left join
(
    SELECT 
        LEFT(putaway_deadline_48h,10) 日期
        ,仓库名称 仓库
        ,单据
        ,'应上架' TYPE
        ,sum(应上架) 采购订单应上架
    FROM dwm.dwd_th_ffm_arrivalnotice_dayV2
    WHERE 1=1
        and 单据='采购订单'
        and left(putaway_deadline_48h,10) BETWEEN LEFT(now()- INTERVAL 7 DAY ,10) AND LEFT(now()- INTERVAL 1 DAY ,10)
    group by 1,2,3,4
) t19 on t0.日期 = t19.日期 and t0.仓库 = t19.仓库

-- 销退订单 应上架
left join
(
    SELECT 
        LEFT(putaway_deadline_48h,10) 日期
        ,仓库名称 仓库
        ,单据
        ,'应上架' TYPE
        ,sum(应上架) 销退订单应上架
    FROM dwm.dwd_th_ffm_arrivalnotice_dayV2
    WHERE 1=1
        and 单据='销退订单'
        and left(putaway_deadline_48h,10) BETWEEN LEFT(now()- INTERVAL 7 DAY ,10) AND LEFT(now()- INTERVAL 1 DAY ,10)
    group by 1,2,3,4
) t20 on t0.日期 = t20.日期 and t0.仓库 = t20.仓库

-- 采购订单 未及时上架
left join
(
    SELECT 
        /* LEFT(now()- INTERVAL 1 DAY ,10) 日期 */
        LEFT(putaway_deadline_48h,10) 日期
        ,仓库名称 仓库
        ,单据
        ,'未及时上架' type
        ,count(notice_number) 采购订单未及时上架
    FROM dwm.dwd_th_ffm_arrivalnotice_dayV2
    WHERE 1=1
    and 单据='采购订单'
    and 及时上架='0' 
    and  应上架='1' 
    AND shelf_status <> '1080'
    and left(putaway_deadline_48h,10) BETWEEN LEFT(now()- INTERVAL 7 DAY ,10) AND LEFT(now()- INTERVAL 1 DAY ,10)
    group by 1,2,3,4
) t21 on t0.日期 = t21.日期 and t0.仓库 = t21.仓库

-- 销退订单 未及时上架
left join
(
    SELECT 
        /* LEFT(now()- INTERVAL 1 DAY ,10) 日期 */
        LEFT(putaway_deadline_48h,10) 日期
        ,仓库名称 仓库
        ,单据
        ,'未及时上架' type
        ,count(notice_number) 销退订单未及时上架
    FROM dwm.dwd_th_ffm_arrivalnotice_dayV2
    WHERE 1=1
    and 单据='销退订单'
    and 及时上架='0' 
    and  应上架='1' 
    AND shelf_status <> '1080'
    and left(putaway_deadline_48h,10) BETWEEN LEFT(now()- INTERVAL 7 DAY ,10) AND LEFT(now()- INTERVAL 1 DAY ,10)
    group by 1,2,3,4
) t22 on t0.日期 = t22.日期 and t0.仓库 = t22.仓库

-- 出库
-- B2C 已审核单量
left join
(
    SELECT 
        LEFT(created_time,10) 日期
        ,null paltform
        ,warehouse_name
        ,TYPE 单据
        ,'已审核单量' 指标
        ,COUNT(delivery_sn) 已审核单量
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where  1=1
        and LEFT(created_time,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        AND audit_time IS NOT NULL
        and TYPE='B2C'
    GROUP BY 1,2,3,4
) t23 on t0.日期 = t23.日期 and t0.仓库 = t23.仓库

-- B2C 未审核单量
left join
(
    SELECT 
        LEFT(created_time,10) 日期
        ,null paltform
        ,warehouse_name
        ,type 单据
        ,'未审核单量' 指标
        ,COUNT(delivery_sn) 未审核单量
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(created_time,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        AND  audit_time IS  NULL
        and TYPE='B2C'
    GROUP BY 1,2,3,4
) t24 on t0.日期 = t24.日期 and t0.仓库 = t24.仓库

-- B2C 商品数量
left join
(
    SELECT 
        LEFT(if(type='B2C', delivery_time, pack_time),10) 日期
        ,null paltform
        ,warehouse_name
        ,type 单据
        ,'商品数量' 指标
        ,sum(goods_num) 商品数量
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(created_time,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and audit_time is not null
        and TYPE='B2C'
    GROUP BY 1,2,3,4
) t25 on t0.日期 = t25.日期 and t0.仓库 = t25.仓库

-- B2C 出库单量
left join
(
    SELECT 
        LEFT(if(type='B2C', delivery_time, pack_time),10) 日期
        ,null paltform
        ,warehouse_name
        ,type 单据
        ,'出库单量' 指标
        ,COUNT(delivery_sn) 出库单量
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(pack_time,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and TYPE='B2C'
    GROUP BY 1,2,3,4
) t26 on t0.日期 = t26.日期 and t0.仓库 = t26.仓库

-- shopee B2C 及时发货
left join
(
    SELECT 
        LEFT(deadline,10) 日期
        ,'Shopee' paltform
        ,warehouse_name
        ,type 单据
        ,'及时发货' 指标
        ,sum(及时发货) 及时发货
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(deadline,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='Shopee'
        and TYPE='B2C'
    GROUP BY 1,2,3,4
) t27 on t0.日期 = t27.日期 and t0.仓库 = t27.仓库

-- shopee B2C 应发货
left join
(
    SELECT 
        LEFT(deadline,10) 日期
        ,'Shopee' paltform
        ,warehouse_name
        ,type 单据
        ,'应发货' 指标
        ,sum(应发货) 应发货
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(deadline,10) BETWEEN LEFT(now() - INTERVAL 7 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='Shopee'
        and TYPE='B2C'
    GROUP BY 1,2,3,4
) t28 on t0.日期 = t28.日期 and t0.仓库 = t28.仓库




where 1=1
	and t0.日期='${dt}'
	and t0.仓库='BST'