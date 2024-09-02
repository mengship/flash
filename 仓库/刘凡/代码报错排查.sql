-- 0830
WITH Store_list AS 
(
    SELECT
        ss.id AS `store_id`,
        ss.name AS `store_name`,
        mr.`name` 大区,
        ss.`province_code` 府id,
        sp.`name` 府名称,
        sp.`en_name` 府英文,
        case
            WHEN ss.`category` in (1) THEN 'SP'
        WHEN ss.`category` in (2) THEN 'DC'
            WHEN ss.`category` in (10) THEN 'BDC'
            WHEN ss.`category` in (14) THEN 'PDC'
            WHEN ss.`category` in (13) THEN 'CDC'
            WHEN ss.`category` in (4, 5, 7) THEN 'Shop'
            WHEN ss.`category` in (8, 12) THEN 'HUB'
        end 网点类型,
        ss.lng, # AS `经度`,
        ss.lat, # AS `纬度`,
        date(date_add(ss.created_at, INTERVAL 8 HOUR)) AS `创建日期`,
        ss.opening_at AS `系统记录营业时间`
    FROM fle_staging.sys_store ss
    left JOIN `fle_staging`.`sys_province` sp on sp.`code` =ss.`province_code`
    LEFT JOIN `fle_staging`.`sys_manage_region` mr on mr.`id` =ss.`manage_region`
    WHERE ss.category IN (1,10,13,14,2) and ss.`name` not like '%_BSP%' and ss.name not like 'Virtual_%'
),
contract_list as 
(
    SELECT
        cs.`contract_id` 合同编号,
        cs.store_id,
        cs.contract_name 合同名称,
        ss.name 网点,
        ss.category,
        ss.lng ms_lng,
        ss.lat ms_lat,
        case cs.`is_main`
            when 1 then '主合同'
            when 2 THEN '附合同'
            when 3 THEN 'LOI'
        end 合同类型,
        cs.`rent_due_date` '每月付房租的日期',
            case  cs.`money_symbol`
            when 1 then '泰铢'
            when 2 then '美元'
            when 3 then '人民币'
            when 4 then '比索'
            end 币种,
        cs.`created_at` 合同提交时间,
        cs.`manage_id` 合同提交人id,
        case cs.`contract_status`
        when 1 then '待审核'
        when 2 then '审核驳回'
        when 3 then '审核通过'
        when 4 then '已撤回 '
        end 合同状态,
        case ca.`status`
            when 1 then '待归档'
            when 2 then '已归档'
            when 3 then '已作废'
            when 4 then '待上传盖章合同'
            when 5 then '已终止'
            when 6 then '待作废'
            when 7 then '待终止'
            else status
        end '归档状态',
        cs.`contract_begin` 合同开始日期,
        cs.`contract_end` 合同结束日期,
        case cs.`contract_lease_type`
        when 1 THEN '月付'
        when 2 THEN '年付'
        when 3 THEN '季付'
        when 4 THEN '半年付'
        when 5 THEN '一次性付款'
        end 付款方式,
        lon_lat,
        SUBSTRING_INDEX(lon_lat,',',1) + 0 AS `经度`,#经纬度转为数字格式 -- 经度lng
        SUBSTRING_INDEX(lon_lat,',',-1) + 0 AS `纬度`, -- 纬度lat
        a.经度+0 '矫正经度',a.纬度+0 '矫正纬度',
        -- coalesce(a.经度,SUBSTRING_INDEX(lon_lat,',',1))+0 'lng',
        -- coalesce(a.纬度,SUBSTRING_INDEX(lon_lat,',',-1))+0 'lat',
        case when a.经度 is not null  then a.经度+0
            when a.经度 is null and cw.warehouse_longitude <>'' then cw.warehouse_longitude+0
            when a.经度 is null and cw.warehouse_longitude ='' then SUBSTRING_INDEX(lon_lat,',',1)+0
        end 'lng',
        case when a.纬度 is not null  then a.纬度+0
            when a.纬度 is null and cw.warehouse_latitude <>'' then cw.warehouse_latitude+0
            when a.纬度 is null and cw.warehouse_latitude ='' then SUBSTRING_INDEX(lon_lat,',',-1)+0
        end 'lat',
        amt.费用开始日期,
        amt.费用结束日期,
        amt.类别,
        sum(amt.不含税金额) '不含税金额',
        amt.VAT税率  'VAT税率'  ,
        sum(amt.VAT税额)  'VAT税额'  ,
        sum(amt.含税金额)   '含税金额' ,
        amt.WHT类别  'WHT类别',
        amt.WHT税率  'WHT税率',
        sum(amt.WHT金额)  'WHT金额'
        ,cs.warehouse_id
        ,ws.store_id 实际使用网点
    FROM
    (SELECT *,replace(replace(bank_collection,'[',''),']','') update_bank_collection FROM oa_production.contract_store_renting )cs
    left join oa_production.contract_warehouse cw on cw.warehouse_id=cs.warehouse_id
    left join oa_production.warehouse_store ws on ws.warehouse_main_id=cw.id
    left join bi_pro.hr_staff_info si on si.staff_info_id=cs.contract_leader_id
    left join fle_staging.sys_store ss on ss.id=cs.store_id
    left join
    (
        select
            csd.contract_store_renting_id,
            csd.cost_start_date 费用开始日期,
            csd.cost_end_date 费用结束日期,
            csd.amount_no_tax 不含税金额,
            csd.vat_rate VAT税率,
            csd.amount_vat VAT税额,
            csd.amount_has_tax 含税金额,
            case csd.wht_category
                when 0 then '/'
                when 1 then 'PND3'
                when 2 then 'PND53'
            end WHT类别,
            csd.wht_rate WHT税率,
            csd.amount_wht WHT金额,
            '房租' 类别
        FROM
            `oa_production`.contract_store_renting_detail csd
        union all

        SELECT
            csa.contract_store_renting_id,
            csa.start_time 费用开始日期,
            csa.end_time 费用结束日期,
            csa.area_service_amount_no_tax 不含税金额,
            csa.area_vat_rate VAT税率,
            csa.area_service_amount_vat VAT税额,
            null 含税金额,
            case csa.area_wht_category
                when 0 then '/'
                when 1 then 'PND3'
                when 2 then 'PND53'
            end WHT类别,
            csa.area_wht_rate WHT税率,
            csa.area_amount_wht WHT金额,
            '区域服务费' 类别
        FROM `oa_production`.`contract_store_renting_area` csa
    ) amt on amt.contract_store_renting_id=cs.id
    LEFT JOIN `oa_production`.`contract_archive` ca on ca.`cno` =cs.`contract_id`
    LEFT JOIN tmpale.tmp_th_store_location a on a.contract_id=cs.contract_id
    WHERE 1=1
        -- and cs.contract_id='202201184708'
        and cs.`contract_status` in (1,3)
        and cs.`contract_end`>= CURRENT_DATE
        and ca.`status` not in (3,5)
        and (CURRENT_DATE >= cs.`contract_effect_date` and CURRENT_DATE <=cs.`contract_end`)
        -- and (CURRENT_DATE >= csa.`start_time` and CURRENT_DATE <=csa.`end_time`)
        and (current_date between 费用开始日期 and 费用结束日期)
        and 类别='房租'
        and ss.category in (1,10,13,14)
        group by 
            cs.contract_id
            ,cs.store_id
            ,cs.`is_main`
            ,amt.费用开始日期
            ,amt.费用结束日期
            ,amt.类别
),
rental_payment as
(
    select 
        t.*,
        ROW_NUMBER() over (partition by t.`contract_no`, t.费用类型 order by t.`created_at` desc) rn
    from 
    (
        select 
            * 
        from 
        (
            select 
                *
                ,ROW_NUMBER() over (partition by a.`contract_no`,a.`费用类型` order by 时长 ) arn
            from 
            (
                SELECT 
                    ps.`contract_no`,
                    date(ps.`created_at`)   付款日期,
                    ps.`created_at`,
                    psr.`apply_no`,
                    ps.`store_id`,
                    ps.`store_name`,
                    ps.`cost_start_date`   费用发生日期,
                    ps.`cost_end_date`     费用结束日期,
                    case psr.pay_status
                        when 1 then '待支付'
                        when 2 then '已支付'
                        when 3 then '未支付'
                        end   支付状态,
                    case ps.`cost_type`
                        when 0 then '未设置'
                        when 1 then '房租'
                        when 2 then '押金'
                        when 3 then '区域服务费'
                        when 4 then '房产税'
                        when 5 then '定金'
                        else ps.`cost_type` end   费用类型,
                        abs(datediff(ps.`cost_start_date`,'2123-12-31')) 时长,
                    sum(ps.`actually_amount`)  'actually_amount'
                FROM `oa_production`.`payment_store_renting_detail` ps
                LEFT JOIN oa_production.`payment_store_renting` psr on ps.`store_renting_id` = psr.`id`
                WHERE psr.pay_status in (1,2)
                group by contract_no,psr.`apply_no`,ps.`created_at`, ps.`store_id`, ps.`cost_type`, ps.`cost_start_date`,
                        ps.`cost_end_date`,psr.pay_status,ps.`cost_type`
            ) a
        ) aa where aa.arn=1
    ) t
),store_near as 
(
    select
        pi.ticket_pickup_store_id,
        store.store_id,
        store.store_name,
        store.store_category,
        store.lng,
        store.lat,
        store.manage_region_name,
        store.manage_piece_name,
        count(pi.pno) cnt
    from tmpale.dwd_th_store_detail store
    left join 
    (
        select 
            * 
        from
        fle_staging.parcel_info pi
        where  pi.created_at >= date_sub(CONVERT_TZ(curdate(), '+07:00', '+00:00'),interval 1 day)
            and pi.created_at < date_sub(CONVERT_TZ(curdate(), '+07:00', '+00:00'), interval 0 day)
            and pi.state<>9 and pi.returned=0 
    ) pi on store.store_id = pi.ticket_pickup_store_id
    where 1=1
        and store.store_category IN (1, 10, 13, 14) 
        and  store.`store_name` not like '%_BSP%' 
        and store.store_name not like 'Virtual_%'
    group by 1,2,3,4,5,6
),service_area_list as 
(
    SELECT 
        ss.id store_id
        ,ss.`name` 
        , COUNT(sd.`code`)  cnt
    FROM `fle_staging`.`sys_store`  ss
    LEFT JOIN `fle_staging`.`sys_district` sd on sd.`store_id` =ss.`id` and sd.`opened` =1
    WHERE ss.`category` in (1)
    GROUP BY 1,2
    union all
    SELECT 
        ss.id store_id
        ,ss.`name` 
        , COUNT(sd.`code`)  cnt
    FROM `fle_staging`.`sys_store`  ss
    LEFT JOIN `fle_staging`.`sys_district` sd on sd.`separation_store_id`  =ss.`id` and sd.`opened` =1
    WHERE ss.`category` in (10,14)
    GROUP BY 1,2
) -- 服务区域

SELECT 
    a.合同编号,
    a.距离1公里内的网点,
    t.cnt 同场网点合同数量,
    st_lng_lat.经纬度数量,
    a.经纬度完全一致的网点,
    t2.cnt 同场网点合同数量,
        st_lng_lat2.经纬度数量,
    a.合同网点,
    a.合同网点id,
    a.合同名称,
    -- a.经纬度匹配,
    round(a.距离) 距离_米,
    a.ms_lng,a.ms_lat,
    a.lng '合同/仓库lng',
    a.lat '合同/仓库lat',
    a.合同编号,
    a.合同类型,
    a.每月付房租的日期,
    a.币种,
    a.类别,
    a.合同开始日期,
    a.合同结束日期,
    a.应付金额,
    a.付款方式,
    a.费用发生日期,
    a.费用结束日期,
    --  a.费用类型,
    a.付款编号,
    a.最近一次付款日期,
    a.支付状态,
    a.实付金额
    ,a.warehouse_id,
    a.实际使用网点
FROM
(
    SELECT
        cl.合同编号,
        cl.合同类型,
        cl.网点 合同网点,
        cl.合同名称,
        cl.ms_lat,cl.ms_lng,
        cl.store_id 合同网点id,
        cl.每月付房租的日期,
        cl.币种,
        cl.类别,
        cl.合同状态,
        cl.归档状态,
        cl.合同开始日期,
        cl.合同结束日期,
        cl.付款方式,
        cl.lon_lat,
        cl.矫正经度,
        cl.矫正纬度,
        cl.lng,cl.lat,
        cl.warehouse_id,
        cl.实际使用网点,
        -- if(sn2.store_id is not null,'经纬度相同','不完全一致') 经纬度匹配,
        st_distance_sphere(point(Store_list_1.lng,Store_list_1.lat),point(if(cl.lat >90,cl.lat,cl.lng),least(if(cl.lat >90,cl.lng,cl.lat),89))) 距离,
        rp.费用发生日期,
        rp.费用结束日期,
        -- rp.费用类型,
        rp.apply_no 付款编号,
        rp.付款日期 最近一次付款日期,
        rp.支付状态,
        rp.actually_amount 实付金额,
            cl.不含税金额+cl.VAT税额-cl.WHT金额 '应付金额',
        sum(store_near.cnt) 前一天揽件量,
        if(sum(store_near.cnt)>0 or sa.cnt >= 1,'Y','N') 是否营业,
        group_concat(DISTINCT store_near.store_name order by store_near.store_name) AS 距离1公里内的网点,
        group_concat(DISTINCT sn2.store_name order by sn2.store_name) AS 经纬度完全一致的网点
    FROM contract_list cl
    LEFT JOIN Store_list AS Store_list_1 on cl.store_id=Store_list_1.store_id
    LEFT JOIN 
    (
        SELECT 
            lng
            ,lat
            ,GROUP_CONCAT(store_name) AS `com_store`
        FROM Store_list
        GROUP BY 1,2
        HAVING count(1) >1
    ) com ON Store_list_1.lng = com.lng AND Store_list_1.lat = com.lat
    -- LEFT JOIN update_address ua on ua.object_id=Store_list_1.store_id and ua.rn=1
    -- LEFT JOIN pickup_store_list ps on ps.store_id=Store_list_1.store_id
    -- LEFT JOIN delivery_store_list ds on ds.store_id=Store_list_1.store_id
    LEFT JOIN rental_payment rp on rp.contract_no=cl.合同编号 and rp.rn=1 and rp.费用类型=cl.类别
    LEFT JOIN  store_near ON 1=1 AND st_distance_sphere(point(store_near.lng,store_near.lat),point(least(if(cl.lat >90,cl.lat,cl.lng),180), least(if(cl.lat >90,cl.lng,cl.lat),89))) < 1000
    LEFT JOIN  store_near sn2 ON 1=1 AND st_distance_sphere(point(sn2.lng,sn2.lat),point(least(if(cl.lat >90,cl.lat,cl.lng),180), least(if(cl.lat >90,cl.lng,cl.lat),89))) =0
    LEFT JOIN service_area_list sa on sa.store_id=store_near.store_id
    WHERE Store_list_1.网点类型 in ('SP','BDC','PDC','CDC')
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
) a
left join
(
    select 
        t.距离1公里内的网点
        ,count(t.合同编号) cnt
    from
    (
        select 
            cl.合同编号
            ,group_concat(DISTINCT store_near.store_name order by store_near.store_name) AS 距离1公里内的网点
        from contract_list cl
        LEFT JOIN  store_near ON 1=1 AND st_distance_sphere(point(store_near.lng,store_near.lat),point(least(if(cl.lat >90,cl.lat,cl.lng),180),least(if(cl.lat >90,cl.lng,cl.lat),89))) < 1000
        group by 1
    )t
    group by 1
) t on t.距离1公里内的网点=a.距离1公里内的网点
left join
(
    select 
        t.经纬度完全一致的网点
        ,count(t.合同编号) cnt
    from
    (
        select 
            cl.合同编号
            ,group_concat(DISTINCT store_near.store_name order by store_near.store_name) AS 经纬度完全一致的网点
        from contract_list cl
        LEFT JOIN  store_near ON 1=1 AND st_distance_sphere(point(store_near.lng,store_near.lat), point(least(if(cl.lat >90,cl.lat,cl.lng),180), least(if(cl.lat >90,cl.lng,cl.lat),89)))=0
        group by 1
    )t
    group by 1
) t2 on t2.经纬度完全一致的网点=a.经纬度完全一致的网点
left join
(
    select 
        距离1公里内的网点
        ,count(distinct st.经纬度) 经纬度数量 
    from
    (
        SELECT  
            Store_list_1.store_id
            ,Store_list_1.store_name
            ,Store_list_1.lng
            ,Store_list_1.lat
            ,concat(Store_list_1.lng,',', Store_list_1.lat) 经纬度
            ,group_concat(DISTINCT com.store_name order by com.store_name) AS 距离1公里内的网点
        FROM store_near AS Store_list_1
        LEFT JOIN store_near com
        ON 1=1  AND st_distance_sphere(point(Store_list_1.lng,Store_list_1.lat), point(least(if(com.lat >90,com.lat,com.lng),180), least(if(com.lat >90,com.lng,com.lat),89))) < 1000
        group by 1,2,3,4,5
    ) st
    group by 1
) st_lng_lat on st_lng_lat.距离1公里内的网点=t.距离1公里内的网点
left join
(
    select 
        经纬度完全一致的网点
        ,count(distinct st.经纬度) 经纬度数量 
    from 
    (
        SELECT  
            Store_list_1.store_id
            ,Store_list_1.store_name
            ,Store_list_1.lng
            ,Store_list_1.lat
            ,concat(Store_list_1.lng,',', Store_list_1.lat) 经纬度
            ,group_concat(DISTINCT com.store_name order by com.store_name) AS 经纬度完全一致的网点
        FROM store_near AS Store_list_1
        LEFT JOIN store_near com
        ON 1=1  AND st_distance_sphere(point(Store_list_1.lng,Store_list_1.lat), point(least(if(com.lat >90,com.lat,com.lng),180), least(if(com.lat >90,com.lng,com.lat),89)))=0
        group by 1,2,3,4,5
    ) st
    group by 1
) st_lng_lat2 on st_lng_lat2.经纬度完全一致的网点=t2.经纬度完全一致的网点
where  合同编号 in ('202407259035')

  -- a.距离1公里内的网点 like '%SRU%ี'