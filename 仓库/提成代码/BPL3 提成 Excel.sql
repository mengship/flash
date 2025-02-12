SELECT
    a.仓库,
    a.员工ID,
    a.职位,
    null 分组,
    a.出勤,
    null Inbound,
    null Picking,
    null Packing,
    null Outbound,
    null B2B,
    工作量 计提件数,
    round(基础提成2 * 考勤系数 - 迟到 * 5 + 全勤奖, 2) 工作量提成,
    round(基础提成2 * 考勤系数 - 迟到 * 5 + 全勤奖, 2) 提成,
    null 业务罚款,
    null 现场管理罚款,
    迟到扣款 出勤罚款,
    全勤奖,
    null 奖励,
    round((基础提成2 * 考勤系数 - 迟到 * 5 + 全勤奖) * kpi, 2) 最终提成,
    迟到,
    旷工,
    年假,
    事假,
    病假,
    产假,
    丧假,
    婚假,
    公司培训假
FROM
(
    SELECT
        a.*
        ,IF(职位='Office',0.5*AVG(IF(职位!='Office' and 是否当月入职='非当月入职',基础提成,null)) over (PARTITION by 'BPL3'),基础提成) 基础提成2
    FROM     
    (
        SELECT
            a.*
            --         ,调整超额提成*考勤系数-迟到*5 考勤提成
            ,迟到*5 迟到扣款
            ,if(职位='Inbound Supervisor',1.3*AVG(if(职位='Inbound' and 是否当月入职='非当月入职',调整超额提成*考勤系数-迟到*5,null)) over (PARTITION by 'BPL3'),
                                    IF(职位='Supervisor',1.3*AVG (if(职位 in ('Pack','Pick','Outbound') and 是否当月入职='非当月入职',调整超额提成*考勤系数-迟到*5,null)) over (PARTITION by 'BPL3'),调整超额提成)) 基础提成
        FROM
        (
            SELECT
                a.*
                ,工作量-调整目标值 超额总工作量
                ,(工作量-调整目标值)*超额系数 超额总提成
                ,(工作量-调整目标值)*超额系数*出勤/部门出勤 超额提成
                ,(工作量-调整目标值)*超额系数/部门出勤 平均日超额提成
                ,if(应出勤=出勤,800,0) 全勤奖
                ,IF(请假天数=0,1,if(请假天数>0 and 请假天数<=1,0.7,if(请假天数>1 and 请假天数<=2,0.5,if(请假天数>2 and 请假天数<=3,0.3,0)))) 考勤系数
                ,IF(是否当月入职='非当月入职',(工作量-调整目标值)*超额系数*出勤/部门出勤,((工作量-调整目标值)*超额系数*出勤/部门出勤)-((工作量-调整目标值)*超额系数/部门出勤)*7) 调整超额提成
            FROM
            (
                SELECT
                    a.*
                    ,if(职位='Inbound',实际上架数量,if(职位 in ('Pack','Pick','Outbound'),订单量,null)) 工作量
                    ,订单量
                    ,实际上架数量
                    ,目标值*部门出勤/部门应出勤 调整目标值 # @TODO 如果是半个月,这里要加 0.5
                FROM
                (
                    SELECT
                        'BPL3' 仓库
                        ,tt.id 员工ID
                        ,职位
                        ,人效
                        ,IF(职位='Inbound',人效*3*26,IF(职位 in ('Pack','Pick','Outbound'),人效*9*26,null)) 目标值 # @TODO inbound 3 人, 'Pack','Pick','Outbound'共 9 人,需要进行调整
                        ,超额系数
                        ,应出勤
                        ,出勤
                        ,迟到
                        ,旷工
                        ,年假
                        ,事假
                        ,病假
                        ,产假
                        ,丧假
                        ,婚假
                        ,公司培训假
                        ,事假+病假+产假+丧假+婚假 请假天数
                        ,hsi.hire_date 入职日期
                        ,IF(LEFT(hsi.hire_date,7)=LEFT(date(NOW()-INTERVAL 1 hour),7),'当月入职','非当月入职') 是否当月入职
                        ,if(职位='Inbound',SUM(if(职位='Inbound',应出勤,0) ) over (PARTITION by 'BPL3'),
                                IF(职位 in ('Pack','Pick','Outbound'),SUM(if(职位 in ('Pack','Pick','Outbound'),应出勤,0)) over (PARTITION by 'BPL3'),null)) 部门应出勤
                        ,if(职位='Inbound',SUM(if(职位='Inbound',出勤,0) ) over (PARTITION by 'BPL3'),
                                IF(职位 in ('Pack','Pick','Outbound'),SUM(if(职位 in ('Pack','Pick','Outbound'),出勤,0)) over (PARTITION by 'BPL3'),null)) 部门出勤
                        ,tt.kpi

                    FROM tmpale.tmp_th_ffm_BPL3_stat2 tt  #@TODO 改配载表
                    left join bi_pro.hr_staff_info hsi on tt.ID=hsi.staff_info_id
                    left JOIN
                    (
                        select
                            人员信息
                            ,sum(应出勤) 应出勤
                            ,sum(出勤) 出勤
                            ,sum(迟到) 迟到
                                -- ,sum(请假) 请假
                            ,sum(旷工) 旷工
                            ,sum(年假) 年假
                            ,sum(事假) 事假
                            ,sum(病假) 病假
                            ,sum(产假) 产假
                            ,sum(丧假) 丧假
                            ,sum(婚假) 婚假
                            ,sum(公司培训假) 公司培训假
                        FROM dwm.dwd_th_ffm_staff_dayV2
                        where left(统计日期, 7)=substr(date_sub(now()+ interval -1 hour,interval 1 month), 1,7)
                        group by 人员信息
                    )a on tt.ID=a.人员信息
                )a
                LEFT JOIN
                (
                    select
                        t.warehouse_name
                        ,COUNT(DISTINCT t.delivery_sn)  订单量
                    from dwm.dwd_th_ffm_outbound_dayV2 t
                    where date(t.pack_time) BETWEEN date_sub(date_sub(date_format(now(),'%y-%m-%d'),interval extract(day from now())-1 day),interval 1 month)
                        and date_sub(date_sub(date_format(now(),'%y-%m-%d'),interval extract(day from now()) day),interval 0 month) # @TODO 时间需要更改
                        and t.TYPE ='B2C'
                        and t.warehouse_name ='BPL3'
                        and t.seller_name not in ('FFM-TH', 'Flash -Thailand')
                    group by t.warehouse_name
                )b on b.warehouse_name=a.仓库
                left join
                (
                    select
                        仓库
                        ,SUM(实际上架数量) 实际上架数量
                    from
                    (
                        -- erp部分
                        select
                                    vp.physicalwarehouse_name 仓库
                            ,o.shelf_order_sn 上架单号
                            ,o.shelf_end_time +INTERVAL 7 hour 上架结束时间
                            ,os.in_num 实际上架数量
                            from erp_wms_prod.on_shelf_order o
                            left join erp_wms_prod.on_shelf_order_detail os on os.shelf_order_id =o.id
                            left join erp_wms_prod.warehouse w on w.id =o.warehouse_id
                            left join erp_wms_prod.seller s on o.seller_id = s.id
                        left join tmpale.dim_th_ffm_virtualphysical vp on w.name = vp.virtualwarehouse_name
                        where vp.physicalwarehouse_name='BPL3'
                        and o.status =30
                        and s.name not in ('FFM-TH', 'Flash -Thailand')
                        and date(o.shelf_end_time +INTERVAL 7 hour) 
                            BETWEEN date_sub(date_sub(date_format(now(),'%y-%m-%d'),interval extract(day from now())-1 day),interval 1 month)
                            and date_sub(date_sub(date_format(now(),'%y-%m-%d'),interval extract(day from now()) day),interval 0 month) # @TODO 时间需要更改

                        union all -- wms部分
                        select
                            w.name 仓库
                            ,o.shelf_on_order_sn 上架单号
                            ,o.shelf_on_end 上架结束时间
                            ,soog.number 实际上架数量
                        from wms_production.shelf_on_order o
                        left join wms_production.shelf_on_order_goods soog on o.id = soog.shelf_on_order_id
                        left join wms_production.warehouse w on w.id =o.warehouse_id
                        left join wms_production.seller s on o.seller_id = s.id
                        where w.name = 'BPL3- Bangphli3 Livestream Warehouse'
                        and o.status = 1030
                        and date(o.shelf_on_end) BETWEEN date_sub(date_sub(date_format(now(),'%y-%m-%d'),interval extract(day from now())-1 day),interval 1 month)
                        and date_sub(date_sub(date_format(now(),'%y-%m-%d'),interval extract(day from now()) day),interval 0 month) # @TODO 时间需要更改
                    )a
                    group by 仓库
                )c on c.仓库=a.仓库
            )a
        )a
    )a
)a
    order by 职位,员工ID
        ;