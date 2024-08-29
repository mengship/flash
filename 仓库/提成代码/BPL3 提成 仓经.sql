# BPL3 提成
with att as
(
SELECT
    人员信息
    ,统计日期
    ,一级部门
    ,部门
    ,仓库
    ,职位类别
    ,职位
    ,上班打卡时间
    ,班次开始
    ,下班打卡时间
    ,班次结束
    ,公休日
    ,休息日
    ,应出勤
    ,应出勤-请假-旷工 出勤
    ,请假
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
FROM
    (
    SELECT
        人员信息
        ,统计日期
        ,一级部门
        ,部门
        ,仓库
        ,职位类别
        ,职位
        ,上班打卡时间
        ,班次开始
        ,下班打卡时间
        ,班次结束
        ,公休日
        ,休息日
        ,应出勤
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
    FROM
        (
        SELECT
            人员信息
            ,统计日期
            ,一级部门
            ,部门
            ,仓库
            ,职位类别
            ,职位
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
        FROM
            (
            SELECT
                人员信息
                ,统计日期
                ,一级部门
                ,部门
                ,仓库
                ,职位类别
                ,职位
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
                ,if(shift_start<'12:00','13:00','19:00') 旷工最晚时间
            FROM
                (
                SELECT
                    ad.`staff_info_id` 人员信息
                    ,ad.`stat_date` 统计日期
                    ,sd.`一级部门` 一级部门
                    ,sd.`name` 部门
                    ,case when 二级部门='AGV Warehouse' then 'AGV'
                        when 二级部门='BPL2-Bangphli Return Warehouse' then 'BPL-Return'
                        when 二级部门='Bangphli Livestream Warehouse' then 'BPL3'
                        when 二级部门='BST-Bang Sao Thong Warehouse' then 'BST'
                        when 二级部门='LAS-Lasalle Material Warehouse' then 'LAS'
                    end 仓库
                    ,case
                        when 三级部门 in ('Packing (AGV)','Packaging (LAS-Lasalle Material)','Packing (Bangphli Livestream)','Packing (BST-Bang Sao Thong)','Packing (BPL2)') then 'Packing'
                        when 三级部门 in ('Picking (BPL2)','Picking (BST-Bang Sao Thong)','Picking (AGV)','Picking (Bangphli Livestream)','Picking (LAS-Lasalle Material)') then 'Picking'
                        when 三级部门 in ('Outbound (Bangphli Livestream)','Outbound (AGV)','Outbound (BST-Bang Sao Thong)') then 'Outbound'
                        when 三级部门 in ('Inbound (BST-Bang Sao Thong)','Inbound (Bangphli Livestream)','Inbound (AGV)','Inbound (BPL2)','Inbound (LAS-Lasalle Material)') then 'Inbound'
                        when 三级部门 in ('B2B (BST-Bang Sao Thong)') then 'B2B'
                        else 'HO'
                    end 职位类别
                    ,hjt.`job_name` 职位
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
                    ,if(ad.PH!=0,1,0) 公休日
                    ,if(ad.OFF!=0,1,0) 休息日
                    ,if(ad.PH=0 AND ad.OFF=0,1,0) 应出勤
                    -- ,if(ad.PH=0 AND ad.OFF=0 AND ad.attendance_time>0,1,0) 出勤天数
                    ,case when ad.PH=0 AND ad.OFF=0 AND ad.attendance_time=0 then 0
                        when ad.PH=0 AND ad.OFF=0 AND ad.attendance_time=5 then 0.5
                        when ad.PH=0 AND ad.OFF=0 AND ad.attendance_time=10 then 1
                        end 出勤
                    -- ,if(ad.PH=0 AND ad.OFF=0 AND ad.attendance_time=0,1,0) 未出勤天数
                    ,case when ad.PH=0 AND ad.OFF=0 AND ad.attendance_time=0 then 1
                        when ad.PH=0 AND ad.OFF=0 AND ad.attendance_time=5 then 0.5
                        when ad.PH=0 AND ad.OFF=0 AND ad.attendance_time=10 then 0
                        end 未出勤
                    ,ad.`AB`
                    -- 当天没有上班卡或者下班卡为全天旷工；迟到30分钟内，算作迟到，5泰铢/分钟罚款；迟到超过30分钟，算半天旷工；迟到超过4小时，算全天旷工
                    ,case when ad.PH=0 AND ad.OFF=0
                            AND (ad.`attendance_started_at` is null or ad.`attendance_end_at` is null)
                            AND ad.leave_type not in (1,2,12,3,18,4,5,17,7,10,16,19) then 1
                        when ad.PH=0 AND ad.OFF=0
                            AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))>0
                            AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))<=30  then 0
                        when ad.PH=0 AND ad.OFF=0
                            AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))>30
                            AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))<=(UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_end`,':','00' ))-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' )))/2  then 0.5
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
                LEFT JOIN `fle_staging`.`staff_info` si on si.`id` =ad.`staff_info_id`
                LEFT JOIN `fle_staging`.`sys_store` ss on ss.`id` =si.`organization_id`
                LEFT JOIN `bi_pro`.`hr_job_title` hjt on hjt.`id` =si.`job_title`
                -- LEFT JOIN `fle_staging`.`sys_department` sd on sd.`id` =si.`department_id`
                LEFT JOIN `dwm`.`dwd_hr_organizational_structure_detail` sd ON sd.`id`=si.`department_id`
                WHERE sd.`一级部门`='Thailand Fulfillment'
                AND ad.`stat_date` BETWEEN date_sub(date_sub(date_format(now(),'%y-%m-%d'),interval extract(day from now())-1 day),interval 1 month)
                                        and date_sub(date_sub(date_format(now(),'%y-%m-%d'),interval extract(day from now()) day),interval 0 month) # @TODO 时间需要改 整月时间

                ) ad
            ) ad
        ) ad
    ) ad
ORDER BY 统计日期
)
SELECT
    仓库
    ,员工ID
    ,职位
    ,人效
    ,目标值
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
    ,请假天数
    ,入职日期
    ,是否当月入职
    ,部门应出勤
    ,部门出勤
    ,工作量
    ,订单量
    ,实际上架数量
    ,round(调整目标值, 2) 目标
    ,round(超额总工作量) 超额总工作量
    ,round(超额总提成) 超额总提成
    ,round(超额提成) 超额提成
    ,round(基础提成2) 个人提成
    ,全勤奖
    ,考勤系数
    ,迟到扣款
    ,round(基础提成2*考勤系数-迟到*5+全勤奖, 2) 考勤提成
    ,kpi kpi系数
    ,round((基础提成2*考勤系数-迟到*5+全勤奖)*kpi, 2) 最终提成
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
                                    ,IF(职位='Inbound',人效*3*26,IF(职位 in ('Pack','Pick','Outbound'),人效*18*26,null)) 目标值 # @TODO inbound 3 人, 'Pack','Pick','Outbound'共 18 人,需要进行调整
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
                            left join
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
                                    FROM att
                                    group by 人员信息
                                    )a on tt.ID=a.人员信息
                    --         left join
                    --                 (
                    --                 select
                    --                         职位
                    --                         ,SUM(应出勤) 部门应出勤
                    --                         ,SUM(出勤)  部门出勤
                    --                 from att
                    --                 )b on b.职位=tt.职位
                            )a
                    LEFT join
                            (
                            select
                                    t.仓库
                                    ,COUNT(DISTINCT t.发货单号)  订单量
                            from test.th_ffm_outbound_total_detail_l t
                            where date(t.打包完成时间) BETWEEN date_sub(date_sub(date_format(now(),'%y-%m-%d'),interval extract(day from now())-1 day),interval 1 month)
                                    and date_sub(date_sub(date_format(now(),'%y-%m-%d'),interval extract(day from now()) day),interval 0 month) # @TODO 时间需要更改
                                    and t.类型 ='erp发货单'
                                    and t.仓库 ='BPL3'
                            group by t.仓库
                            )b on b.仓库=a.仓库
                    left join
                            (
                            select
                                    仓库
                                    ,SUM(实际上架数量) 实际上架数量
                            from
                                    (
                                    select
                                            vp.physicalwarehouse_name 仓库
                                    ,o.shelf_order_sn 上架单号
                                    ,o.shelf_end_time +INTERVAL 7 hour 上架结束时间
                                    ,os.in_num 实际上架数量
                                    from erp_wms_prod.on_shelf_order o
                                    left join erp_wms_prod.on_shelf_order_detail os on os.shelf_order_id =o.id
                                    left join erp_wms_prod.warehouse w on w.id =o.warehouse_id
                                left join tmpale.dim_th_ffm_virtualphysical vp on w.name = vp.virtualwarehouse_name
                                where vp.physicalwarehouse_name='BPL3'
                                and o.status =30
                                and date(o.shelf_end_time +INTERVAL 7 hour) 
                                    BETWEEN date_sub(date_sub(date_format(now(),'%y-%m-%d'),interval extract(day from now())-1 day),interval 1 month)
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