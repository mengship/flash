select
月份,
人员信息,
入职日期,
离职日期,
职位,
应出勤,
出勤,
迟到,
旷工,
年假,
事假,
病假,
产假,
丧假,
婚假,
公司培训假,
跨国探亲假
from
    (
        select
            left(统计日期, 7) 月份,
            人员信息,
            入职日期,
            离职日期,
            职位,
            在职状态,
            sum(应出勤) 应出勤,
            sum(出勤) 出勤,
            sum(迟到) 迟到,
            sum(旷工) 旷工,
            sum(年假) 年假,
            sum(事假) 事假,
            sum(病假) 病假,
            sum(产假) 产假,
            sum(丧假) 丧假,
            sum(婚假) 婚假,
            sum(公司培训假) 公司培训假,
            sum(跨国探亲假) 跨国探亲假
        from
            (
                SELECT
                    人员信息,
                    统计日期,
                    一级部门,
                    部门,
                    仓库,
                    职位类别,
                    职位,
                    入职日期,
                    离职日期,
                    在职,
                    上班打卡时间,
                    班次开始,
                    下班打卡时间,
                    班次结束,
                    公休日,
                    休息日,
                    应出勤,
                    应出勤 - 请假 - 旷工 出勤 ,
                    请假 ,
                    if(迟到 > 0, floor(迟到), 0) 迟到,
                    旷工,
                    年假,
                    事假,
                    病假,
                    产假,
                    丧假,
                    婚假,
                    公司培训假,
                    跨国探亲假,
                    旷工最晚时间,
                    now() + interval -1 hour update_time,
                    在职状态
                FROM
                    (
                        SELECT
                            人员信息,
                            统计日期,
                            一级部门,
                            部门,
                            仓库,
                            职位类别,
                            职位,
                            入职日期,
                            离职日期,
                            在职,
                            上班打卡时间,
                            班次开始,
                            下班打卡时间,
                            班次结束,
                            公休日,
                            休息日,
                            应出勤 ,
                            请假,
                            请假时段,
                            case
                                when 应出勤 = 1
                                AND (
                                    上班打卡时间 is null
                                    or 下班打卡时间 is null
                                )
                                AND 请假 = 0 then 0
                                when 应出勤 = 1
                                AND (
                                    上班打卡时间 is null
                                    or 下班打卡时间 is null
                                )
                                AND 请假 = 0.5 then 0
                                when 应出勤 = 1
                                AND 请假 = 1 then 0
                                when 应出勤 = 1
                                AND 请假 = 0.5
                                AND 请假时段 = '上午半天假'
                                AND (UNIX_TIMESTAMP(上班打卡时间) - UNIX_TIMESTAMP(旷工最晚时间)) / 60 <= 120 then (UNIX_TIMESTAMP(上班打卡时间) - UNIX_TIMESTAMP(旷工最晚时间)) / 60
                                when 应出勤 = 1
                                AND 请假 = 0.5
                                AND 请假时段 = '上午半天假'
                                AND (UNIX_TIMESTAMP(上班打卡时间) - UNIX_TIMESTAMP(旷工最晚时间)) / 60 > 120 then 0
                                when 应出勤 = 1
                                AND 请假 = 0.5
                                AND 请假时段 = '下午半天假'
                                AND (UNIX_TIMESTAMP(上班打卡时间) - UNIX_TIMESTAMP(班次开始)) / 60 <= 120 then (UNIX_TIMESTAMP(上班打卡时间) - UNIX_TIMESTAMP(班次开始)) / 60
                                when 应出勤 = 1
                                AND 请假 = 0.5
                                AND 请假时段 = '下午半天假'
                                AND (UNIX_TIMESTAMP(上班打卡时间) - UNIX_TIMESTAMP(班次开始)) / 60 > 120 then 0
                                when 应出勤 = 1
                                AND 请假 = 0
                                AND (UNIX_TIMESTAMP(上班打卡时间) - UNIX_TIMESTAMP(班次开始)) / 60 <= 120 then (UNIX_TIMESTAMP(上班打卡时间) - UNIX_TIMESTAMP(班次开始)) / 60
                                when 应出勤 = 1
                                AND 请假 = 0
                                AND (UNIX_TIMESTAMP(上班打卡时间) - UNIX_TIMESTAMP(班次开始)) / 60 > 120 then 0
                                when 应出勤 = 1
                                AND 请假 = 0
                                AND (UNIX_TIMESTAMP(上班打卡时间) - UNIX_TIMESTAMP(旷工最晚时间)) / 60 > 0 then 0
                                else 0
                            end 迟到 -- 当天没有上班卡或者下班卡为全天旷工；迟到120分钟内，算作迟到，5泰铢/分钟罚款；迟到超过120分钟，算半天旷工；迟到超过13:00或19:00，算全天旷工
                                ,
                                case
                                when 应出勤 = 1
                                AND (
                                    上班打卡时间 is null
                                    or 下班打卡时间 is null
                                )
                                AND 请假 = 0 then 1
                                when 应出勤 = 1
                                AND (
                                    上班打卡时间 is null
                                    or 下班打卡时间 is null
                                )
                                AND 请假 = 0.5 then 0.5
                                when 应出勤 = 1
                                AND 请假 = 1 then 0
                                when 应出勤 = 1
                                AND 请假 = 0.5
                                AND 请假时段 = '上午半天假'
                                AND (UNIX_TIMESTAMP(上班打卡时间) - UNIX_TIMESTAMP(旷工最晚时间)) / 60 <= 120 then 0
                                when 应出勤 = 1
                                AND 请假 = 0.5
                                AND 请假时段 = '上午半天假'
                                AND (UNIX_TIMESTAMP(上班打卡时间) - UNIX_TIMESTAMP(旷工最晚时间)) / 60 > 120 then 0.5
                                when 应出勤 = 1
                                AND 请假 = 0.5
                                AND 请假时段 = '下午半天假'
                                AND (UNIX_TIMESTAMP(上班打卡时间) - UNIX_TIMESTAMP(班次开始)) / 60 <= 120 then 0
                                when 应出勤 = 1
                                AND 请假 = 0.5
                                AND 请假时段 = '下午半天假'
                                AND (UNIX_TIMESTAMP(上班打卡时间) - UNIX_TIMESTAMP(班次开始)) / 60 > 120 then 0.5
                                when 应出勤 = 1
                                AND 请假 = 0
                                AND (UNIX_TIMESTAMP(上班打卡时间) - UNIX_TIMESTAMP(班次开始)) / 60 <= 120 then 0
                                when 应出勤 = 1
                                AND 请假 = 0
                                AND (UNIX_TIMESTAMP(上班打卡时间) - UNIX_TIMESTAMP(班次开始)) / 60 > 120 then 0.5
                                when 应出勤 = 1
                                AND 请假 = 0
                                AND (UNIX_TIMESTAMP(上班打卡时间) - UNIX_TIMESTAMP(旷工最晚时间)) / 60 > 0 then 1
                                else 0
                            end 旷工,
                            年假,
                            事假,
                            病假,
                            产假,
                            丧假,
                            婚假,
                            公司培训假,
                            跨国探亲假,
                            旷工最晚时间,
                            在职状态
                        FROM
                            (
                                SELECT
                                    人员信息,
                                    统计日期,
                                    一级部门,
                                    部门,
                                    仓库,
                                    职位类别,
                                    职位,
                                    入职日期,
                                    离职日期,
                                    在职,
                                    上班打卡时间,
                                    班次开始,
                                    下班打卡时间,
                                    班次结束,
                                    公休日,
                                    休息日,
                                    应出勤,
                                    出勤,
                                    未出勤,
                                    请假,
                                    请假时段,
                                    年假,
                                    事假,
                                    病假,
                                    产假,
                                    丧假,
                                    婚假,
                                    公司培训假,
                                    跨国探亲假,
                                    concat(统计日期, ' ', 旷工最晚时间, ':', '00') 旷工最晚时间,
                                    在职状态
                                FROM
                                    (
                                        SELECT
                                            人员信息,
                                            统计日期,
                                            一级部门,
                                            部门,
                                            仓库,
                                            职位类别,
                                            职位,
                                            入职日期,
                                            离职日期,
                                            在职,
                                            上班打卡时间,
                                            班次开始,
                                            下班打卡时间,
                                            班次结束,
                                            公休日,
                                            休息日,
                                            应出勤,
                                            出勤,
                                            未出勤,
                                            请假,
                                            请假时段,
                                            年假,
                                            事假,
                                            病假,
                                            产假,
                                            丧假,
                                            婚假,
                                            公司培训假,
                                            跨国探亲假,
                                            if(shift_start < '12:00', '13:00', '19:00') 旷工最晚时间,
                                            在职状态
                                        FROM
                                            (
                                                SELECT
                                                    ad.`staff_info_id` 人员信息,
                                                    ad.`stat_date` 统计日期,
                                                    sd.`一级部门` 一级部门,
                                                    sd.`二级部门` 二级部门,
                                                    sd.`name` 部门,
                                                    case
                                                        when ad.`staff_info_id`='664339' then 'LCP' # 饶文齐的二级部门存在问题，手动修复
                                                        when 二级部门 = 'AGV Warehouse' then 'AGV'
                                                        when 二级部门 = 'BPL2-Bangphli Return Warehouse' then 'BPL-Return'
                                                        when 二级部门 = 'Bangphli Livestream Warehouse' then 'BPL3'
                                                        when 二级部门 = 'BST-Bang Sao Thong Warehouse' then 'BST'
                                                        when 二级部门 = 'LAS-Lasalle Material Warehouse' then 'LAS'
                                                        when 二级部门 = 'LCP Warehouse' then 'LCP'
                                                        -- when 二级部门 = 'Warehouse Audit' then 'Audit'
                                                    end 仓库,
                                                    case
                                                        when left(三级部门, 4) = 'Pack' then 'Packing'
                                                        when left(三级部门, 4) = 'Pick' then 'Picking'
                                                        when left(三级部门, 3) = 'Out' then 'Outbound'
                                                        when left(三级部门, 3) = 'Inb' then 'Inbound'
                                                        when left(三级部门, 3) = 'B2B' then 'B2B'
                                                        else 'HO'
                                                    end 职位类别,
                                                    hire_date 入职日期,
                                                    leave_date 离职日期,
                                                    case
                                                        when ad.state = '1' then 1
                                                        else 0
                                                    end 在职,
                                                    hjt.`job_name` 职位,
                                                    ad.`shift_start`,
                                                    ad.`shift_end`,
                                                    ad.`attendance_started_at` as 上班打卡时间,
                                                    if(
                                                        ad.`shift_start` <> '',
                                                        concat(ad.`stat_date`, ' ', ad.`shift_start`, ':', '00'),
                                                        null
                                                    ) as 班次开始,
                                                    ad.`attendance_end_at` as 下班打卡时间,
                                                    if(
                                                        ad.`shift_end` <> '',
                                                        concat(ad.`stat_date`, ' ', ad.`shift_end`, ':', '00'),
                                                        null
                                                    ) as 班次结束,
                                                    case
                                                        ad.leave_time_type
                                                        when 1 then '上午半天'
                                                        when 2 then '下午半天'
                                                        when 3 then '全天'
                                                    end 请假时段,
                                                    ad.attendance_time,
                                                    if(ad.PH != 0, 1, 0) 公休日,
                                                    if(ad.OFF != 0, 1, 0) 休息日,
                                                    if(
                                                        ad.PH = 0
                                                        AND ad.OFF = 0,
                                                        1,
                                                        0
                                                    ) 应出勤 ,
                                                    case
                                                        when ad.PH = 0
                                                        AND ad.OFF = 0
                                                        AND ad.attendance_time = 0 then 0
                                                        when ad.PH = 0
                                                        AND ad.OFF = 0
                                                        AND ad.attendance_time = 5 then 0.5
                                                        when ad.PH = 0
                                                        AND ad.OFF = 0
                                                        AND ad.attendance_time = 10 then 1
                                                    end 出勤 ,
                                                    case
                                                        when ad.PH = 0
                                                        AND ad.OFF = 0
                                                        AND ad.attendance_time = 0 then 1
                                                        when ad.PH = 0
                                                        AND ad.OFF = 0
                                                        AND ad.attendance_time = 5 then 0.5
                                                        when ad.PH = 0
                                                        AND ad.OFF = 0
                                                        AND ad.attendance_time = 10 then 0
                                                    end 未出勤,
                                                    ad.`AB` ,
                                                    case
                                                        when ad.PH = 0
                                                        AND ad.OFF = 0
                                                        AND (
                                                            ad.`attendance_started_at` is null
                                                            or ad.`attendance_end_at` is null
                                                        )
                                                        AND ad.leave_type not in (1, 2, 12, 3, 18, 4, 5, 17, 7, 10, 16, 19) then 1
                                                        when ad.PH = 0
                                                        AND ad.OFF = 0
                                                        AND UNIX_TIMESTAMP(ad.`attendance_started_at`) - UNIX_TIMESTAMP(
                                                            concat(ad.`stat_date`, ' ', ad.`shift_start`, ':', '00')
                                                        ) > 0
                                                        AND UNIX_TIMESTAMP(ad.`attendance_started_at`) - UNIX_TIMESTAMP(
                                                            concat(ad.`stat_date`, ' ', ad.`shift_start`, ':', '00')
                                                        ) <= 30 then 0 -- when ad.PH=0 AND ad.OFF=0 
                                                        --  AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))>30
                                                        --  AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))<=240  then 0.5
                                                        when ad.PH = 0
                                                        AND ad.OFF = 0
                                                        AND UNIX_TIMESTAMP(ad.`attendance_started_at`) - UNIX_TIMESTAMP(
                                                            concat(ad.`stat_date`, ' ', ad.`shift_start`, ':', '00')
                                                        ) > 30
                                                        AND UNIX_TIMESTAMP(ad.`attendance_started_at`) - UNIX_TIMESTAMP(
                                                            concat(ad.`stat_date`, ' ', ad.`shift_start`, ':', '00')
                                                        ) <=(
                                                            UNIX_TIMESTAMP(
                                                                concat(ad.`stat_date`, ' ', ad.`shift_end`, ':', '00')
                                                            ) - UNIX_TIMESTAMP(
                                                                concat(ad.`stat_date`, ' ', ad.`shift_start`, ':', '00')
                                                            )
                                                        ) / 2 then 0.5 -- when ad.PH=0 AND ad.OFF=0 
                                                        --  AND UNIX_TIMESTAMP(ad.`attendance_started_at`)-UNIX_TIMESTAMP(concat(ad.`stat_date`,' ',ad.`shift_start`,':','00' ))>240 then 1
                                                        when ad.PH = 0
                                                        AND ad.OFF = 0
                                                        AND UNIX_TIMESTAMP(ad.`attendance_started_at`) - UNIX_TIMESTAMP(
                                                            concat(ad.`stat_date`, ' ', ad.`shift_start`, ':', '00')
                                                        ) >(
                                                            UNIX_TIMESTAMP(
                                                                concat(ad.`stat_date`, ' ', ad.`shift_end`, ':', '00')
                                                            ) - UNIX_TIMESTAMP(
                                                                concat(ad.`stat_date`, ' ', ad.`shift_start`, ':', '00')
                                                            )
                                                        ) / 2 then 1
                                                        else 0
                                                    end 旷工,
                                                    case
                                                        when ad.leave_type in (1, 2, 12, 3, 18, 4, 5, 17, 7, 10, 16, 19)
                                                        AND ad.leave_time_type = 1 then 0.5
                                                        when ad.leave_type in (1, 2, 12, 3, 18, 4, 5, 17, 7, 10, 16, 19)
                                                        AND ad.leave_time_type = 2 then 0.5
                                                        when ad.leave_type in (1, 2, 12, 3, 18, 4, 5, 17, 7, 10, 16, 19)
                                                        AND ad.leave_time_type = 3 then 1
                                                        else 0
                                                    end 请假,
                                                    case
                                                        when ad.leave_type = 1
                                                        and ad.leave_time_type = 1 then 0.5
                                                        when ad.leave_type = 1
                                                        and ad.leave_time_type = 2 then 0.5
                                                        when ad.leave_type = 1
                                                        and ad.leave_time_type = 3 then 1
                                                        else 0
                                                    end 年假,
                                                    case
                                                        -- 带薪,不带薪事假
                                                        when ad.leave_type in (2, 12)
                                                        and ad.leave_time_type = 1 then 0.5
                                                        when ad.leave_type in (2, 12)
                                                        and ad.leave_time_type = 2 then 0.5
                                                        when ad.leave_type in (2, 12)
                                                        and ad.leave_time_type = 3 then 1
                                                        else 0
                                                    end 事假,
                                                    case
                                                        -- 带薪,不带薪病假
                                                        when ad.leave_type in (3, 18)
                                                        and ad.leave_time_type = 1 then 0.5
                                                        when ad.leave_type in (3, 18)
                                                        and ad.leave_time_type = 2 then 0.5
                                                        when ad.leave_type in (3, 18)
                                                        and ad.leave_time_type = 3 then 1
                                                        else 0
                                                    end 病假,
                                                    case
                                                        -- 产假,陪产假,产检
                                                        when ad.leave_type in (4, 5, 17)
                                                        and ad.leave_time_type = 1 then 0.5
                                                        when ad.leave_type in (4, 5, 17)
                                                        and ad.leave_time_type = 2 then 0.5
                                                        when ad.leave_type in (4, 5, 17)
                                                        and ad.leave_time_type = 3 then 1
                                                        else 0
                                                    end 产假,
                                                    case
                                                        when ad.leave_type = 7
                                                        and ad.leave_time_type = 1 then 0.5
                                                        when ad.leave_type = 7
                                                        and ad.leave_time_type = 2 then 0.5
                                                        when ad.leave_type = 7
                                                        and ad.leave_time_type = 3 then 1
                                                        else 0
                                                    end 丧假,
                                                    case
                                                        when ad.leave_type = 10
                                                        and ad.leave_time_type = 1 then 0.5
                                                        when ad.leave_type = 10
                                                        and ad.leave_time_type = 2 then 0.5
                                                        when ad.leave_type = 10
                                                        and ad.leave_time_type = 3 then 1
                                                        else 0
                                                    end 婚假,
                                                    case
                                                        when ad.leave_type = 16
                                                        and ad.leave_time_type = 1 then 0.5
                                                        when ad.leave_type = 16
                                                        and ad.leave_time_type = 2 then 0.5
                                                        when ad.leave_type = 16
                                                        and ad.leave_time_type = 3 then 1
                                                        else 0
                                                    end 公司培训假,
                                                    case
                                                        when ad.leave_type = 19
                                                        and ad.leave_time_type = 1 then 0.5
                                                        when ad.leave_type = 19
                                                        and ad.leave_time_type = 2 then 0.5
                                                        when ad.leave_type = 19
                                                        and ad.leave_time_type = 3 then 1
                                                        else 0
                                                    end 跨国探亲假,
                                                    if(si.`state` = 1 , 1, 0) 在职状态
                                                FROM
                                                    `bi_pro`.`attendance_data_v2` ad
                                                    LEFT JOIN `fle_staging`.`staff_info` si on si.`id` = ad.`staff_info_id`
                                                    LEFT JOIN `fle_staging`.`sys_store` ss on ss.`id` = si.`organization_id`
                                                    LEFT JOIN `bi_pro`.`hr_job_title` hjt on hjt.`id` = si.`job_title` -- LEFT JOIN `fle_staging`.`sys_department` sd on sd.`id` =si.`department_id` 
                                                    LEFT JOIN `dwm`.`dwd_hr_organizational_structure_detail` sd ON sd.`id` = si.`department_id`
                                                WHERE
                                                    sd.`一级部门` = 'Thailand Fulfillment'
                                                        -- 上个月的第一天
                                                    AND ad.`stat_date` >= date_sub(date_sub(date_format(now(),'%y-%m-%d'),interval extract(day from now())-1 day),interval 1 month)
                                                        -- 上个月的最后一天
                                                    and ad.`stat_date` <= date_sub(date_sub(date_format(now(),'%y-%m-%d'),interval extract(day from now()) day),interval 0 month)
                                                         

                                            ) ad
                                    ) ad
                            ) ad
                    ) ad -- where 人员信息='687216' and left(统计日期,7)='2024-02'
            ) OA
        where
            1=1
            and 仓库 in ('LCP')
            and 职位 not in(
                        'Central Control Manager',
                        'Warehouse Deputy Manager',
                        'Warehouse Manager',
                        'Inventory Control Supervisor',
                        'Operations Central Control Manager',
                        'Internship'
                        )
            -- and 人员信息='664339'
        group by
            1,
            2,
            3,
            4,
            5,
            6
    )
where
    应出勤 = 出勤
    and 迟到 <= 0
    and (
        在职状态 = 1
        or 离职日期 >= DATE_ADD(concat(月份, '-01'), interval 1 month)
    )
    and 入职日期 <= concat(月份, '-01')
    -- and 人员信息 = '664339'
ORDER BY
    应出勤 desc