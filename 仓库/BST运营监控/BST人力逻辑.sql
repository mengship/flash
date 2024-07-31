-- BST人力逻辑
select
    统计日期,
    rn,
    count(员工ID) 在职人数,
    sum(应出勤) 应出勤,
    sum(今日出勤) 实际出勤,
    round(sum(今日出勤) / count(员工ID), 4) `出勤率(实际/在职)`,
    round(if(sum(应出勤) = 0, 0, sum(今日出勤) / sum(应出勤)), 4) `出勤率(实际/应出勤)`
from
    dwm.dwd_th_ffm_staff_day
where
    仓库 = 'BST'
    and 员工状态 = '在职'
    and 统计日期 >= date_sub(date(now() + interval -1 hour), interval 30 day)
    and 统计日期 < date(now() + interval -1 hour)
group by
    统计日期,
    rn
order by
    统计日期 desc