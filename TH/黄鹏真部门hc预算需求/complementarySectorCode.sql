/* 黄鹏真数据，补充字段数据 
日期 2024-02-02

*/

select
    部门ID,
    部门名称,
    职位ID,
    职位名称,
    计划HC数量,
    公司,
    hr.一级部门,
    hr.二级部门,
    hr.三级部门,
    hr.四级部门,
    hs.cnt
from
    tmpale.tmp_th_org_job_department0202 t
    left join `dwm`.`dwd_hr_organizational_structure_detail` hr on t.部门ID = hr.id
    left join (
        select
            dept_id,
            job_title_id,
            sum(count) cnt
        from
            backyard_pro.hr_staffing
        group by
            dept_id,
            job_title_id
    ) hs on t.部门ID = hs.dept_id
    and t.职位ID = hs.job_title_id;