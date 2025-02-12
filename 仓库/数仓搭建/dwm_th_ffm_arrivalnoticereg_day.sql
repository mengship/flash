/*=====================================================================+
表名称：  dwm_th_ffm_arrivalnoticereg_day
功能描述：泰国入库 入库单到货表（天粒度汇总）
                        
需求来源：
编写人员: 王昱棋
设计日期：2024/10/25
        修改日期: 
        修改人员:     
        修改原因: 
-----------------------------------------------------------------------
---存在问题：
-----------------------------------------------------------------------
+=====================================================================*/ 
      

-- 入库单明细表
drop table if exists dwm.dwm_th_ffm_arrivalnoticereg_day;
create table dwm.dwm_th_ffm_arrivalnoticereg_day as
-- delete from dwm.dwm_th_ffm_arrivalnoticereg_day where dt >= date_sub(date(now() + interval -1 hour),interval 90 day); -- 先删除数据
-- insert into dwm.dwm_th_ffm_arrivalnoticereg_day -- 再插入数据
select 
    LEFT(reg_time,10) 日期
    ,仓库名称 仓库
    ,'到货单量' 指标
    ,count(if(单据='采购订单', notice_number, null)) 采购订单到货单量
    ,count(if(单据='销退订单', notice_number, null)) 销退订单到货单量
FROM dwm.dwd_th_ffm_arrivalnotice_dayV2 
WHERE 1=1
-- and reg_time >= date_sub(date(now() + interval -1 hour),interval 90 day)
group by 1,2,3