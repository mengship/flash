# BPL Return
select 仓库
     , 工号
     , 职位
     , 总工作量
     , 目标值
     , 系数
     , 考核天数
     , 应出勤
     , 出勤
     , 调整目标
#      ,工作量提成
     ,round(case when 职位='Acting supervisor' then (sum(工作量提成) over(partition by 1)) / (count(1) over(partition by 1) - 1)
         else 工作量提成 end , 2) as 工作量提成
,是否全勤
,if(是否全勤='是',800,0) 全勤奖
,round(case when 职位='Acting supervisor' then (sum(工作量提成) over(partition by 1)) / (count(1) over(partition by 1) - 1)
         else 工作量提成 end + if(是否全勤='是',800,0), 2) 基础提成
,请假天数
,出勤/应出勤
,round((case when 职位='Acting supervisor' then (sum(工作量提成) over(partition by 1)) / (count(1) over(partition by 1) - 1)
         else 工作量提成 end + if(是否全勤='是',800,0) )*出勤/应出勤, 2) 考勤提成

from (
         select 仓库
              , 工号
              , 职位
              , 总工作量
              , 目标值
              , 系数
              , 考核天数
              , 应出勤
              , 出勤
              , 调整目标
              , round((总工作量 - (sum(调整目标) over (partition by 修正职位))) * 系数 * 出勤 / (sum(出勤) over (partition by 修正职位)), 2) 工作量提成
              ,修正职位
         ,sum(调整目标) over (partition by 修正职位)
         ,是否全勤
         ,请假天数
         from (
                  select bs.仓库
                       , bs.工号
                       , bs.职位
                       , case when bs.职位='Pack' then 'PackPick'
                           when bs.职位='Pick' then 'PackPick'
                               else bs.职位 end as 修正职位
                       , case
                             when bs.职位 = 'Acting supervisor' then null
                             when bs.职位 = 'Inbound' then sum(inbound.工作量) over (partition by 1)
                             when bs.职位 in ('Pack', 'Pick') then sum(outbound.工作量) over (partition by 1) end as 总工作量
                       , bs.目标值
                       , bs.系数
                       , bs.考核天数
                       , staff.应出勤
                       , staff.出勤
                       , round((bs.目标值 * staff.出勤) / bs.考核天数, 2)                                                          调整目标
                        ,staff.是否全勤
                        ,staff.请假天数
                  from tmpale.tmp_th_ffm_BPLReturn_stat2 bs
                           left join
                       (
                           SELECT 月份
                                , 工号
                                , sum(罚款总金额)                        罚款总金额
                                , sum(if(罚款类别一 = '业务', 罚款总金额, 0))   业务罚款
                                , sum(if(罚款类别一 = '现场管理', 罚款总金额, 0)) 现场管理罚款
                                , sum(if(罚款类别一 = '考勤', 罚款总金额, 0))   考勤罚款
                           FROM (
                                    SELECT fb.`fine_sn`                                         罚款编号
                                         , case fb.`status`
                                               when 0 then '初始化'
                                               when 1 then '创建'
                                               when 2 then '审核'
                                               when 3 then '作废'
                                        end                                                     状态
                                         , mb.`job_number`                                      工号
                                         , mb.`name`                                            姓名
                                         , wh.`name`                                            所属仓库
                                         , wb.`name`                                            罚款类别一
                                         , wbd.`zh`                                             罚款类别二
                                         , fb.`biz_order_sn`                                    关联单号
                                         , fb.`biz_date`                                        业务发生日期
                                         , fb.`amount` * 0.01                                   预罚款金额
                                         , fb.`reason`                                          罚款原因
                                         , case fb.`adjust_type`
                                               when 0 then '增加'
                                               when 1 then '减少'
                                        end                                                     调整类型
                                         , fb.`adjust_amount`                                   调整金额
                                         , fb.`adjust_reason`                                   调整原因
                                         , (fb.`amount` + fb.`adjust_amount`) * 0.01            罚款总金额
                                         , fb.`creator_name`                                    创建人
                                         , left(date_add(fb.`created`, interval -60 minute), 7) 月份
                                         , date_add(fb.`created`, interval -60 minute)          创建时间
                                         , fb.`auditor_id`                                      审核人
                                         , fb.`audited`                                         审核时间
                                         , fb.`remark`                                          备注
                                    FROM `wms_production`.`fine_bill` fb
                                             LEFT JOIN `wms_production`.`wordbook_detail` wbd on wbd.`id` = fb.`type`
                                             LEFT JOIN `wms_production`.`wordbook` wb on wb.`id` = wbd.`wordbook_id`
                                             LEFT JOIN `wms_production`.`warehouse` wh on wh.`id` = fb.`warehouse_id`
                                             LEFT JOIN `wms_production`.`member` mb on mb.`id` = fb.`member_id`
                                    WHERE fb.`status` != 3
                                      and left(date_sub(now() + interval -1 hour, interval 1 month), 7) =
                                          left(date_add(fb.`created`, interval -60 minute), 7)
                                ) fb
                           GROUP BY 月份
                                  , 工号
                       ) fine on bs.工号 = fine.工号
                           left join (
                      SELECT left(统计日期, 7) 月份
                           , 仓库            物理仓
                           , 人员信息
                           , 职位类别
                           , sum(应出勤)      应出勤
                           , sum(出勤)       出勤
                           , sum(迟到)       迟到
                           -- ,sum(请假) 请假
                           , sum(旷工)       旷工
                           , sum(年假)       年假
                           , sum(事假)       事假
                           , sum(病假)       病假
                           , sum(产假)       产假
                           , sum(丧假)       丧假
                           , sum(婚假)       婚假
                           , sum(公司培训假)    公司培训假
                           , if(sum(出勤)=sum(应出勤), '是', '否') 是否全勤
                            , sum(年假) + sum(事假) + sum(病假) + sum(产假) + sum(丧假) + sum(婚假) 请假天数
                      FROM (
                               SELECT *
                               FROM `dwm`.`dwd_th_ffm_attendance_detail1` # @TODO 修改时间
                           ) ad
                      where left(统计日期, 7) = left(date_sub(now() + interval -1 hour, interval 1 month), 7)
                      GROUP BY 仓库
                             , 人员信息
                             , 部门
                             , 职位类别
                  ) staff on staff.人员信息 = bs.工号
                           left join (
                      SELECT left(出库完成时间, 7)     月份
                           , 物理仓
                           , 复核人ID
                           , count(distinct 箱单号) 工作量
                      FROM dwm.dwd_th_ffm_outbound_detail1
                      WHERE 订单状态 != 1000
                        AND 业务类型 in ('发货', '出库')
                        AND 物理仓 = 'BPL-Return'
                        and left(出库完成时间, 7) = left(date_sub(now() + interval -1 hour, interval 1 month), 7) # @TODO 修改时间
                      GROUP BY left(出库完成时间, 7)
                             , 物理仓
                             , 复核人ID
                  ) outbound on outbound.复核人ID = bs.工号
                           left join (
                      SELECT left(收货完成时间, 7) 月份
                           , 物理仓
                           , 收货人ID
                           , case
                                 when 物理仓 = 'BPL-Return' then sum(if(业务类型 = '包裹销退', 修正商品数量, 0))
                                 else sum(if(业务类型 = '采购入库', 修正商品数量, 0))
                          end                工作量
                      FROM dwm.dwd_th_ffm_inbound_detail1
                      WHERE 物理仓 = 'BPL-Return'
                        and left(收货完成时间, 7) = left(date_sub(now() + interval -1 hour, interval 1 month), 7) # @TODO 修改时间
                      GROUP BY left(收货完成时间, 7)
                             , 物理仓
                             , 收货人ID
                  ) inbound on inbound.收货人ID = bs.工号
                  order by bs.职位, bs.工号
              ) t0
         order by 职位, 工号
     ) t1
order by 职位, 工号