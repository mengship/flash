# 注意修改每月小时工扣减金额
with inbound as
(
SELECT
          SUM(商品数量2) 入库数量
FROM
    (
    SELECT
              -- 时区转换；状态
              an.`notice_number` 订单号
              ,s2.name 货主
              ,an.`id` 订单ID
              ,'入库单' 订单类型
              ,case an.`from_order_type`
                  when 1 then '采购入库'
                  when 2 then '调拨入库'
                  when 3 then '退货入库'
                  when 4 then '其他入库'
                  end 业务类型
              ,an.`complete_time` 收货完成时间
              ,mb.`job_number` 收货人ID
              ,mb.`real_name` 收货人名称
              ,wh.`name` 虚拟仓
              ,case
                  when  wh.`name`='BKK-WH-LAS2电商仓' then 'LAS'
              end 物理仓
              ,an.`status`  订单状态
              ,ang.`SKU` SKU
              ,ang.`通知数量` 通知数量
              ,ang.`商品数量` 商品数量
              ,CASE s2.name when 'TCL' THEN ang.`商品数量`*6
                              when 'Intrepid - Tefal' then ang.`商品数量`*2
                              else ang.`商品数量`
                      end 商品数量2
              ,ang.`修正商品数量` 修正商品数量
    FROM `wms_production`.`arrival_notice` an
    LEFT JOIN wms_production.seller s2 on an.seller_id=s2.id
    LEFT JOIN `wms_production`.`member` mb on an.`complete_id`=mb.id
    LEFT JOIN `wms_production`.`warehouse` wh on wh.`id`=an.`warehouse_id`
    LEFT JOIN
        (
        SELECT
            ang.`arrival_notice_id`
            ,count(distinct ang.`seller_goods_id`) SKU
            ,sum(ang.`通知数量`) 通知数量
            ,sum(ang.`goods_number`) 商品数量
            ,sum(ang.`three_num`+ang.`two_num`+ang.`one_num`) 修正商品数量
        FROM
            (
            SELECT
                arrival_notice_id
                ,seller_goods_id
                ,通知数量
                ,goods_number
                ,two_conversion
                ,three_conversion
                ,if(three_conversion>0, floor(goods_number / three_conversion), 0) three_num
                ,case when three_conversion>0 AND two_conversion>0 then floor(goods_number % three_conversion / two_conversion)
                    when three_conversion>0 AND two_conversion=0 then floor(goods_number / three_conversion)
                    when three_conversion=0 AND two_conversion>0 then floor(goods_number / two_conversion)
                    else 0
                    end two_num
                ,case when three_conversion>0 AND two_conversion>0 then (goods_number % three_conversion % two_conversion)
                    when three_conversion>0 AND two_conversion=0 then (goods_number % three_conversion)
                    when three_conversion=0 AND two_conversion>0 then (goods_number % two_conversion)
                    else goods_number
                    end one_num
            FROM
                (
                SELECT
                    ang.`arrival_notice_id` arrival_notice_id
                    ,ang.`seller_goods_id` seller_goods_id
                    ,ang.`number` 通知数量
                    ,ang.`in_num` goods_number
                    ,ifnull(sg.`two_conversion`, 0) two_conversion
                    ,ifnull(sg.`three_conversion`, 0) three_conversion
                FROM `wms_production`.`arrival_notice_goods` ang
                LEFT JOIN `wms_production`.`seller_goods` sg ON ang.`seller_goods_id`=sg.`id`
                ) ang
            ) ang
        GROUP BY ang.`arrival_notice_id`
        ) ang on an.`id`=ang.`arrival_notice_id`
        WHERE wh.`name`='BKK-WH-LAS2电商仓'
    ) inbound
WHERE 收货完成时间 BETWEEN 
convert_tz(date_sub(date_sub(date_format(now(),'%y-%m-%d'),interval extract(day from now())-1 day),interval 1 month),'+07:00', '+07:00') 
and 
convert_tz(date_sub(date_sub(date_format(now(),'%y-%m-%d'),interval extract(day from now())-1 day),interval 0 month),'+07:00', '+07:00') #@TODO 需要改时间
),
outbound as
( -- 出库 与 打包
SELECT
        SUM(打包单量) 打包单量
        ,SUM(出库单量) 出库单量
FROM
        (
        SELECT
                仓库
                ,CASE name when 'TCL' THEN 单量*6
                          when 'Intrepid - Tefal' then 单量*2
                          else 单量
                  end 打包单量
                  ,CASE name when 'TCL' THEN 单量*6
                          when 'Intrepid - Tefal' then 单量*1.5
                          else 单量
                  end 出库单量
        FROM
                (
                SELECT
                        'LAS' 仓库
                        ,s.name
                        ,COUNT(DISTINCT do.delivery_sn) 单量
                FROM wms_production.delivery_order do
                left join wms_production.seller s on do.seller_id =s.id

               -- left join tmpale.delete_tmp_th_ffm_warehouse_info t on do.warehouse_id =t.warehouse_id
                WHERE -- t.warehouse_name='BKK-WH-LAS2电商仓'
                        do.warehouse_id = '39' -- 39代表'BKK-WH-LAS2电商仓'
                        and do.delivery_time BETWEEN 
                        convert_tz(date_sub(date_sub(date_format(now(),'%y-%m-%d'),interval extract(day from now())-1 day),interval 1 month),'+07:00', '+08:00')
                         and 
                         convert_tz(date_sub(date_sub(date_format(now(),'%y-%m-%d'),interval extract(day from now())-1 day),interval 0 month),'+07:00', '+08:00')
                          # @TODO 需要改时间
                GROUP BY 'LAS'
                        ,s.name
                ) t0
        ) t0
),
back as
(
SELECT
        SUM(销退单量) 销退单量
FROM
        (
        SELECT
                货主
                ,CASE 货主 when 'TCL' THEN 单量*6
                        when 'Intrepid - Tefal' then 单量*1.5
                        else 单量
                        end 销退单量
        FROM
                (
                SELECT
                        货主
                        ,COUNT(DISTINCT 订单号) 单量
                FROM
                        (
                        SELECT
                            -- 时区转换；状态
                            dro.`back_sn` 订单号
                            ,s3.name 货主
                            ,dro.`id` 订单ID
                            ,'销退入库单' 订单类型
                            ,case dro.`back_type`
                                when 'primary' then '普通退货'
                                when 'backgoods' then '退货换货'
                                when 'allRejected' then '全部拒收'
                                when 'package' then '包裹销退'
                                when 'crossBorder' then '跨境订单'
                                when 'interceptCrossBorder' then '拦截跨境销退'
                                end 业务类型
                            ,dro.`complete_time` 收货完成时间
                            ,mb.`job_number` 收货人ID
                            ,mb.`real_name` 收货人名称
                            ,wh.`name` 虚拟仓
                            ,case
                                when  wh.`name`='BKK-WH-LAS2电商仓' then 'LAS'
                            end 物理仓
                            ,dro.`status`  订单状态
                        --     ,drog.`SKU` SKU
                        --     ,drog.`通知数量` 通知数量
                        --     ,drog.`商品数量` 商品数量
                        --     ,drog.`修正商品数量` 修正商品数量
                        FROM `wms_production`.`delivery_rollback_order` dro
                        left join wms_production.seller s3 on dro.seller_id =s3.id
                        LEFT JOIN `wms_production`.`member` mb on dro.`complete_id`=mb.`id`
                        LEFT JOIN `wms_production`.`warehouse` wh on wh.`id`=dro.`warehouse_id`
                        WHERE wh.`name`='BKK-WH-LAS2电商仓'
                        ) t0
                WHERE 收货完成时间 BETWEEN convert_tz(date_sub(date_sub(date_format(now(),'%y-%m-%d'),interval extract(day from now())-1 day),interval 1 month),'+07:00', '+07:00') 
                and 
                convert_tz(date_sub(date_sub(date_format(now(),'%y-%m-%d'),interval extract(day from now())-1 day),interval 0 month),'+07:00', '+07:00') #@TODO 需要改时间
                GROUP BY  货主
                ) t0
        ) t0
),
  fine as (
        -- 每个月罚款
        SELECT
            月份,
            工号,
            sum(罚款总金额) 罚款总金额,
            sum(if(罚款类别一 = '业务', 罚款总金额, 0)) 业务罚款,
            sum(if(罚款类别一 = '现场管理', 罚款总金额, 0)) 现场管理罚款,
            sum(if(罚款类别一 = '考勤', 罚款总金额, 0)) 考勤罚款
        FROM
            (
                SELECT
                    fb.`fine_sn` 罚款编号,
                    case
                        fb.`status`
                        when 0 then '初始化'
                        when 1 then '创建'
                        when 2 then '审核'
                        when 3 then '作废'
                    end 状态,
                    mb.`job_number` 工号,
                    mb.`name` 姓名,
                    wh.`name` 所属仓库,
                    wb.`name` 罚款类别一,
                    wbd.`zh` 罚款类别二,
                    fb.`biz_order_sn` 关联单号,
                    fb.`biz_date` 业务发生日期,
                    fb.`amount` * 0.01 预罚款金额,
                    fb.`reason` 罚款原因,
                    case
                        fb.`adjust_type`
                        when 0 then '增加'
                        when 1 then '减少'
                    end 调整类型,
                    fb.`adjust_amount` 调整金额,
                    fb.`adjust_reason` 调整原因,
                    (fb.`amount` + fb.`adjust_amount`) * 0.01 罚款总金额,
                    fb.`creator_name` 创建人,
                    left(date_add(fb.`created`, interval -60 minute), 7) 月份,
                    date_add(fb.`created`, interval -60 minute) 创建时间,
                    fb.`auditor_id` 审核人,
                    fb.`audited` 审核时间,
                    fb.`remark` 备注
                FROM
                    `wms_production`.`fine_bill` fb
                    LEFT JOIN `wms_production`.`wordbook_detail` wbd on wbd.`id` = fb.`type`
                    LEFT JOIN `wms_production`.`wordbook` wb on wb.`id` = wbd.`wordbook_id`
                    LEFT JOIN `wms_production`.`warehouse` wh on wh.`id` = fb.`warehouse_id`
                    LEFT JOIN `wms_production`.`member` mb on mb.`id` = fb.`member_id`
                WHERE
                    fb.`status` != 3
                    -- 上个月的月份
                and left(date_add(fb.`created`, interval -60 minute), 7)=left(date_sub(date_add(now(),interval -60 minute),interval 1 month),7) # @tod0 修改时间

            ) fb
        GROUP BY
            月份,
            工号
    )
SELECT
        员工id
        ,职位类别
        ,工作仓库
        ,kpi系数
        ,是否本月入职
        ,工作量
        ,超额提成目标值
        ,人数
        ,提成系数
        ,超额提成
        ,0 小时工扣减 -- @todo
        ,SUM(超额提成/人数) over (PARTITION by 1)-0 总提成 -- @todo
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
        ,出勤/应出勤 考勤系数
        , ((SUM(超额提成/人数) over (PARTITION by 1))-0)/(sum(1) over(partition by 1)) 工作量提成 # 工作量提成=总提成/总人数 @todo
        ,((SUM(超额提成/人数) over (PARTITION by 1))-0)
        ,(sum(1) over(partition by 1))
        ,coalesce(fine.业务罚款, 0) 业务罚款
        ,coalesce(fine.现场管理罚款, 0) 现场管理罚款
        ,coalesce(fine.考勤罚款, 0) 考勤罚款
        -- @todo
        ,((SUM(超额提成/人数) over (PARTITION by 1)-0)/(sum(1) over(partition by 1))) * (出勤/应出勤) - (coalesce(fine.业务罚款,0) + coalesce(fine.现场管理罚款,0) + coalesce(fine.考勤罚款,0) ) 基础提成 # 基础提成=工作量提成*考勤系数-考勤罚款
        ,kpi系数 kpi系数
        ,if(出勤/应出勤=1 and 是否本月入职='非本月入职',800,0) 全勤奖
        -- @todo
        ,(((SUM(超额提成/人数) over (PARTITION by 1)-0)/(sum(1) over(partition by 1))) * (出勤/应出勤) - (coalesce(fine.业务罚款,0) + coalesce(fine.现场管理罚款,0) + coalesce(fine.考勤罚款,0) ) ) * kpi系数  提成 # 提成=基础提成*KPI 系数
        -- @todo
        ,if(cast(kpi系数 as decimal(38,2) )>0,((((SUM(超额提成/人数) over (PARTITION by 1)-0)/(sum(1) over(partition by 1))) * (出勤/应出勤) - (coalesce(fine.业务罚款,0) + coalesce(fine.现场管理罚款,0) + coalesce(fine.考勤罚款,0) ) ) * kpi系数) + if(出勤/应出勤=1 and 是否本月入职='非本月入职',800,0),0 ) 应发提成 # 提成=基础提成*KPI 系数
FROM
        (-- 超额提成
        SELECT
                员工id
                ,职位类别
                ,工作仓库
                ,kpi系数
                ,是否本月入职
                ,入库工作量
                ,打包工作量
                ,出库工作量
                ,销退工作量
                ,CASE when 职位类别='Outbound' then 出库工作量
                        when 职位类别='Inbound' then 入库工作量
                        when 职位类别='Pickpacking' then 打包工作量
                        when 职位类别='Back' then 销退工作量
                        else 0
                end 工作量
                ,提成系数
                ,超额提成目标值
                ,人数
                ,if((入库工作量-超额提成目标值)<0,0,(入库工作量-超额提成目标值)*提成系数) 入库超额提成
                ,if((打包工作量-超额提成目标值)<0,0,(打包工作量-超额提成目标值)*提成系数) 打包超额提成
                ,if((出库工作量-超额提成目标值)<0,0,(出库工作量-超额提成目标值)*提成系数) 出库超额提成
                ,if((销退工作量-超额提成目标值)<0,0,(销退工作量-超额提成目标值)*提成系数) 销退超额提成
                ,CASE when 职位类别='Outbound' then if((出库工作量-超额提成目标值)<0,0,(出库工作量-超额提成目标值)*提成系数)
                        when 职位类别='Inbound' then if((入库工作量-超额提成目标值)<0,0,(入库工作量-超额提成目标值)*提成系数)
                        when 职位类别='Pickpacking' then if((打包工作量-超额提成目标值)<0,0,(打包工作量-超额提成目标值)*提成系数)
                        when 职位类别='Back' then if((销退工作量-超额提成目标值)<0,0,(销退工作量-超额提成目标值)*提成系数)
                        else 0
                end 超额提成
        FROM
                (-- 工作量基本信息
                SELECT
                        员工id
                        ,t.职位类别
                        ,t.工作仓库
                        ,菜鸟入库*1.2 菜鸟入库量
                        ,t.菜鸟出库*1.2 菜鸟打包量
                        ,t.菜鸟出库*1.2 菜鸟出库量
                        ,菜鸟销退*1.2 菜鸟销退
                        ,kpi系数
                        ,IF(LEFT(hsi.hire_date,7)=left(date_sub(date_add(now(),interval -60 minute),interval 1 month),7),'本月入职','非本月入职') 是否本月入职 # @todo
                        ,入库数量
                        ,打包单量
                        ,出库单量
                        ,销退单量
                        ,物料仓入库量
                        ,物料仓打包量
                        ,物料仓出库量
                        ,CASE when t.工作仓库='电商仓' and t.职位类别='Inbound' then 菜鸟入库*1.2+入库数量
                                when t.工作仓库='物料仓' and t.职位类别='Inbound' then 物料仓入库量
                                else 0
                        END 入库工作量
                        ,CASE when t.工作仓库='电商仓' and t.职位类别='Pickpacking'   then (t.菜鸟出库*1.2+打包单量)
                                when t.工作仓库='物料仓' and t.职位类别='Pickpacking' then 物料仓打包量
                                else 0
                        END 打包工作量
                        ,CASE when t.工作仓库='电商仓' and t.职位类别='Outbound' then t.菜鸟出库*1.2+出库单量
                                when t.工作仓库='物料仓'  and t.职位类别='Outbound' then 物料仓出库量
                                else 0
                        END 出库工作量
                        ,CASE WHEN t.工作仓库='销退' THEN 销退单量+菜鸟销退*1.2
                                ELSE 0
                        END 销退工作量
                        ,c.人数
                        ,提成系数
                        ,目标值
                        ,CASE when t.工作仓库='电商仓' then 目标值*26*c.人数
                                when t.工作仓库='物料仓' then 目标值*c.人数
                                when t.工作仓库='销退' then 目标值*26*c.人数
                                else 0
                        end 超额提成目标值
                FROM tmpale.tmp_th_ffm_las_stat t
                left join bi_pro.hr_staff_info hsi on t.员工id=hsi.staff_info_id
                left join inbound on 1=1
                left join outbound on 1=1
                left join back on 1=1
                left join
                        (
                        select
                                月份
                                ,SUM(工作量) 物料仓入库量
                        FROM
                                (
                                SELECT
                              left(收货完成时间, 7) 月份
                              ,物理仓
                              ,收货人ID
                              ,case when 物理仓='BPL-Return' then sum(if(业务类型='包裹销退',修正商品数量,0))
                                    else sum(if(业务类型='采购入库',修正商品数量,0))
                                    end 工作量
                              FROM dwm.dwd_th_ffm_inbound_detail1
                              WHERE 物理仓='LAS'
                              and left(收货完成时间, 7)=left(date_sub(date_add(now(),interval -60 minute),interval 1 month),7) #@TODO 修改时间
                              GROUP BY left(收货完成时间, 7)
                              ,物理仓
                              ,收货人ID
                                ) t0
                        group by 月份
                        )a on 1=1
                left join
                        (
                        select
                                月份
                                ,SUM(工作量) 物料仓打包量
                                ,SUM(工作量) 物料仓出库量
                        FROM
                                (
                                SELECT
                              left(出库完成时间, 7) 月份
                              ,物理仓
                              ,复核人ID
                              ,count(distinct 箱单号) 工作量
                              FROM dwm.dwd_th_ffm_outbound_detail1
                              WHERE 订单状态!=1000
                              AND 业务类型 in ('发货','出库')
                              AND 物理仓='LAS'
                               and left(出库完成时间, 7)=left(date_sub(date_add(now(),interval -60 minute),interval 1 month),7) # @TODO 修改时间
                              GROUP BY left(出库完成时间, 7)
                              ,物理仓
                              ,复核人ID
                                ) t0
                        )b on 1=1
                LEFT JOIN
                        (
                        select
                                工作仓库
                                ,职位类别
                                ,COUNT(DISTINCT 员工ID ) 人数
                        FROM tmpale.tmp_th_ffm_las_stat t
                        left join bi_pro.hr_staff_info hsi on t.员工id=hsi.staff_info_id
                        GROUP BY 工作仓库
                                ,职位类别
                        )c on t.工作仓库=c.工作仓库 and t.职位类别=c.职位类别
                LEFT JOIN tmpale.tmp_th_las_rule d on t.工作仓库=d.工作仓库 and t.职位类别=d.职位类别
                where t.员工id is not null and length(t.员工id)>0
                ) t0
        )a
left JOIN
        (
        SELECT
                left(统计日期,7) 月份
                ,人员信息
                ,sum(应出勤) 应出勤
                ,sum(出勤) 出勤
                ,sum(迟到) 迟到
                ,sum(旷工) 旷工
                ,sum(年假) 年假
                ,sum(事假) 事假
                ,sum(病假) 病假
                ,sum(产假) 产假
                ,sum(丧假) 丧假
                ,sum(婚假) 婚假
                ,sum(公司培训假) 公司培训假
        FROM
                (
                SELECT
                        *
                FROM `dwm`.`dwd_th_ffm_attendance_detail1` d
                join tmpale.tmp_th_ffm_las_stat t on d.人员信息=t.员工id
                ) ad
                GROUP BY  left(统计日期,7)
                ,人员信息
        )b on a.员工id=b.人员信息
left join fine on a.员工id=fine.工号;
