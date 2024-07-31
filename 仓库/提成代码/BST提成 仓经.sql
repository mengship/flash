-- dwd_th_ffm_commission_two 跑完后跑这个
select
    月份,
    物理仓,
    工号,
    部门,
    分组,
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
    部门工作量,
    超额提成目标,
    超额提成工作量,
    超额系数,
    部门人数,
    人均工作量提成金额,
    阶梯提成工作量,
    打包拣货分组人数,
    阶梯提成金额,
    计提工作量,
    提成,
    提成系数,
    提成 提成1,
    全勤奖,
    业务罚款,
    现场管理罚款,
    考勤罚款,
    奖励,
    提成2,
    出勤率,
    旷工请假折算系数,
    case
        when 部门 = 'Supervisor' then sum(应发提成) over(partition by 月份) /(count(应发提成) over(partition by 月份) -1)
        else 应发提成
    end 应发提成
from
    (
        select
            月份,
            物理仓,
            工号,
            部门,
            分组,
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
            超额提成工作量0 部门工作量,
            超额提成目标,
            超额提成工作量,
            超额系数,
            部门人数,
            人均工作量提成金额,
            阶梯提成工作量,
            case
                when 部门 = 'Picking' then 1
                when 部门 = 'Packing' then 分组人数
            end 打包拣货分组人数,
            阶梯提成金额,
            计提工作量,
            基础提成 提成,
            提成系数,
            提成 提成1,
            全勤奖,
            业务罚款,
            现场管理罚款,
            考勤罚款,
            奖励,
            奖惩提成 提成2,
            出勤率,
            case
                when 旷工 > 0 then 0
                when 请假 > 3 then 0
                when 应出勤 <= 0 then 0
                when 请假 > 2
                and 请假 <= 3 then 0.6
                when 请假 > 1
                and 请假 <= 2 then 0.75
                when 请假 <= 1 then 1
            end 旷工请假折算系数,
            应发提成
        from
            (
                select
                    *,
                    case
                        when 旷工 > 0 then 0
                        when 请假 > 3 then 0
                        when 应出勤 <= 0 then 0
                        when 请假 > 2
                        and 请假 <= 3 then 奖惩提成 / 应出勤 * 出勤 * 0.6
                        when 请假 > 1
                        and 请假 <= 2 then 奖惩提成 / 应出勤 * 出勤 * 0.75
                        when 请假 <= 1 then 奖惩提成 / 应出勤 * 出勤
                    end 应发提成,
                    case
                        when 部门 in ('Inbound', 'Outbound', 'B2B') then if(超额系数 != 0, 基础提成 / 超额系数, 0)
                        when 部门 in ('Put away', 'Picking') then Picking
                        when 部门 in ('Packing') then Packing
                        else 0
                    end 计提工作量
                from
                    (
                        select
                            *,
                            if(
                                提成 - IFNULL(业务罚款, 0) - IFNULL(现场管理罚款, 0) - IFNULL(考勤罚款, 0) + 全勤奖 + 奖励 > 0,
                                提成 - IFNULL(业务罚款, 0) - IFNULL(现场管理罚款, 0) - IFNULL(考勤罚款, 0) + 全勤奖 + 奖励,
                                0
                            ) 奖惩提成
                        from
                            (
                                select
                                    *,
                                    case
                                        when 部门 in('Picking', 'Packing') then 基础提成
                                        else if(基础提成 * 提成系数 >= 5000, 5000, 基础提成 * 提成系数)
                                    end 提成,
                                    if(出勤 = 应出勤, 800, 0) 全勤奖,
                                    0 奖励
                                from
                                    (
                                        select
                                            *,
                                            if(人均工作量提成金额 + 阶梯提成金额 > 0, 人均工作量提成金额 + 阶梯提成金额, 0) 基础提成
                                        from
                                            (
                                                select
                                                    *,
                                                    超额提成工作量 * 超额系数 / 部门人数 人均工作量提成金额,
                                                    case
                                                        when 部门 = 'Picking'
                                                        and 阶梯提成工作量 -40000 > 0 then 7000
                                                        when 部门 = 'Picking'
                                                        and 阶梯提成工作量 -35000 > 0 then 5800
                                                        when 部门 = 'Picking'
                                                        and 阶梯提成工作量 -32000 > 0 then 5000
                                                        when 部门 = 'Picking'
                                                        and 阶梯提成工作量 -28000 > 0 then 4300
                                                        when 部门 = 'Picking'
                                                        and 阶梯提成工作量 -25000 > 0 then 3600
                                                        when 部门 = 'Picking'
                                                        and 阶梯提成工作量 -22000 > 0 then 3000
                                                        when 部门 = 'Picking'
                                                        and 阶梯提成工作量 -19000 > 0 then 2400
                                                        when 部门 = 'Picking'
                                                        and 阶梯提成工作量 -16000 > 0 then 1800
                                                        when 部门 = 'Picking'
                                                        and 阶梯提成工作量 -13000 > 0 then 1200
                                                        when 部门 = 'Picking'
                                                        and 阶梯提成工作量 -10000 > 0 then 500
                                                        when 部门 = 'Packing'
                                                        and 阶梯提成工作量 -38000 > 0 then 13200 / 分组人数
                                                        when 部门 = 'Packing'
                                                        and 阶梯提成工作量 -33000 > 0 then 11100 / 分组人数
                                                        when 部门 = 'Packing'
                                                        and 阶梯提成工作量 -31000 > 0 then 9600 / 分组人数
                                                        when 部门 = 'Packing'
                                                        and 阶梯提成工作量 -29000 > 0 then 8400 / 分组人数
                                                        when 部门 = 'Packing'
                                                        and 阶梯提成工作量 -27000 > 0 then 7200 / 分组人数
                                                        when 部门 = 'Packing'
                                                        and 阶梯提成工作量 -25000 > 0 then 6120 / 分组人数
                                                        when 部门 = 'Packing'
                                                        and 阶梯提成工作量 -23000 > 0 then 5100 / 分组人数
                                                        when 部门 = 'Packing'
                                                        and 阶梯提成工作量 -21000 > 0 then 4200 / 分组人数
                                                        when 部门 = 'Packing'
                                                        and 阶梯提成工作量 -19000 > 0 then 3210 / 分组人数
                                                        when 部门 = 'Packing'
                                                        and 阶梯提成工作量 -17000 > 0 then 2310 / 分组人数
                                                        when 部门 = 'Packing'
                                                        and 阶梯提成工作量 -15000 > 0 then 1410 / 分组人数
                                                        when 部门 = 'Packing'
                                                        and 阶梯提成工作量 -13000 > 0 then 600 / 分组人数
                                                        else 0
                                                    end 阶梯提成金额
                                                from
                                                    (
                                                        select
                                                            *,
                                                            超额提成工作量0 - 超额提成目标 超额提成工作量
                                                        from
                                                            (
                                                                select
                                                                    sr.*,
                                                                    出勤 / 应出勤 出勤率,
                                                                    年假 + 事假 + 病假 + 产假 + 丧假 + 婚假 请假,
                                                                    部门人数,
                                                                    分组人数,
                                                                    case
                                                                        when sr.部门 = 'Inbound' then (目标值 / 全勤天数) * 部门总出勤
                                                                        when sr.部门 = 'Outbound' then (目标值 / 全勤天数) * 部门总出勤
                                                                        when sr.部门 = 'B2B' then (目标值 / 全勤天数) * 部门总出勤
                                                                        else 0
                                                                    end 超额提成目标,
                                                                    case
                                                                        when sr.部门 = 'Inbound' then 仓库入库
                                                                        when sr.部门 = 'Outbound' then 仓库出库
                                                                        when sr.部门 = 'B2B' then 部门B2B
                                                                        else 0
                                                                    end 超额提成工作量0,
                                                                    case
                                                                        when sr.部门 = 'Picking' then Picking
                                                                        when sr.部门 = 'Packing' then 分组打包
                                                                        else 0
                                                                    end 阶梯提成工作量
                                                                FROM
                                                                    dwm.dwd_th_ffm_commission_two sr
                                                                    LEFT JOIN (
                                                                        SELECT
                                                                            月份,
                                                                            物理仓,
                                                                            count(distinct if(部门 != 'Supervisor', 工号, null)) 仓库人数,
                                                                            max(应出勤) 全勤天数,
                                                                            sum(应出勤) 仓库总出勤
                                                                        FROM
                                                                            dwm.dwd_th_ffm_commission_two
                                                                        GROUP BY
                                                                            月份,
                                                                            物理仓
                                                                    ) sw ON sr.月份 = sw.月份
                                                                    AND sr.物理仓 = sw.物理仓
                                                                    LEFT JOIN (
                                                                        SELECT
                                                                            left(收货完成时间, 7) 月份,
                                                                            物理仓,
                                                                            sum(修正商品数量) 仓库入库
                                                                        FROM
                                                                            dwm.dwd_th_ffm_inbound_detail1
                                                                        WHERE
                                                                            物理仓 = 'BST'
                                                                            AND 业务类型 = '采购入库'
                                                                        GROUP BY
                                                                            1,
                                                                            2
                                                                    ) inbound ON sr.月份 = inbound.月份
                                                                    AND sr.物理仓 = inbound.物理仓
                                                                    LEFT JOIN (
                                                                        SELECT
                                                                            left(出库完成时间, 7) 月份,
                                                                            物理仓,
                                                                            count(distinct 箱单号) 仓库出库
                                                                        FROM
                                                                            dwm.dwd_th_ffm_outbound_detail1
                                                                        WHERE
                                                                            订单状态 != 1000
                                                                            AND 业务类型 in ('发货', '出库')
                                                                            AND 物理仓 = 'BST'
                                                                            AND 虚拟仓 != 'Fans-B2B'
                                                                        GROUP BY
                                                                            1,
                                                                            2
                                                                    ) outbound ON sr.月份 = outbound.月份
                                                                    AND sr.物理仓 = outbound.物理仓
                                                                    left join (
                                                                        SELECT
                                                                            月份,
                                                                            物理仓,
                                                                            分组,
                                                                            count(0) 分组人数,
                                                                            sum(Packing) 分组打包
                                                                        FROM
                                                                            dwm.dwd_th_ffm_commission_two
                                                                        GROUP BY
                                                                            月份,
                                                                            物理仓,
                                                                            分组
                                                                    ) swg ON sr.月份 = swg.月份
                                                                    AND sr.物理仓 = swg.物理仓
                                                                    AND sr.分组 = swg.分组
                                                                    LEFT JOIN (
                                                                        SELECT
                                                                            月份,
                                                                            物理仓,
                                                                            部门,
                                                                            count(distinct 工号) 部门人数,
                                                                            sum(应出勤) 部门总出勤,
                                                                            sum(Inbound) 部门入库,
                                                                            sum(Picking) 部门拣货,
                                                                            sum(Packing) 部门打包,
                                                                            sum(Outbound) 部门出库,
                                                                            sum(B2B) 部门B2B,
                                                                            sum(PickingPacking) 部门拣货打包
                                                                        FROM
                                                                            dwm.dwd_th_ffm_commission_two
                                                                        GROUP BY
                                                                            月份,
                                                                            物理仓,
                                                                            部门
                                                                    ) swd ON sr.月份 = swd.月份
                                                                    AND sr.物理仓 = swd.物理仓
                                                                    AND sr.部门 = swd.部门
                                                                where
                                                                    sr.物理仓 = 'BST'
                                                            )
                                                    )
                                            )
                                    )
                            )
                    )
            )
        order by
            人均工作量提成金额 desc,
            部门,
            阶梯提成金额 desc,
            分组
    )