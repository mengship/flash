select
    物理仓,
    工号,
    部门,
    分组,
    出勤,
    Inbound,
    Picking,
    Packing,
    Outbound,
    B2B,
    计提工作量,
    基础提成,
    提成,
    业务罚款,
    现场管理罚款,
    考勤罚款,
    全勤奖,
    奖励,
    应发提成,
    迟到,
    旷工,
    年假,
    事假,
    病假,
    产假,
    丧假,
    婚假,
    公司培训假
from
    dwm.dwd_th_ffm_commission_five
where
    物理仓 = 'AGV'
order by
    物理仓,
    部门,
    分组;