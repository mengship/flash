-- BST出库逻辑 tiktok Shopee 12点截单时间
-- 通过销售平台逻辑来区分
-- 汇总逻辑
select
date(订单审核时间)    日期
,仓库
,sum(及时打包V2) 及时发货量
,count(发货单号) 发货单量
,round(sum(及时打包V2) / count(发货单号), 4) 发货及时率
from
dwm.dwd_th_ffm_outboundtimely_day_V4
where 1=1
and 仓库='BST'
and date(订单审核时间)>=date_sub(date(now() + interval -1 hour),interval 30 day)
and date(订单审核时间)< date(now() + interval -1 hour)
group by date(订单审核时间)
        ,仓库
order by date(订单审核时间) desc;

-- 明细逻辑
/*=====================================================================+
 表名称：  dwd_th_ffm_outboundtimely_day_V4
 功能描述：泰国的出库的时效明细表
 
 需求来源：
 编写人员: 
 设计日期：
 修改日期: 2024/5/9
 修改人员: 王昱棋           
 修改原因:  
 -----------------------------------------------------------------------
 ---存在问题：
 -----------------------------------------------------------------------
 +=====================================================================*/

flag=4
shellname=dwd_th_ffm_outboundtimely_day_V4
type=th
erremail=tiance@flashexpress.com
sh /home/deploy/script/shell/base.sh ${flag} ${shellname} ${type} ${erremail}

drop table if exists dwm.dwd_th_ffm_outboundtimely_day_V4;
create table dwm.dwd_th_ffm_outboundtimely_day_V4  as
select 仓库名称
     , 仓库
     , saleplatformname
     , 货主名称
     , 发货单号
     , 类型
     , 订单状态
     , 快递公司
     , 订单创建时间
     , 订单审核时间
     , 生成拣货单时间
     , 拣货完成时间
     , 打包完成时间
     , 绑定交接单时间
     , 快递已签收时间
     , sla_start_date
     , 流程最晚时间
     , date(流程最晚时间) 流程最晚日期
     , 数据状态
     , 是否及时
     , 复核打包人ID
     , 是否为异常单
     , `获取电子面单号失败`
     , `系统报缺`
     , `待补货`
     , `待上架`
     , `系统报错`
     , 昨日留存且未完成订单
     , 昨日留存且今日完成订单
     , 今日新增完且今日完成订单
     , 今日新增完且未完成订单
     , 待生成拣货单
     , 待拣货
     , 待打包
     , 待绑定交接单
     , 待签收
     , if(仓库名称 != 'BPL-Return Warehouse'
    , case
--                      when 待生成波次=1 then '待生成波次'
          when (类型 = 'wms发货单') and 待生成拣货单 = 1 then '待生成拣货单'
--                      when 待打印拣货单=1 then '待打印拣货单'
          when (类型 = 'wms发货单') and 待拣货 = 1 then '待拣货'
          when (类型 = 'wms发货单') and 待打包 = 1 then '待打包'
          when (类型 = 'wms发货单') and 待绑定交接单 = 1 then '待绑定交接单'
          when (类型 = 'wms发货单') and 待签收 = 1 then '待签收'
          when (类型 = 'erp发货单') and 待生成拣货单 = 1 then '待生成拣货单'
--                      when 待打印拣货单=1 then '待打印拣货单'
          when (类型 = 'erp发货单') and 待拣货 = 1 then '待拣货'
          when (类型 = 'erp发货单') and 待打包 = 1 then '待打包'
          when (类型 = 'erp发货单') and 待绑定交接单 = 1 then '待绑定交接单'
          when (类型 = 'erp发货单') and 待签收 = 1 then '待签收'
          when (类型 = 'wms出库单') and 待生成拣货单 = 1 then '待生成拣货单'
--                      when 待打印拣货单=1 then '待打印拣货单'
          when (类型 = 'wms出库单') and 待拣货 = 1 then '待拣货'
          when (类型 = 'wms出库单') and 待打包 = 1 then '待打包'
--                      when ( 类型='wms出库单') and 待绑定交接单=1 then '待绑定交接单'
          when (类型 = 'wms出库单') and 待签收 = 1 then '待签收'
          when (类型 = 'erp出库单') and 待生成拣货单 = 1 then '待生成拣货单'
--                      when 待打印拣货单=1 then '待打印拣货单'
          when (类型 = 'erp出库单') and 待拣货 = 1 then '待拣货'
          when (类型 = 'erp出库单') and 待打包 = 1 then '待打包'
--                      when ( 类型='erp出库单') and 待绑定交接单=1 then '待绑定交接单'
          when (类型 = 'erp出库单') and 待签收 = 1 then '待签收'
          else '已完成'
              end
    , case
              --                      when 待生成波次=1 then '待生成波次'
--                      when 待生成拣货单=1 then '待生成拣货单'
--                      when 待打印拣货单=1 then '待打印拣货单'
--                      when 待拣货=1 then '待拣货'
          when (类型 = 'wms发货单') and 待打包 = 1 then '待打包'
          when (类型 = 'wms发货单') and 待绑定交接单 = 1 then '待绑定交接单'
          when (类型 = 'wms发货单') and 待签收 = 1 then '待签收'
          when (类型 = 'erp发货单') and 待打包 = 1 then '待打包'
          when (类型 = 'erp发货单') and 待绑定交接单 = 1 then '待绑定交接单'
          when (类型 = 'erp发货单') and 待签收 = 1 then '待签收'
          when (类型 = 'wms出库单') and 待打包 = 1 then '待打包'
--                      when (类型='wms出库单') and 待绑定交接单=1 then '待绑定交接单'
          when (类型 = 'wms出库单') and 待签收 = 1 then '待签收'
--                      when (类型='erp出库单') and 待打包=1 then '待打包'
          when (类型 = 'erp出库单') and 待拣货 = 1 then '待拣货'
          when (类型 = 'erp出库单') and 待签收 = 1 then '待签收'
          else '已完成'
              end) 仓内状态
        ,pick_id -- 拣货人
        ,pack_id -- 打包人
        ,handover_id -- 交接人
        ,sign_id -- 签收人
        ,品种数量
        ,商品数量
        ,修正商品数量
        ,商品体积
        ,商品重量
        , if(数据状态 = '完结数据', 1, 0)                                                           完结订单数
        , if((类型 = 'wms发货单' or 类型 = 'erp发货单') and 数据状态 = '完结数据', 1, 0)                      完结发货订单数
        , if((类型 = 'wms出库单' or 类型 = 'erp出库单') and 数据状态 = '完结数据', 1, 0)                      完结出库订单数
        , if(打包完成时间 is not null and 数据状态 = '完结数据', 1, 0)                                    完结打包订单数
        , if(是否及时='及时' and 数据状态 = '完结数据', 1, 0)                                      完结及时打包订单数
        , if(是否及时='及时' and (类型 = 'wms发货单' or 类型 = 'erp发货单') and 数据状态 = '完结数据', 1, 0) 完结发货及时打包订单数
        , if(是否及时='及时' and (类型 = 'wms出库单' or 类型 = 'erp出库单') and 数据状态 = '完结数据', 1, 0) 完结出库及时打包订单数
        , if(是否及时='及时', 1, 0) 及时打包V2
        , 已拣货
        , 已打包
        , 已绑定交接单
        , 已签收
        , 及时拣货
        , 及时打包
        , 及时交接
        , 及时发货
from (
         select 仓库名称
              , 仓库
              , saleplatformname
              , 货主名称
              , 发货单号
              , 类型
              , 订单状态
              , 快递公司
              , 订单创建时间
              , 订单审核时间
              , 生成拣货单时间
              , 拣货完成时间
              , 打包完成时间
              , 绑定交接单时间
              , 快递已签收时间
              , sla_start_date
              , 流程最晚时间
              /* , if(date_add(now(), interval -60 minute) >= 打包最晚时间, '完结数据', '过程数据') 数据状态
              , if(nvl(打包完成时间, '2099-12-31') < 打包最晚时间, '及时', '不及时')                是否及时 */
              , case when date_add(now(), interval -60 minute) >= 流程最晚时间 then '完结数据'
              else '过程数据' end as 数据状态
              , case when cutoff is not null and nvl(打包完成时间, '2099-12-31') < 流程最晚时间 then '及时'
                    when cutoff is null and nvl(打包完成时间, '2099-12-31') < 流程最晚时间 then '及时'
                    else '不及时' end as 是否及时
              , 复核打包人ID
              , 是否为异常单
              , `获取电子面单号失败`
              , `系统报缺`
              , `待补货`
              , `待上架`
              , `系统报错`
              , 昨日留存且未完成订单
              , 昨日留存且今日完成订单
              , 今日新增完且今日完成订单
              , 今日新增完且未完成订单
              , 待生成拣货单
              , 待拣货
              , 待打包
              , 待绑定交接单
              , 待签收
              , pick_id -- 拣货人
              , pack_id -- 打包人
              , handover_id -- 交接人
              , sign_id -- 签收人
              , 品种数量
              , 商品数量
              , 修正商品数量
              , 商品体积
              , 商品重量
              , 已拣货
              , 已打包
              , 已绑定交接单
              , 已签收
              , 及时拣货
              , 及时打包
              , 及时交接
              , 及时发货
         from (
                  SELECT 仓库
                       , an.`仓库名称` 仓库名称
                       , an.saleplatformname
                       , an.`货主名称` 货主名称
                       , 发货单号
                       , 类型
                       , 订单状态
                       , 快递公司
                       , 订单创建时间
                       , 订单审核时间
                       , 生成拣货单时间
                       , 拣货完成时间
                       , 打包完成时间
                       , 绑定交接单时间
                       , 快递已签收时间
                       , sla_start_date
                       , cutoff
                       , case
                             when cutoff is null and off_date is null
                                 then concat(统计日期后一天, ' ', substr(订单审核时间, 12, 8))
                             when cutoff is null and off_date is not null then concat(统计日期, ' ', '23:59:59')
                             when cutoff is not null and off_date is null and
                                  订单审核时间 >= concat(sla_start_date, ' ', cutoff)
                                 then 截单后最晚时间
                             when cutoff is not null and off_date is null and
                                  订单审核时间 < concat(sla_start_date, ' ', cutoff)
                                 then 截单前最晚时间
                             when cutoff is not null and off_date is not null then 截单前最晚时间
                      		 end          流程最晚时间
                       , 复核打包人ID
                       , 是否为异常单
                       , `获取电子面单号失败`
                       , `系统报缺`
                       , `待补货`
                       , `待上架`
                       , `系统报错`
                       , 昨日留存且未完成订单
                       , 昨日留存且今日完成订单
                       , 今日新增完且今日完成订单
                       , 今日新增完且未完成订单
                       , 待生成拣货单
                       , 待拣货
                       , 待打包
                       , 待绑定交接单
                       , 待签收
                       , pick_id -- 拣货人
                       , pack_id -- 打包人
                       , handover_id -- 交接人
                       , sign_id -- 签收人
                       , 品种数量
                       , 商品数量
                       , 修正商品数量
                       , 商品体积
                       , 商品重量
                       , 已拣货
                       , 已打包
                       , 已绑定交接单
                       , 已签收
                       , 及时拣货
                       , 及时打包
                       , 及时交接
                       , 及时发货
                  FROM (
                           SELECT an.仓库名称
                                , an.类型
                                , an.仓库
                                , an.货主名称
                                , an.发货单号
                                , an.快递公司
                                , an.订单状态
                                , an.订单创建时间
                                , an.订单审核时间
                                , an.生成拣货单时间
                                , an.拣货完成时间
                                , an.打包完成时间
                                , an.绑定交接单时间
                                , an.快递已签收时间
                                , an.系统提示
                                , an.saleplatformname
                                , t_default.sla_start_date
                                , special.operation
                                , special.cutoff
                                , if(special.cutoff is not null , special.统计日期, t_default.统计日期) 统计日期
                                , if(special.cutoff is not null , special.统计日期后一天, t_default.统计日期后一天) 统计日期后一天
                                , if(special.cutoff is not null , special.统计日期后二天, t_default.统计日期后二天) 统计日期后二天
                                , if(special.cutoff is not null , special.截单前最晚时间, null) 截单前最晚时间
                                , if(special.cutoff is not null , special.截单后最晚时间, null) 截单后最晚时间

                                , sh.`off_date`                                                    off_date
                                , an.复核人ID 复核打包人ID
                                , an.是否为异常单
                                , if(an.是否为异常单 = 'error' and an.订单状态 = '获取电子面单号失败', 1, 0)                `获取电子面单号失败`
                                , if(an.是否为异常单 = 'error' and an.订单状态 = '分配库存失败' AND an.系统提示 = '系统报缺', 1, 0) `系统报缺`
                                , if(an.是否为异常单 = 'error' and an.订单状态 = '分配库存失败' AND an.系统提示 = '待补货', 1, 0)  `待补货`
                                , if(an.是否为异常单 = 'error' and an.订单状态 = '分配库存失败' AND an.系统提示 = '待上架', 1, 0)  `待上架`
                                , if(an.是否为异常单 = 'error' and an.订单状态 = '分配库存失败' AND an.系统提示 = '报错', 1, 0)   `系统报错`
                                , if(((date(an.订单审核时间) < today AND an.打包完成时间 is null and an.类型 <> 'erp出库单') or
                                      (date(an.订单审核时间) < today AND an.拣货完成时间 is null and an.类型 = 'erp出库单')) and
                                     an.是否为异常单 = 'normal', 1,
                                     0)                                                            昨日留存且未完成订单
                                , if(((date(an.订单审核时间) < today AND date(an.打包完成时间) = today and an.类型 <> 'erp出库单') or
                                      (date(an.订单审核时间) < today AND date(an.拣货完成时间) = today and an.类型 = 'erp出库单')) and
                                     an.是否为异常单 = 'normal', 1,
                                     0)                                                            昨日留存且今日完成订单
                                , if(((date(an.订单审核时间) = today AND date(an.打包完成时间) = today and an.类型 <> 'erp出库单') or
                                      (date(an.订单审核时间) = today AND date(an.拣货完成时间) = today and an.类型 = 'erp出库单')) and
                                     an.是否为异常单 = 'normal', 1,
                                     0)                                                            今日新增完且今日完成订单
                                , if(((date(an.订单审核时间) = today AND an.打包完成时间 is null and an.类型 <> 'erp出库单') or
                                      (date(an.订单审核时间) = today AND an.拣货完成时间 is null and an.类型 = 'erp出库单')) and
                                     an.是否为异常单 = 'normal', 1,
                                     0)                                                            今日新增完且未完成订单
                                , if(an.生成拣货单时间 is null, 1, 0)                                        待生成拣货单
                                , if(an.拣货完成时间 is null, 1, 0)                                         待拣货
                                , if(an.打包完成时间 is null, 1, 0)                                         待打包
                                , if(an.绑定交接单时间 is null, 1, 0)                                        待绑定交接单
                                , if(an.快递已签收时间 is null, 1, 0)                                        待签收

                                , if(an.拣货完成时间 is not null, 1, 0)                                         已拣货
                                , if(an.打包完成时间 is not null, 1, 0)                                         已打包
                                , if(an.绑定交接单时间 is not null, 1, 0)                                        已绑定交接单
                                , if(an.快递已签收时间 is not null, 1, 0)                                        已签收
                                , if(TIMESTAMPDIFF(MINUTE, an.订单审核时间,   an.拣货完成时间) < 60*2 , 1, 0) 及时拣货
                                , if(TIMESTAMPDIFF(MINUTE, an.拣货完成时间,   an.打包完成时间) < 60*4 , 1, 0) 及时打包
                                , if(TIMESTAMPDIFF(MINUTE, an.打包完成时间,   an.绑定交接单时间) < 60*2 , 1, 0) 及时交接
                                , if(TIMESTAMPDIFF(MINUTE, an.绑定交接单时间, an.快递已签收时间) < 60*12 , 1, 0) 及时发货
                                ,an.pick_id -- 拣货人
                                ,an.pack_id -- 打包人
                                ,an.handover_id -- 交接人
                                ,an.sign_id -- 签收人
                                ,an.品种数量
                                ,an.商品数量
                                ,an.修正商品数量
                                ,an.商品体积
                                ,an.商品重量
                           FROM dwm.dwd_th_ffm_outbound_day an
                                    LEFT JOIN
                                ( -- 特殊时效表 有截单时间区分
                                    select
                                    warehousename
                                    , saleplatformname
                                    , warehouseid
                                    , sellerid
                                    , sellername
                                    , sla_type
                                    , sla_start_date
                                    , operation
                                    , action
                                    , cutoff
                                    , befcutofflast
                                    , befcutofflastdiff
                                    , aftcutofflast
                                    , aftcutofflastdff
                                    , 统计日期
                                    , 统计日期后一天
                                    , 统计日期后二天
                                    , 截单前
                                    , 截单后
                                    , 截单前最晚时间
                                    , 截单后最晚时间
                                    from
                                    dwm.dim_th_special_time_v4
                                    # where 1=2
                                ) special
                                ON an.`仓库` = special.`warehousename` 
                                # 改为货主关联
                                # and an.货主名称 = special.sellername
                                and an.salePlatformName = special.saleplatformname
                                AND date(an.`订单审核时间`) = special.`sla_start_date`
                                 LEFT JOIN
                                ( -- 普通时效表 没有截单时间区
                                    select
                                    sla_start_date
                                    ,统计日期
                                    ,统计日期后一天
                                    ,统计日期后二天
                                    from
                                    dwm.dim_th_default_time
                                    -- order by sla_start_date  desc
                                ) t_default
                                ON 1=1
                                AND date(an.`订单审核时间`) = t_default.`sla_start_date`
                                -- 配置表信息
                                left join tmpale.tmp_th_timeload  tt
                                on an.`仓库` = tt.`warehousename` 
                                and an.salePlatformName = special.saleplatformname
                                    LEFT JOIN
                                ( -- 假期信息
                                    SELECT sh.`off_date`
                                    FROM fle_staging.`sys_holiday` sh
                                    WHERE sh.`off_date` >= date_add(curdate(), interval -365 day)
                                      AND sh.`off_date` < date_add(curdate(), interval 21 day)
                                      AND sh.`deleted` = 0
                                    GROUP BY sh.`off_date`
                                    ORDER BY sh.`off_date`
                                ) sh ON date(an.`订单审核时间`) = sh.`off_date`
                       		 	where date(an.`订单审核时间`)>='2023-12-01'
                            ) an
                  WHERE
                      if(类型 = 'wms发货单', 订单状态 not in ('取消发货'), 1 = 1)
                    AND if(类型 = 'erp发货单', 订单状态 not in ('订单关闭'), 1 = 1)
              ) an
     ) t0


-- dim_th_special_time_v4的逻辑
/*=====================================================================+
 表名称：  dim_th_special_time_v4
 功能描述：泰国时效维表V4版本
 
 需求来源：
 编写人员: 
 设计日期：
 修改日期: 2024/5/9
 修改人员: 王昱棋           
 修改原因:  
 -----------------------------------------------------------------------
 ---存在问题：
 -----------------------------------------------------------------------
 +=====================================================================*/


drop table if exists dwm.dim_th_special_time_v4;
create table dwm.dim_th_special_time_v4 as
SELECT
    warehousename
	,saleplatformname
    ,warehouseid
    ,sellerid
    ,sellername
    ,'截单口径' sla_type
    ,sla_start_date
    ,operation
    ,action
    ,cutoff
    ,befcutofflast
    ,befcutofflastdiff
    ,aftcutofflast
    ,aftcutofflastdff
    ,max(统计日期) 统计日期
    ,max(统计日期后一天) 统计日期后一天
    ,max(统计日期后二天) 统计日期后二天
    ,max(截单前) 截单前
    ,max(截单后) 截单后
    ,cast(CONCAT(max(截单前), ' ', befcutofflast) as datetime) 截单前最晚时间
    ,cast(CONCAT(max(截单后), ' ', aftcutofflast) as datetime) 截单后最晚时间
FROM
    (
    SELECT
        warehousename
        ,saleplatformname
        ,warehouseid
        ,sellerid
        ,sellername
        ,sla_start_date
        ,operation
        ,action
        ,cutoff
        ,befcutofflast
        ,befcutofflastdiff
        ,aftcutofflast
        ,aftcutofflastdff
        ,日期
        ,顺序
        ,if(顺序 = 1, 日期, null) 统计日期
        ,if(顺序 = 1 + 1, 日期, null) 统计日期后一天
        ,if(顺序 = 1 + 2, 日期, null) 统计日期后二天
        ,if(befcutofflastdiff + 1 = 顺序, 日期, null) 截单前
        ,if(aftcutofflastdff + 1 = 顺序, 日期, null) 截单后
    FROM
        (
        SELECT
            warehousename
            ,saleplatformname
            ,warehouseid
            ,sellerid
            ,sellername
            ,sla_start_date
            ,operation
            ,action
            ,cutoff
            ,befcutofflast
            ,befcutofflastdiff
            ,aftcutofflast
            ,aftcutofflastdff
            ,日期
            ,off_day
            # ,row_number() over (partition by warehousename,saleplatformname,sla_start_date,operation,action order by 日期) 顺序
            ,row_number() over (partition by warehousename,saleplatformname,sla_start_date order by 日期) 顺序
        FROM
            (
            SELECT
                warehousename
                ,saleplatformname
                ,warehouseid
                ,sellerid
                ,sellername
                ,sla_start_date
                ,operation
                ,action
                ,cutoff
                ,befcutofflast
                ,befcutofflastdiff
                ,aftcutofflast
                ,aftcutofflastdff
                ,dd.`date` 日期
                ,sh.`off_date`
                ,if(sh.`off_date` is not null, 'off_day', 'work_day') off_day
            FROM
                (
                SELECT
                    warehousename
                    ,saleplatformname
                    ,warehouseid
                    ,sellerid
                    ,sellername
                    ,operation
                    ,action
                    ,sla_start_date
                    ,cutoff
                    ,befcutofflast
                    ,befcutofflastdiff
                    ,aftcutofflast
                    ,aftcutofflastdff
                FROM
                    (
                    SELECT
                        dd.`date` sla_start_date
                        ,1 flag
                    FROM tmpale.`ods_th_dim_date` dd
                    WHERE dd.`date` >= '2023-12-01'
                        AND dd.`date` <= '2024-12-31'
                    ) dd
                LEFT JOIN
                    (
                    SELECT
                        warehousename
                        ,saleplatformname
                        ,warehouseid
                        ,sellerid
                        ,sellername
                        ,operation
                        ,action
                        ,cutoff
                        ,befcutofflast
                        ,befcutofflastdiff
                        ,aftcutofflast
                        ,aftcutofflastdff
                        ,1 flag
                    FROM tmpale.tmp_th_timeload
                    WHERE country='TH'
                    and warehousename='BST'
                    and saleplatformname is not null
                    and length(saleplatformname)>0
                    ) out_sla ON dd.`flag` = out_sla.`flag`
                ) out_sla
            -- 先关联最近14天日期
            LEFT JOIN tmpale.`ods_th_dim_date` dd
                ON dd.`date` >= out_sla.`sla_start_date` AND
                    dd.`date` <= date_add(out_sla.`sla_start_date`, interval 14 day)
            LEFT JOIN
                (
                SELECT sh.`off_date`
                FROM fle_staging.`sys_holiday` sh
                WHERE sh.`off_date` >= '2023-12-01'
                    AND sh.`off_date` <= '2024-12-31'
                    AND sh.`deleted` = 0
                GROUP BY sh.`off_date`
                ORDER BY sh.`off_date`
                ) sh ON dd.`date` = sh.`off_date`
            -- 去掉节假日
            WHERE sh.`off_date` is null
            ) out_sla
        ) out_sla
    )  out1
GROUP BY    
	warehousename
	,saleplatformname
    ,warehouseid
    ,sellerid
    ,sellername
    ,sla_start_date
    ,operation
    ,action
    ,cutoff
    ,befcutofflast
    ,befcutofflastdiff
    ,aftcutofflast
    ,aftcutofflastdff
order by 	warehousename
			,saleplatformname
			,sla_start_date