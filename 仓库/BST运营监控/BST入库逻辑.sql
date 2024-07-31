-- BST入库逻辑

select
dt.date
,arr.及时收货量 入库单及时收货量
,arr.及时上架量 入库单及时上架量
,arr.入库单量   入库单入库单量
,arr.24H收货及时率  入库单24H收货及时率
,arr.48上架及时率   入库单48上架及时率
,rollback.及时收货量    销退单及时收货量
,rollback.及时上架量    销退单及时上架量
,rollback.销退单量  销退单销退单量
,rollback.24H销退收货及时率 销退单24H销退收货及时率
,rollback.48销退上架及时率  销退单48销退上架及时率
from
(SELECT date FROM tmpale.ods_th_dim_date where date>= date_sub(date(now() + interval -1 hour),interval 30 day) and date<date(now() + interval -1 hour)) dt
  left join
(
    -- 入库单
    select
    date(已到货时间)    日期
    ,仓库
    ,sum(及时收货订单数) 及时收货量
    ,sum(及时上架订单数) 及时上架量
    ,count(入库单号) 入库单量
    ,round(sum(及时收货订单数) / count(入库单号), 4) 24H收货及时率
    ,round(sum(及时上架订单数) / count(入库单号), 4) 48上架及时率
    from
    dwm.dwd_th_ffm_arrivalnoticetimely_day
    where 1=1
    and 仓库='BST'
    group by date(已到货时间)
            ,仓库
) arr on dt.date = arr.日期
left join
(
    -- 销退单入库
    select
    date(销退登记时间) 日期
    ,仓库
    ,sum(及时收货订单数) 及时收货量
    ,sum(及时上架订单数) 及时上架量
    ,count(销退入库单号) 销退单量
    ,round(sum(及时收货订单数) / count(销退入库单号) , 4) 24H销退收货及时率
    ,round(sum(及时上架订单数) / count(销退入库单号) , 4) 48销退上架及时率
    from
    dwm.dwd_th_ffm_rollbacktimely_day
    where 1=1
    and 仓库='BST'
    group by date(销退登记时间),仓库
) rollback
on dt.date = rollback.日期
order by dt.date desc;


-- 销退明细时效逻辑
         /*=====================================================================+
        表名称：  dwd_th_ffm_rollbacktimely_day
        功能描述：泰国销退时效明细表
                               
        需求来源：
        编写人员: 王昱棋
        设计日期：2024/7/30
              修改日期: 
              修改人员:            
              修改原因: 
      -----------------------------------------------------------------------
      ---存在问题：
      -----------------------------------------------------------------------
      +=====================================================================*/ 

drop table if exists dwm.dwd_th_ffm_rollbacktimely_day;
create table dwm.dwd_th_ffm_rollbacktimely_day as
-- 销退单的入库时效
SELECT 
    状态,
    创建时间,
    审核时间,
    销退登记时间,
    收货完成时间,
    上架时间,
    上架状态,
    销退入库单号,
    原订单号,
    原运单号,
    订单来源,
    外部单号,
    货主名称,
    销退单类型,
    退款状态,
    退款账号,
    退货人,
    退货人联系方式,
    承运商,
    运单号,
    创建人,
    客服备注,
    商品种类,
    商品数量,
    仓库名称,
    仓库,
    收货质检最晚时间,
    上架最晚时间,
    if(收货完成时间 <= 收货质检最晚时间, 1, 0)         及时收货订单数,
    if(上架时间 <= 上架最晚时间, 1, 0)                及时上架订单数
FROM (
        SELECT an.状态,
            an.创建时间,
            an.审核时间,
            an.销退登记时间,
            an.收货完成时间,
            an.上架时间,
            an.上架状态,
            an.销退入库单号,
            an.原订单号,
            an.原运单号,
            an.订单来源,
            an.外部单号,
            an.货主名称,
            an.销退单类型,
            an.退款状态,
            an.退款账号,
            an.退货人,
            an.退货人联系方式,
            an.承运商,
            an.运单号,
            an.创建人,
            an.客服备注,
            an.商品种类,
            an.商品数量,
            an.仓库名称,
            an.仓库
            , case
                when cutoff is null and sh.off_date is not null
                    then concat(统计日期, ' ', '23:59:59') -- 若是 default规则 且 节假日 取 统计日期后一天
                when cutoff is null and sh.off_date is null
                    then concat(统计日期后一天, ' ', substr(销退登记时间, 12, 8)) -- 若是 default规则 且非节假日 取 统计日期后二天  已到货时间

                when cutoff is not null and sh.off_date is not null
                    then 收货质检截单前最晚时间 -- 若是 特殊规则 且 节假日 取 收货质检截单前最晚时间
                when cutoff is not null and sh.off_date is null and
                        销退登记时间 >= concat(sla_start_date, ' ', cutoff) 
                    then 收货质检截单后最晚时间 -- 若是 特殊规则 且 车辆到港时间 大于 截单时间 取 收货质检截单后最晚时间
                when cutoff is not null and sh.off_date is null and
                        销退登记时间 < concat(sla_start_date, ' ', cutoff) 
                    then 收货质检截单前最晚时间 -- 若是 特殊规则 且 车辆到港时间 小于 截单时间 取 截单前最晚时间
            end          收货质检最晚时间
            , case
                when cutoff is null and sh.off_date is not null
                    then concat(统计日期后一天, ' ', '23:59:59') -- 若是 default规则 且 节假日 取 统计日期后一天
                when cutoff is null and sh.off_date is null
                    then concat(统计日期后二天, ' ', substr(销退登记时间, 12, 8)) -- 若是 default规则 且 非节假日 取 统计日期后二天 

                when cutoff is not null and sh.off_date is not null
                    then 上架截单前最晚时间 -- 若是 特殊规则 且 节假日 取 收货质检截单前最晚时间
                when cutoff is not null and 销退登记时间 >= concat(sla_start_date, ' ', cutoff) then 上架截单后最晚时间 
                when cutoff is not null and 销退登记时间 < concat(sla_start_date, ' ', cutoff) then 上架截单前最晚时间 
            end          上架最晚时间
        FROM (
                -- 订单明细
                SELECT case
                    dr.status
                    when 1000 then '取消退货'
                    when 1001 then '已删除此销退单'
                    when 1010 then '无需寄回直接退款'
                    when 1020 then '等待审核'
                    when 1030 then '审核完成'
                    when 1040 then '买家已寄回'
                    when 1045 then '已到货'
                    when 1050 then '收货中'
                    when 1060 then '收货完成'
                    when 1070 then '上架中'
                    when 1080 then '上架完成'
                    when 9000 then '取消'
                    else dr.status
                    end                                                      状态,
                dr.created + interval -1 hour                                创建时间,
                dr.created + interval -1 hour                                审核时间,
                dr.arrival_time                                              销退登记时间,
                dr.complete_time                                             收货完成时间,
                dr.shelf_end_time                                            上架时间,
                case
                    dr.shelf_status
                    when 1070 then '上架中'
                    when 1080 then '上架完成'
                    end                                                      上架状态,
                dr.back_sn                                                   销退入库单号,
                dr.delivery_sn                                               原订单号,
                dr.express_sn                                                原运单号,
                case
                    dr.order_source_type
                    when 1 then '人工录入'
                    when 2 then '批量导入'
                    when 3 then '接口获取'
                    when 4 then '系统生成'
                    else dr.order_source_type
                    end                                                      订单来源,
                dr.external_order_sn                                         外部单号,
                s.name                                                       货主名称,
                case
                    dr.back_type
                    when 'primary' then '普通退货'
                    when 'backgoods' then '退货换货'
                    when 'allRejected' then '全部拒收'
                    when 'package' then '包裹销退'
                    when 'crossBorder' then '跨境订单'
                    when 'interceptCrossBorder' then '拦截跨境销退'
                    else dr.back_type
                    end                                                      销退单类型,
                case
                    dr.back_status
                    when 1 then '待退款'
                    when 2 then '已退款'
                    when 3 then '不退款'
                    else dr.back_status
                    end                                                      退款状态,
                dr.bank_id                                                   退款账号,
                dr.back_man                                                  退货人,
                dr.back_man_phone                                            退货人联系方式,
                dr.back_express_name                                         承运商,
                UPPER(dr.back_express_sn)                                    运单号,
                m.real_name                                                  创建人,
                dr.back_express_status_remark                                客服备注,
                dr.kinds_num                                                 商品种类,
                dr.goods_num                                                 商品数量,
                w.name                                                    as 仓库名称,
                case
                    when w.`name` = 'Fans-B2B' then 'BST'
                    when w.`name` = 'Fans-B2C' then 'BST'
                    when w.`name` = 'PMD-WH' then 'LAS'
                    when w.`name` = 'BKK-WH-LAS物料仓' then 'LAS'
                    when w.`name` = 'AutoWarehouse' then 'AGV'
                    when w.`name` = 'BPL-Return Warehouse' then 'BPL-Return'
                    when w.`name` = 'BangsaoThong' then 'BST'
                    when w.`name` = 'Fans-Demo' then 'BST'
                    when w.`name` = 'BKK-WH-Ecommerce' then 'LAS'
                    when w.`name` = 'BPL3- Bangphli3 Livestream Warehouse' then 'BPL3'
                    when w.`name` = 'BPL4-LIVESTREAM-02' then 'BPL3'
                    when w.`name` = 'BPL3-LIVESTREAM' then 'BPL3' -- MY
                    when w.`name` = 'Puncak Alam' then 'PA'
                    when w.`name` = 'PA-FEX' then 'PA'
                    when w.`name` = 'PH-Santarosawarehouse'
                        then 'Santa rosa warehouse'
                    when w.`name` = 'Santa Rosa Material Warehouse' then 'cabuyao warehouse' -- ID
                    when w.`name` = '印尼直播仓01' then 'Royal Kosambi仓库' 
                    when w.`name` = 'GLK仓库' then 'GLK' 
                    else w.`name`
                    end                                                      仓库
                FROM wms_production.delivery_rollback_order dr
                left join wms_production.seller s on dr.seller_id = s.id
                left join wms_production.member m on dr.creator_id = m.id
                left join wms_production.warehouse w ON dr.warehouse_id = w.id
                where dr.status > 1020
                    and dr.status <> 9000
                    and dr.back_type in ('primary', 'backgoods', 'allRejected')
                    and dr.arrival_time >= date_sub(date(now() + interval -1 hour),interval 30 day) -- 近30天数据
            ) an
                LEFT JOIN
            (
                SELECT 仓库名称
                    , 货主名称
                    , sla_start_date
                    , operation
                    , cutoff
                    , 统计日期
                    , 统计日期后一天
                    , 统计日期后二天
                    , max(if(action = '收货质检', 截单前最晚时间, null)) 收货质检截单前最晚时间
                    , max(if(action = '收货质检', 截单后最晚时间, null)) 收货质检截单后最晚时间
                    , max(if(action = '上架', 截单前最晚时间, null))   上架截单前最晚时间
                    , max(if(action = '上架', 截单后最晚时间, null))   上架截单后最晚时间
                FROM (
                        SELECT 仓库名称
                            , 货主名称
                            , if(sla.`cutoff` is not null, '截单时间', '24H')                         sla_type
                            , default_sla.`sla_start_date`                                        sla_start_date
                            , default_sla.`operation`                                             operation
                            , default_sla.`action`                                                action
                            , if(sla.`cutoff` is not null, sla.`cutoff`, default_sla.`cutoff`)    cutoff
                            , if(sla.`befcutofflast` is not null, sla.`befcutofflast`,
                                    default_sla.`befcutofflast`)                                     befcutofflast
                            , if(sla.`befcutofflastdiff` is not null, sla.`befcutofflastdiff`,
                                    default_sla.`befcutofflastdiff`)                                 befcutofflastdiff
                            , if(sla.`aftcutofflast` is not null, sla.`aftcutofflast`,
                                    default_sla.`aftcutofflast`)                                     aftcutofflast
                            , if(sla.`aftcutofflastdff` is not null, sla.`aftcutofflastdff`,
                                    default_sla.`aftcutofflastdff`)                                  aftcutofflastdff
                            , default_sla.`统计日期`
                            , default_sla.`统计日期后一天`
                            , default_sla.`统计日期后二天`
                            , if(sla.`截单前` is not null, sla.`截单前`, default_sla.`截单前`)             截单前
                            , if(sla.`截单后` is not null, sla.`截单后`, default_sla.`截单后`)             截单后
                            , if(sla.`截单前最晚时间` is not null, sla.`截单前最晚时间`, default_sla.`截单前最晚时间`) 截单前最晚时间
                            , if(sla.`截单后最晚时间` is not null, sla.`截单后最晚时间`, default_sla.`截单后最晚时间`) 截单后最晚时间
                        FROM (
                                -- 默认时效规则: 发货24H,入库48H
                                SELECT 仓库名称
                                        , 货主名称
                                        , sla_start_date
                                        , operation
                                        , action
                                        , null cutoff
                                        , null befcutofflast
                                        , null befcutofflastdiff
                                        , null aftcutofflast
                                        , null aftcutofflastdff
                                        , 统计日期
                                        , 统计日期后一天
                                        , 统计日期后二天
                                        , null 截单前
                                        , null 截单后
                                        , null 截单前最晚时间
                                        , null 截单后最晚时间
                                FROM (
                                            SELECT sla_start_date
                                                , flag
                                                , 统计日期
                                                , 统计日期后一天
                                                , 统计日期后二天
                                            FROM dwm.dim_th_default_time
                                            -- WHERE sla_start_date='2023-03-06'
                                        ) default_sla
                                            LEFT JOIN
                                        (
                                            SELECT 仓库名称
                                                , 货主名称
                                                , operation
                                                , action
                                                , 1 flag
                                            FROM (
                                                    SELECT 仓库名称
                                                        , 货主名称
                                                        , 1 flag
                                                    FROM dwm.dwd_th_ffm_arrivalnotice_day
                                                        -- WHERE 货主名称 in ('haina','Fanslink')
                                                    GROUP BY 仓库名称
                                                            , 货主名称
                                                ) seller
                                                    LEFT JOIN
                                                (
                                                    SELECT operation
                                                        , action
                                                        , 1 flag
                                                    FROM tmpale.dwd_th_timeload
                                                    WHERE operation = '入库'
                                                    GROUP BY operation
                                                            , action
                                                ) sla ON seller.`flag` = sla.`flag`
                                        ) seller ON seller.`flag` = default_sla.`flag`
                            ) default_sla
                                LEFT JOIN
                            (
                                -- 特定货主时效规则 fanslink已经退仓了，没有符合的货主，此处逻辑不生效
                                SELECT warehousename
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
                                        -- ,统计日期
                                        -- ,统计日期后一天
                                        -- ,统计日期后二天
                                        , 截单前
                                        , 截单后
                                        , 截单前最晚时间
                                        , 截单后最晚时间
                                FROM dwm.dim_th_special_time
                            ) sla
                            ON default_sla.`仓库名称` = sla.`warehousename` AND
                                default_sla.`货主名称` = sla.`sellername` AND 
                                default_sla.`sla_start_date` = sla.`sla_start_date` AND
                                default_sla.`operation` = sla.`operation` AND
                                default_sla.`action` = sla.`action`
                    ) sla
                GROUP BY 仓库名称
                        , 货主名称
                        , sla_start_date
                        , operation
                        , cutoff
                        , 统计日期
                        , 统计日期后一天
                        , 统计日期后二天
                -- 时效起始日期
            ) sla ON an.`仓库名称` = sla.`仓库名称` AND an.`货主名称` = sla.`货主名称` AND
                    date(an.`销退登记时间`) = sla.`sla_start_date`
                left join
            (
                SELECT sh.`off_date`
                FROM fle_staging.`sys_holiday` sh
                WHERE sh.`off_date` >= '2021-12-01'
                AND sh.`off_date` < date_add(curdate(), interval 21 day)
                AND sh.`deleted` = 0
                GROUP BY sh.`off_date`
                ORDER BY sh.`off_date`
            ) sh ON date(an.`销退登记时间`) = sh.`off_date`
        where 1=1
    ) an