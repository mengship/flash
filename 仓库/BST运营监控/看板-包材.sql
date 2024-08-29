-- wms
select
     bd.业务日期
    ,week(left(bd.业务日期,10)+ interval 1 day) 周
    ,bd.仓库
    ,bd.包材名称
    ,sum(bd.条码数量)   数量
    ,max(ps.价格)  单价
    ,sum(bd.条码数量)*max(ps.价格)  成本
from
(
    -- wms
    select 
        -- a.business_sn     as                         '包材单号',
        a.business_date   as                         '业务日期',
        -- s.name            as                         '货主',
        -- w.name            as                         '仓库',
        case when w.name='AutoWarehouse' then 'AGV'
            when w.name='BPL-Return Warehouse' then 'BPL-Return'
            when w.name='BPL3-LIVESTREAM' then 'BPL3'
            when w.name='BangsaoThong' then 'BST'
            when w.name IN ('BKK-WH-LAS2电商仓')         then 'LAS'
            when w.name='LCP Warehouse' then 'LCP' end 仓库,
        -- 0 + convert(a.settlement_amount / 100, char) '结算金额',
        -- c.container_id    as                         '包材ID',
        d.name            as                          包材名称,
        -- d.bar_code        as                         '包材条码',
        c.number          as                         '条码数量',
        -- e.seller_goods_id as                         '包材映射商品ID',
        f.bar_code        as                         '包材映射商品条码'
    from wms_production.billing_detail a
        join wms_production.billing_projects b on a.billing_projects_id = b.id
        join wms_production.container_inventory_log c on a.business_id = c.container_order_id
        join wms_production.container d on c.container_id = d.id
        left join wms_production.container_goods_mapping e on e.container_id = c.container_id
        left join wms_production.seller_goods f on e.seller_goods_id = f.id
        join wms_production.seller s on a.seller_id = s.id
        join wms_production.warehouse w on a.warehouse_id = w.id
    where b.documents = 6
    and a.business_date >= date_sub(date(now() + interval -1 hour),interval 70 day)
    and d.seller_id = 0
    and s.disabled = 0

    union all
    -- erp
    select
        cubd.date
        ,case when w.name='AutoWarehouse'   then 'AGV'
            when w.name='BPL-Return Warehouse'  then 'BPL-Return'
            when w.name='BPL3-LIVESTREAM'   then 'BPL3'
            when w.name='BangsaoThong'  then 'BST'
            when w.name IN ('BKK-WH-LAS2电商仓')    then 'LAS'
            when w.name ='LCP Warehouse' then 'LCP' end 仓库
        ,c.name 包材名称
        ,cubd.actual_num+0 条码数量
        ,c.external_code 包材映射商品条码
        -- ,cubd.seller_id
        -- ,cubd.warehouse_id
        
    from 
    erp_wms_prod.container_use_billing_detail cubd
    left join 
    erp_wms_prod.container c on if(cubd.container_id=0, actual_container_id, cubd.container_id) = c.id
    left join
    erp_wms_prod.seller sl on cubd.seller_id = sl.id
    LEFT JOIN 
    erp_wms_prod.warehouse w ON cubd.warehouse_id = w.id
    where w.name='BPL3-LIVESTREAM'
    and cubd.date >= date_sub(date(now() + interval -1 hour),interval 70 day)
) bd
  left join
(
    select
        bar_code
        ,价格
    from
    (
        select
            bar_code
            ,价格
            ,real_arrival_date
            ,row_number() over(partition by bar_code order by real_arrival_date desc) rn
        from
        (
            -- 资产采购
            select 
                ps.psno,
                ps.cost_company_name,
                case ps.currency
                    when 1 then 'THB'
                    when 2 then 'USD'
                    when 3 then 'CNY'
                end 币种,
                case when mtr.name ='水果箱(新款)S+' then '水果箱S+'
                when mtr.name ='水果箱(新款)M' then '水果箱M'
                when mtr.name ='水果箱(新款)M+' then '水果箱M+'
                when mtr.name ='水果箱(新款)L' then '水果箱L'
                else  mtr.name end 商品名称,
                psp.product_option_code bar_code,
                case ps.currency
                    when 1 then psp.price/1000000
                    when 2 then (psp.price/1000000)* 36.75
                    when 3 then (psp.price/1000000)* 5
                end 价格,
                psp.this_time_num  收货数量,
                if(po.vendor like '%FLASH%','调拨','采购') 采购方式,
                psp.real_arrival_date
            from oa_production.purchase_storage ps
            left join oa_production.purchase_storage_product psp on ps.id=psp.psnoid
            left join oa_production.purchase_order po on po.pono=ps.po
            left join tmpale.tmp_th_mtr_name_list mtr  on mtr.bar_code=psp.product_option_code
            where psp.real_arrival_date is not null
                and ps.status in (3,4,6)
                and ps.cost_company_name='Flash Express'
                -- and mtr.category='物料'
                and mtr.name  in
                ('集包扎带','封车扎带','A4塑料袋','A3塑料袋','集包袋','蓝牙打印纸','Flash胶带','透明胶带','A4气泡袋','A4信封袋',
                '易碎贴纸','COD贴纸','易碎贴纸','PC打印纸单层','大PC打印纸','拉伸膜','气泡膜','小号集包袋',
                '水果箱(新款)C+9','水果箱(新款)D+11','水果箱M','水果箱M+','水果箱(新款)M','水果箱(新款)S+','水果箱(新款)L','水果箱(新款)M+','水果箱L','水果箱S+',
                '纸箱(棕色)Mini','纸箱(棕色)S', '纸箱(棕色)S+','纸箱(棕色)M','纸箱(棕色)M+','纸箱(棕色)L',
                '纸箱(黄色)Mini','纸箱(黄色)S', '纸箱(黄色)S+','纸箱(黄色)M','纸箱(黄色)M+','纸箱(黄色)L')
            group by 1,2,3,4,5,6,7,8,9
        ) ps0
    ) ps1
    where ps1.rn=1
) ps on bd.包材映射商品条码 = ps.bar_code
group by    bd.业务日期
    ,week(left(bd.业务日期,10)+ interval 1 day)
    ,bd.仓库
    ,bd.包材名称
order by bd.业务日期 desc
    ,bd.仓库
    ,bd.包材名称;