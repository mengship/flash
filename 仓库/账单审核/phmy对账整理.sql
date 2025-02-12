

-- 账单审核情况商务已审 数据未审
-- 整体业务单量
-- 入库费 分一二级单位
-- 发货单重量

-- 账单审核情况商务已审 数据未审

select distinct billing_sn  ,s.name,dd.billing_name 账单名称,a.business_audit_time as 商务审核时间,a.accounts_receivable/100 as 账单金额,
  if( type=1,"仓储费","快递费") as type ,date(a.business_audit_time) dd,date(a.data_audit_time)
from wms_production.billing  as a
left join ( select a.billing_name,status,b.seller_id 
 from warehouse_billing_rules as a 
 left join warehouse_billing_rules_ref  as b
 on a.id=b.warehouse_billing_rules_id
 where a.status =3) as dd
 on dd.seller_id=a.seller_id
 left join wms_production.seller s 
 on s.id=a.seller_id 
where   

 a.billing_end between date ('2024-01-01') and date date('2024-08-31')
and   data_auditor_id=0  
and a.accounts_receivable/100>=0        and date(a.business_audit_time)>='2024-05-01' 
and a.status not in (0,50)



-- 整体业务单量
select a.warehouse_name,  a.seller_name,a.comment,
a. months,a.type,a.count_id as 本期单量,number as 本期件量
from (select b.name as warehouse_name,a.name as seller_name,kk.months,kk.type,count(distinct kk.id) as count_id,a.comment,sum( number) number
from (select 
a.seller_id,
a.warehouse_id,
a.id ,
'入库单' as type,
date_format(a.complete_time,'%y#%m') as months, ba.in_num  number
from wms_production.arrival_notice as a 
left join wms_production.arrival_notice_goods ba  on a.id=ba.`arrival_notice_id` 
where DATE(a.complete_time) between date('2024-01-01') and date('2024-08-31')
union all
(select 
ba.seller_id,
ba.warehouse_id,
ba.id ,
'销退单'as type,
date_format(ba.complete_time,'%y#%m') as months,bb.`back_goods_in_number` number
from wms_production.delivery_rollback_order ba
 left join `wms_production`.`delivery_rollback_order_goods` bb on ba.`id` =bb.`delivery_rollback_order_id` 
where date(ba.complete_time) between date('2024-01-01') and date('2024-08-31')
) 
union all 
(select 
d.seller_id,
d.warehouse_id,
d.id,
'发货单'as type, 
date_format(d.delivery_time,'%y#%m')as months,dd.`goods_number` as number
from wms_production.delivery_order d
 left join `wms_production`.`delivery_order_goods` dd on  d.id=dd.`delivery_order_id` 
where date(d.delivery_time) between date('2024-01-01') and date('2024-08-31')
)
union  all
(select
r.seller_id,
r.warehouse_id,
r.id,
'出库单'as type,
date_format(r.out_warehouse_time,'%y#%m')as months,rr.`out_num` as number
from wms_production.return_warehouse  r
left join `wms_production`.`return_warehouse_goods` as rr on r.id=rr.`return_warehouse_id` 
where date(r.out_warehouse_time) between date('2024-01-01') and date('2024-08-31')  and type=1 
 )
 union all
(select
ip.seller_id,
ip.warehouse_id,
ip.id,
'拦截单'as type,
date_format(ip.shelf_on_end_time,'%y#%m')as months,ipp.had_on_num as number 
from wms_production.intercept_place ip 
 left join `wms_production`.`intercept_place_goods`as ipp on ip.id=ipp.intercept_place_id
where date(ip.shelf_on_end_time) between date('2024-01-01') and date('2024-08-31')
 )
 union all
(select
ac.seller_id,
ac.warehouse_id,
ac.id,
'贴码单'as type,
date_format(ac.mark_time,'%y#%m')as months,acc.`affixed_code_num`  as number 
from wms_production.affixed_code ac 
left join `wms_production`.`affixed_code_goods` as acc on ac.`id` =acc.`affixed_code_id`  
where date(ac.mark_time) between date('2024-01-01') and date('2024-08-31')
 )
  union all
(select
seller_id,
warehouse_id,
id,
'卸货费'as type,
date_format(FROM_UNIXTIME(audit_time),'%y#%m')as months,volume/1000/1000/1000 as number
from wms_production.load_unload_order 
where date(FROM_UNIXTIME(audit_time))between date('2024-01-01') and date('2024-08-31') and type=2 
 )
 union all
(select
seller_id,
warehouse_id,
id,
'装货费'as type,
date_format(FROM_UNIXTIME(audit_time),'%y#%m')as months,volume/1000/1000/1000 as number
from wms_production.load_unload_order 
where date(FROM_UNIXTIME(audit_time)) between date('2024-01-01') and date('2024-08-31')  and type=1 

 )
union all 
(select 
ba.from_seller_id,
ba.warehouse_id,
ba.id ,
'货权转出'as type,
date_format(ba.complete_time,'%y#%m') as months,ba.actual_num number
from wms_production.transfer_order ba
where date(ba.complete_time) between date('2024-01-01') and date('2024-08-31')
) 
union all 
(select 
ba.to_seller_id,
ba.warehouse_id,
ba.id ,
'货权转入'as type,
date_format(ba.complete_time,'%y#%m') as months,ba.actual_num number
from wms_production.transfer_order ba
where date(ba.complete_time) between date('2024-01-01') and date('2024-08-31')
) union all 
(select 
ba.seller_id,
ba.warehouse_id,
ba.id ,
'报废单'as type,
date_format(ba.complete_time,'%y#%m') as months,ba.goods_num number
from wms_production.destroy_order ba
where date(ba.complete_time) between date('2024-01-01') and date('2024-08-31')
) 
 )  as kk
left join wms_production.seller as a
on kk.seller_id=a.id
left join wms_production.warehouse as b
on kk.warehouse_id=b.id
group by 
kk.months,
kk.warehouse_id,
kk.seller_id,
kk.type,
a.comment) as a 
 where 
 seller_name ='OAHP'
order by months ,warehouse_name,seller_name,type asc





-- 退仓数据检查
select 
s.name,a.seller_goods_id ,l.location_code ,sg.name as goods_name,
a.total_inventory,a.date,sg.length/1000*sg.width/1000*sg.height /1000*a.total_inventory  as volume,
sg.length/1000,sg.width/1000,sg.height /100
from  seller_goods_location_ref_snapshot  as a
left join seller s 
on a.seller_id =s.id
left join 	location l    
on a.location_id=l.id	
left join seller_goods sg 
on sg.id =a.seller_goods_id 
where a.date=date('2024-09-01')   and s.name ='${seller_name}'


-- 核对包材
select
	gg.delivery_sn,
	gg.good_name,
	num,
	b.container_sn,
	d.settlement_amount as 包材费,
	h.settlement_amount as 手工包材,
	d.billing_name_zh,
	f.name as 包材名,
	h.billing_name_zh,
	b.number,
	b.check_time
	
from
(select gg.delivery_sn, sum(gg.goods_number) num,
	GROUP_CONCAT(distinct gg.name  , gg.goods_number separator '#' ) as good_name,gg.id,gg.seller_name
from ( select a.delivery_sn,c.goods_number,g.name,s.name as seller_name,a.id,g.weight/1000 
from 	wms_production.delivery_order as a
left join wms_production.delivery_order_goods as c
on
	c.delivery_order_id = a.id
left join wms_production.seller_goods as g
on
	g.id = c.seller_goods_id
left join wms_production.seller s 
on a.seller_id=s.id
	where 
	s.name = 'FeiTu 宁波飞土'
	and date(delivery_time) between date('2024-08-01')
and date('2024-08-31')
order by a.delivery_sn,g.name,c.goods_number)gg 
group by gg.delivery_sn,gg.id,gg.seller_name)as gg
left join wms_production.container_order as b
on
	gg.id = b.business_id
left join wms_production.container_inventory_log as g
on g.container_order_id=b.id 
left join wms_production.container as f
on f.id=g.container_id
left join (select d.from_order_sn,e.billing_name_zh ,sum(settlement_amount/100) as settlement_amount
from wms_production.billing_detail as d  -- 账单明细表
left join billing_projects as e
on e.id  =d.billing_projects_id  
where e.id  =10 -- 10 包材费 
and d.charge_sn ='AR2409015451'
 group by  d.from_order_sn,e.billing_name_zh)as d 
on d.from_order_sn=gg.delivery_sn
left join  (select d.`business_sn`,e.billing_name_zh ,sum(settlement_amount/100) as settlement_amount
from wms_production.billing_detail as d  -- 账单明细表
left join billing_projects as e
on e.id  =d.billing_projects_id  
where e.id  =17 -- 手工包材
  and d.charge_sn ='AR2409015451'
 group by  d.`business_sn` ,e.billing_name_zh)as h 
on h.`business_sn`=gg.delivery_sn
order by gg.good_name
-- 仓库费按照库龄收费
select name ,sum(charge),sum(volume ),sum(volume_1)
from (select name,age,
case when age='31-45' then sum(volume)*5
 when age='45以上' then sum(volume)*15
else 0
end charge,
sum(volume) volume ,sum(volume)*1.3 volume_1
from (select 
s.name,a.seller_goods_id ,a.date,a.in_days,sg.length/1000*sg.width/1000*sg.height /1000*a.inventory  as volume ,
case when a.in_days <=30 then '0-30天'
when a.in_days between 31 and 45 then '31-45'
else '45以上'
end age
from seller_goods_days_stock_snapshot  as a
left join seller s 
on a.seller_id =s.id
left join seller_goods sg 
on sg.id =a.seller_goods_id 
left join wms_production.warehouse w 
on a.warehouse_id =w.id 
where a.date between date('2024-04-01') and  date('2024-04-30') and s.name='${seller_name}' 
)
group by age,name)
group by name

-- 仓储费按照货位体积 货位面积 商品体积收费
select a.name,a.mon,a.goods_volume,
b.total_area as 货位面积,b.total_volume as 货位体积
from (select name,date as mon,sum(volume) as goods_volume
from (select 
s.name,a.seller_goods_id ,l.location_code ,sg.name as goods_name,
a.total_inventory,a.date,sg.length/1000*sg.width/1000*sg.height /1000*a.total_inventory  as volume,
sg.length/1000,sg.width/1000,sg.height /100
from  seller_goods_location_ref_snapshot  as a
left join seller s 
on a.seller_id =s.id
left join 	location l    
on a.location_id=l.id	
left join seller_goods sg 
on sg.id =a.seller_goods_id 
where a.date between date('2024-04-01') and date('2024-04-30')   and s.name ='${seller_name}'
and (sg.`length`is not null and  sg.width is not null and sg.height is not null ))
group by name,date) as a
left join 
(select a.date  mon,s.name,
 decode(l.location_attribute,  1 ,'小货架',2, '中货架',3 ,'大货架',4, '落地货架',5, '其他',location_attribute)货位属性,
        sum(l.length / 1000 * l.width / 1000 * ( 100 + r.share_ratio ) / 100) AS total_area,
	   sum(l.length / 1000 * l.width / 1000 *l.height/ 1000 * ( 100 + r.share_ratio ) / 100) AS total_volume
FROM
(select distinct date ,location_id ,seller_id from seller_goods_location_ref_snapshot 
where date between date('2024-04-01') and date('2024-04-30') 
) as a
left join 	location l    on a.location_id=l.id	
left join  wms_production.warehouse w 
on w.id=l.warehouse_id 
left join 	repository r  on l.repository_id = r.id 
left join 	seller as s   on s.id=a.seller_id 
where r.use_attribute NOT IN ( 'temporary', 'waitingTemporary' )   and s.name ='${seller_name}'
group by a.date,s.name) as b
on a.mon=b.mon and a.name=b.name


Fujianzhihu（福建植护）
-- 入库费 分一二级单位
select name, sum(int_num), sum(mod_num),sum(in_num),ifnull(sum(int_num),0)*7.65+IFNULL(sum(mod_num),0) *0.15 deyi,
ifnull(sum(int_num),0)*7+IFNULL(sum(mod_num),0)*2 清远晟棠礼
from (SELECT a.notice_number,b.seller_goods_id,c.name as goods_name,b.in_num,
 two_conversion, b.in_num div two_conversion  as int_num,
 ifnull(b.in_num mod  two_conversion,b.in_num) as mod_num,ss.name
from wms_production.arrival_notice a 
inner join (select distinct  b.seller_id 
from warehouse_billing_rules as a 
left join warehouse_billing_rules_ref  as b
on a.id=b.warehouse_billing_rules_id
where a.status =3 and a.billing_name ='DEYI 留仓新合同 New Contract -- Aug 1st, 2023') s
on a.seller_id =s.seller_id 
left join wms_production.arrival_notice_goods as b
on a.id=b.arrival_notice_id
left join wms_production.seller_goods as c
on c.id=b.seller_goods_id
 left join wms_production.seller ss
on s.seller_id=ss.id
where  date(complete_time) between '${first}' and '${end}' and ss.name='广东供应链(GDSCM)-清远晟棠礼 QingYuan') 
group by name 

-- 发货单情况
select
	a.delivery_sn,sum(c.goods_number) 件数,
	SUM(g.weight*c.goods_number)  /1000 重量,sum(g.volume/1000/1000/1000*c.goods_number) 体积
from
	wms_production.delivery_order as a
left join wms_production.delivery_order_goods as c
on
	c.delivery_order_id = a.id
left join wms_production.seller_goods as g
on c.seller_goods_id =g.id
left join `wms_production`.seller s on s.id=a.`seller_id` 
where  s.name ='OAHP'and date(a.delivery_time) between date('2024-01-01') and date('2024-08-31')
group by a.delivery_sn

-- 出库单情况
select  return_warehouse_sn,
sum(sum_weight)重量,sum(out_num)件数,sum(volume/1000/1000/1000*out_num) 体积,count(b.seller_goods_id) sku数
from (
SELECT  distinct s.name as seller_name,c.bar_code,a.return_warehouse_sn,b.out_num,c.name as goods_name,c.volume,
 c.weight/1000, 
 b.out_num*c.weight/1000 as sum_weight
from wms_production.return_warehouse a 
left join wms_production.return_warehouse_goods as b
on a.id=b.return_warehouse_id
left join wms_production.seller_goods as c
on c.id=b.seller_goods_id
left join address k 
on k.postal_code=k.postal_code 
left join seller as s 
on s.id=a.seller_id
where  s.name='LGF ChaHua 茶花' and 
date(out_warehouse_time) between date('2024-04-01')
and date('2024-04-30') )
group by return_warehouse_sn

-- 入库单卸货体积
elect notice_number,
s.name as seller_name,w.name,unloading_party,unloading_end,a.complete_time,
decode(a.from_order_type,3,'退货入库',1,'采购入库',2,'调拨入库',4,'其他入库',a.from_order_type) 入库类型,
a.remark ,
a.volume/1000 as入库登记体积数,
l.volume/1000/1000/1000 as 卸货单商品体积数,
from wms_production.arrival_notice a
left join seller s 
on a.seller_id=s.id
left join load_unload_order l
on a.notice_number=l.source_sn 
left join member m
on a.register_id =m.id
left join wms_production.warehouse w 
on w.id=a.warehouse_id 
where -- a.volume=0 and 
--  unloading_party='warehouse'  and
  s.name='${seller_name}'  -- and notice_number='AN2401054121'
  and a.complete_time between '${first}' and '${end}'
-- and date(a.unloading_end) between '${first}' and '${end}'

-- lazada 计费卡账单核对 （包材另外）
-- 业务单量和账单关系
select s.name,be.type as 业务类型,be.count_id 系统业务量 ,null ,bill.*,warehouse.charge,decode(bill.billing_name_zh,'仓储费',
if(abs(bill.amount-warehouse.charge)<=1,0,bill.amount-warehouse.charge),0) check_w,bill.c_sn -be.count_id check_b
from wms_production.seller  s 
left join  (select kk.seller_id ,kk.months,kk.type,count(kk.id) as count_id -- 件数
from (select 
a.seller_id,
a.id ,
'入库费' as type,
date_format(complete_time,'%y#%m') as months
from wms_production.arrival_notice a 
where DATE(complete_time) between date('2024-01-01')  and date('2024-08-31')
union all
(select 
seller_id,
id ,
'销退入库费'as type,
date_format(complete_time,'%y#%m') as months
from wms_production.delivery_rollback_order 
where date(complete_time) between date('2024-01-01')  and date('2024-08-31')
) 
union all 
(select 
do.seller_id,
do.id,
'操作费'as type,
date_format(do.delivery_time,'%y#%m')as months
from wms_production.delivery_order do
where date(delivery_time) between date('2024-01-01')  and date('2024-08-31')
)
union  all
(select
rw.seller_id,
rw.id,
'出库费'as type,
date_format(out_warehouse_time,'%y#%m')as months
from wms_production.return_warehouse rw
where date(out_warehouse_time) between date('2024-01-01')  and date('2024-08-31') --  and type=1 -- 退供出库
 )
 union all
(select
seller_id,
id,
'拦截费'as type,
date_format(shelf_on_end_time,'%y#%m')as months
from wms_production.intercept_place-- 拦截
where date(shelf_on_end_time) between date('2024-01-01')  and date('2024-08-31')
 )
 union all
(select
ac.seller_id,
ac.id,
'贴码单'as type,
date_format(mark_time,'%y#%m')as months
from wms_production.affixed_code ac -- 贴码单
where date(mark_time) between date('2024-01-01')  and date('2024-08-31'))
 union all 
 (select 
do.seller_id,
do.id,
'包材费'as type,
date_format(do.delivery_time,'%y#%m')as months
from wms_production.delivery_order do
where date(delivery_time) between date('2024-01-01')  and date('2024-08-31')
)
union all  
(select 
ba.seller_id,
ba.id ,
'报废单'as type,
date_format(ba.complete_time,'%y#%m') as months
from wms_production.destroy_order ba
where date(ba.complete_time) between date('2024-01-01') and date('2024-08-31'))
) as kk
group by
kk.seller_id,
kk.months,
kk.type) as be    -- 收费业务单量
on s.id=be.seller_id
left join (select a.seller_id , sum(charge*volume *1.3) as charge
from (select a.date,a.seller_id,
a.seller_goods_id ,a.in_days,sg.length/1000*sg.width/1000*sg.height /1000*a.inventory  as volume ,
case when a.in_days <=60 then 0
when a.in_days >60 and a.in_days <=120 then 2
when a.in_days >120 and a.in_days <=180 then 3
else 4
end charge 
from seller_goods_days_stock_snapshot  as a
left join wms_production.seller_goods sg 
on sg.id =a.seller_goods_id 
where a.date between  date('2024-01-01')  and date('2024-08-31') 
and (sg.`length`is not null and  sg.width is not null and sg.height is not null ))
group by a.seller_id ) as warehouse -- lazada系列仓储费
on s.id=warehouse.seller_id
left join (select
month (a.business_date) as months,bill.business_audit_time,bill_name.billing_name,
a.seller_id ,
bill.billing_sn  as 账单号,
sum(settlement_amount/100) as amount,
sum(adjustment_amount/100) as  adjustment_amount,
count(a.business_sn) as c_sn,
case when b.billing_name_zh like '%增值%' then '贴码单' 
when b.billing_name_zh like '%出库%' then '出库费'
when b.billing_name_zh like '%入库单%' then '入库费'
when b.billing_name_zh like '%包材费%' then '包材费'
else b.billing_name_zh  end billing_name_zh,
bill.data_auditor_id
from wms_production.billing_detail as a 
left join wms_production.billing_projects as b
on b.id=a.billing_projects_id
left join wms_production.billing as bill
on bill.billing_sn=a.charge_sn
left join ( select b.seller_id ,a.billing_name
 from warehouse_billing_rules as a 
 left join warehouse_billing_rules_ref  as b
 on a.id=b.warehouse_billing_rules_id
 where a.status =3  ) as bill_name 
 on a.seller_id=bill_name.seller_id
where business_date between  date('2024-01-01')  and date('2024-08-31')
and bill.status not in (0,50)
 and settlement_amount>=0 
 and bill.type=1 -- 仓储费
group by  b.billing_name_zh,month (a.business_date),a.seller_id,bill_name.billing_name,bill.data_auditor_id ) as bill
on be.seller_id=bill.seller_id  and be.type=bill.billing_name_zh
where billing_name  in ('Lazada客户','上海遥饮信息技术有限公司 （Lazada客户）','安德') and business_audit_time is not null and data_auditor_id=0
-- 账单和业务单量关系
select distinct s.name,be.type,be.count_id,null ,bill.*,warehouse.charge,decode(bill.billing_name_zh,'仓储费',
if(abs(bill.amount-warehouse.charge)<=5,0,bill.amount-warehouse.charge),0) check_w,bill.c_sn -be.count_id check_b
from  
 (select
month (a.business_date) as months,bill.business_audit_time,bill_name.billing_name,
a.seller_id ,
bill.billing_sn  as 账单号,
sum(settlement_amount/100) as amount,
sum(adjustment_amount/100) as  adjustment_amount,
count(a.business_sn) as c_sn,
case when b.billing_name_zh like '%增值%' then '贴码单' 
when b.billing_name_zh like '%出库%' then '出库费'
when b.billing_name_zh like '%入库单%' then '入库费'
when b.billing_name_zh like '%包材费%' then '包材费'
else b.billing_name_zh  end billing_name_zh,
bill.data_auditor_id
from wms_production.billing_detail as a 
left join wms_production.billing_projects as b
on b.id=a.billing_projects_id
left join wms_production.billing as bill
on bill.billing_sn=a.charge_sn
left join ( select b.seller_id ,a.billing_name
 from warehouse_billing_rules as a 
 left join warehouse_billing_rules_ref  as b
 on a.id=b.warehouse_billing_rules_id
 where a.status =3 ) as bill_name -- and  a.billing_name in ('安德','上海遥饮信息技术有限公司 （Lazada客户）','Lazada客户'))
 on a.seller_id=bill_name.seller_id
where business_date between  date('2024-01-01')  and date('2024-08-31')
and bill.status not in (0,50)
 -- and settlement_amount>0 
 and bill.type=1 -- 仓储费
group by  b.billing_name_zh,month (a.business_date),a.seller_id,bill_name.billing_name,bill.data_auditor_id ) as bill
left join (select a.seller_id , sum(charge*volume *1.3) as charge
from (select a.date,a.seller_id,
a.seller_goods_id ,a.in_days,sg.length/1000*sg.width/1000*sg.height /1000*a.inventory  as volume ,
case when a.in_days <=60 then 0
when a.in_days >60 and a.in_days <=120 then 2
when a.in_days >120 and a.in_days <=180 then 3
else 4
end charge 
from seller_goods_days_stock_snapshot  as a
left join wms_production.seller_goods sg 
on sg.id =a.seller_goods_id 
where a.date between  date('2024-01-01')  and date('2024-08-31') 
and (sg.`length`is not null and  sg.width is not null and sg.height is not null ))
group by a.seller_id ) as warehouse -- lazada系列仓储费
on bill.seller_id=warehouse.seller_id
left join  (select kk.seller_id ,kk.months,kk.type,count(kk.id) as count_id -- 件数
from (select 
a.seller_id,
a.id ,
'入库费' as type,
date_format(complete_time,'%y#%m') as months
from wms_production.arrival_notice a 
where DATE(complete_time) between date('2024-01-01')  and date('2024-08-31')
union all
(select 
seller_id,
id ,
'销退入库费'as type,
date_format(complete_time,'%y#%m') as months
from wms_production.delivery_rollback_order 
where date(complete_time) between date('2024-01-01')  and date('2024-08-31')
) 
union all 
(select 
do.seller_id,
do.id,
'操作费'as type,
date_format(do.delivery_time,'%y#%m')as months
from wms_production.delivery_order do
where date(delivery_time) between date('2024-01-01')  and date('2024-08-31')
)
union  all
(select
rw.seller_id,
rw.id,
'出库费'as type,
date_format(out_warehouse_time,'%y#%m')as months
from wms_production.return_warehouse rw
where date(out_warehouse_time) between date('2024-01-01')  and date('2024-08-31') --  and type=1 -- 退供出库
 )
 union all
(select
seller_id,
id,
'拦截费'as type,
date_format(shelf_on_end_time,'%y#%m')as months
from wms_production.intercept_place-- 拦截
where date(shelf_on_end_time) between date('2024-01-01')  and date('2024-08-31')
 )
 union all
(select
ac.seller_id,
ac.id,
'贴码单'as type,
date_format(mark_time,'%y#%m')as months
from wms_production.affixed_code ac -- 贴码单
where date(mark_time) between date('2024-01-01')  and date('2024-08-31'))
 union all 
 (select
 b.seller_id,
b.id,
'包材费'as type,
date_format(b.created,'%y#%m')as months 
from
wms_production.container_order as b 
where date(b.created) between date('2024-01-01')  and date('2024-08-31')
)union all (select 
ba.seller_id,
ba.id ,
'报废单'as type,
date_format(ba.complete_time,'%y#%m') as months
from wms_production.destroy_order ba
where date(ba.complete_time) between date('2024-01-01') and date('2024-08-31'))
) as kk
group by
kk.seller_id,
kk.months,
kk.type) as be    -- 收费业务单量
on bill.seller_id=be.seller_id  and bill.billing_name_zh=be.type
left join wms_production.seller s 
on s.id=bill.seller_id
where billing_name  in ('Lazada客户','上海遥饮信息技术有限公司 （Lazada客户）','安德') and business_audit_time is not null and data_auditor_id=0

-- 入库费tt 认证
select count(distinct notice_number),sum(int_num), sum(mod_num),sum(in_num),ifnull(sum(int_num),0)*0.4+IFNULL(sum(mod_num),0) *0.15 tt
from (SELECT a.notice_number,b.seller_goods_id,b.in_num,c.name,
 two_conversion, b.in_num div two_conversion  as int_num,
 ifnull(b.in_num mod  two_conversion,b.in_num) as mod_num
from wms_production.arrival_notice a 
left join wms_production.arrival_notice_goods as b
on a.id=b.arrival_notice_id
left join wms_production.seller_goods as c
on c.id=b.seller_goods_id
left join wms_production.seller s on a.seller_id=s.id
where  s.name='Shanghai Ou Duo 上海欧舵'
and date(complete_time) between date('2024-08-01')
and date('2024-08-31') and a.from_order_type  in (1,4))



-- 仓储费tt 认证 库龄货位面积
select 
name,date ,
sum(length / 1000 * width / 1000 * ( 100 + share_ratio ) / 100) AS total_area
from 
(select distinct s.name,a.date, a.location_id ,l.length,l.width,r.share_ratio from seller_goods_batch_location_days_stock_snapshot  as a
left join seller s 
on a.seller_id =s.id
left join 	location l    
on a.location_id=l.id	
left join 	repository r  on l.repository_id = r.id 
where a.date between date('2024-08-01') and date('2024-08-31')   and s.name ='ChengDai 西安诚待' and in_days>30  
and r.use_attribute NOT IN ( 'temporary', 'waitingTemporary' ))
group by name,date  
-- 操作费tt 认证 
select count(delivery_sn),count(delivery_sn)/(TIMESTAMPDIFF(DAY,'2024-08-01','2024-08-31')+1) 日均单量,
case when count(delivery_sn)/(TIMESTAMPDIFF(DAY,'2024-08-01','2024-08-31')+1)<=300 then sum(price)
when count(delivery_sn)/(TIMESTAMPDIFF(DAY,'2024-08-01','2024-08-31')+1)<=500 then sum(price)*0.95
when count(delivery_sn)/(TIMESTAMPDIFF(DAY,'2024-08-01','2024-08-31')+1)<=1500 then sum(price)*0.9
when count(delivery_sn)/(TIMESTAMPDIFF(DAY,'2024-08-01','2024-08-31')+1)<=2000 then sum(price)*0.85
when count(delivery_sn)/(TIMESTAMPDIFF(DAY,'2024-08-01','2024-08-31')+1)>2000 then sum(price)*0.8
else 0 end charge
from (select
	a.delivery_sn,sum(c.goods_number) num ,
	case when sum(c.goods_number)<=4 then 1 else 1+(sum(c.goods_number)-4)*0.3 end price 
	
from
	wms_production.delivery_order as a
left join wms_production.delivery_order_goods as c
on
	c.delivery_order_id = a.id
left join wms_production.seller_goods as g
on c.seller_goods_id =g.id
left join `wms_production`.seller s on s.id=a.`seller_id` 
where  s.name ='ChengDai 西安诚待'and date(a.delivery_time) between date('2024-08-01') and date('2024-08-31')
group by a.delivery_sn) 


-- 仓储费LZD 续约客户  
select name,-- date as mon,
sum(volume) as goods_volume,
sum(volume) *1.3*2 as charge

from (select 
s.name,a.seller_goods_id ,l.location_code ,sg.name as goods_name,
a.total_inventory,a.date,sg.length/1000*sg.width/1000*sg.height /1000*a.total_inventory  as volume,
sg.length/1000,sg.width/1000,sg.height /100
from  seller_goods_location_ref_snapshot  as a
left join seller s 
on a.seller_id =s.id
left join 	location l    
on a.location_id=l.id	
left join seller_goods sg 
on sg.id =a.seller_goods_id 
where a.date between date('2024-08-01') and date('2024-08-31')   and s.name ='LGF LeMei乐美'
and (sg.`length`is not null and  sg.width is not null and sg.height is not null ))
group by name-- ,date

-- 操作费LZD 续约客户  
select count(delivery_sn),count(delivery_sn)/(TIMESTAMPDIFF(DAY,'2024-08-01','2024-08-31')+1) 日均单量,
case when count(delivery_sn)/(TIMESTAMPDIFF(DAY,'2024-08-01','2024-08-31')+1)<=200 then sum(price)
when count(delivery_sn)/(TIMESTAMPDIFF(DAY,'2024-08-01','2024-08-31')+1)<=500 then sum(price)-count(delivery_sn)*0.7
when count(delivery_sn)/(TIMESTAMPDIFF(DAY,'2024-08-01','2024-08-31')+1)>500 then sum(price)-count(delivery_sn)*1
else 0 end charge
from (select
	a.delivery_sn,sum(c.goods_number) num ,
	case when sum(c.goods_number)<=3 then 2.5 else 2.5+(sum(c.goods_number)-3)*0.3 end price 
	
from
	wms_production.delivery_order as a
left join wms_production.delivery_order_goods as c
on
	c.delivery_order_id = a.id
left join wms_production.seller_goods as g
on c.seller_goods_id =g.id
left join `wms_production`.seller s on s.id=a.`seller_id` 
where  s.name ='LGF LeMei乐美'and date(a.delivery_time) between date('2024-08-01') and date('2024-08-31')
group by a.delivery_sn) 


-- 商品体积
select name,-- date as mon,
sum(volume) as goods_volume,sum(volume)*1.3 
from (select 
s.name,a.seller_goods_id ,l.location_code ,sg.name as goods_name,
a.total_inventory,a.date,sg.length/1000*sg.width/1000*sg.height /1000*a.total_inventory  as volume,
sg.length/1000,sg.width/1000,sg.height /100
from  seller_goods_location_ref_snapshot  as a
left join seller s 
on a.seller_id =s.id
left join 	location l    
on a.location_id=l.id	
left join seller_goods sg 
on sg.id =a.seller_goods_id 
where a.date between date('2024-08-01') and date('2024-08-31')   and s.name ='LGF 七犬QiQuan'
and (sg.`length`is not null and  sg.width is not null and sg.height is not null ))
group by name-- ,date


-- 商品库龄体积
select name,age,price,
sum(volume),price*
sum(volume)
from (select 
s.name,a.seller_goods_id ,a.date,a.in_days,sg.length/1000*sg.width/1000*sg.height /1000*a.inventory  as volume ,
case when a.in_days <=7 then '0-7天'
when a.in_days between 8 and 60 then '8-60'
when a.in_days between 61 and 120 then '61-120'
else '121以上'
end age,
case when a.in_days <=7 then 0
when a.in_days between 8 and 60 then 4.5
when a.in_days between 61 and 120 then 5.8
else 7.8
end price
from seller_goods_days_stock_snapshot  as a
left join seller s 
on a.seller_id =s.id
left join seller_goods sg 
on sg.id =a.seller_goods_id 
where a.date between date('2024-08-01') and  date('2024-08-31') and s.name='梦昕实业 MengXinShiYe')
group by name,age,price


-- 月度周转率
select a.name ,all_volume,v1,v2,all_volume/ifnull(v1,1),all_volume/ifnull(v2,1),all_volume/if(ifnull(v1,0)+ifnull(v2,0)=0,1)
from (select 
s.name,
sum(sg.length/1000*sg.width/1000*sg.height /1000*a.total_inventory)  as all_volume
from  seller_goods_location_ref_snapshot  as a
left join seller s 
on a.seller_id =s.id
left join 	location l    
on a.location_id=l.id	
left join seller_goods sg 
on sg.id =a.seller_goods_id 
where a.date between date('2024-08-01') and date('2024-08-31')   and s.name ='虾米盒子 Xiami Box'
group by s.name) as a 
left join 
(select s.name,ifnull(sum(dog.goods_number*sg.volume/1000/1000/1000) ,0)v1
from wms_production.delivery_order do 
left join wms_production.delivery_order_goods dog 
on do.id=dog.delivery_order_id 
left join wms_production.seller_goods sg 
on dog.seller_goods_id =sg.id 
left join wms_production.seller s 
on do.seller_id =s.id 
where s.name='虾米盒子 Xiami Box' and date(do.delivery_time)  between '2024-08-01' and '2024-08-31' group by s.name) b 
on a.name=b.name
left join (
select s.name,ifnull(sum(dog.out_num *sg.volume/1000/1000/1000),0) v2
from wms_production.return_warehouse   do 
left join wms_production.return_warehouse_goods  dog 
on do.id=dog.return_warehouse_id 
left join wms_production.seller_goods sg 
on dog.seller_goods_id  =sg.id 
left join wms_production.seller s 
on do.seller_id =s.id 
where s.name='虾米盒子 Xiami Box' and date(do.out_warehouse_time)  between '2024-08-01' and '2024-08-31' group by s.name) c 
on a.name=b.name


