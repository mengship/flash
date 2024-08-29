SELECT 
    t1.'仓库'
    ,count(DISTINCT t1.'库位编码')                 '规划库位数'
    ,sum(if(t1.'正品库存' > 0, 1, 0))                 '使用库位数'
    ,sum(if(t1.'正品库存' > 0, 1, 0)) / count(distinct t1.'库位编码') '库位利用率' 
    ,sum(t1.'体积') / 1000000000                      '规划库容'
    ,sum(if(t1.'正品库存' > 0, t1.'体积', 0)) / 1000000000                '使用库容'
    ,sum(if(t1.'正品库存' > 0, t1.'体积', 0))/sum(t1.'体积')                   '库容利用率'     

    ,'1-1'
    ,count(DISTINCT (if(t1.'库位编码' like 'H%', t1.'库位编码',0)))  -1             '高位货架库位-规划'
    ,sum(if(((t1.'库位编码' like 'H%') and t1.'正品库存' > 0 ), 1, 0))                 '高位货架库位' #使用
    ,sum(if(t1.'库位编码' like 'H%',t1.'体积',0)） / 1000000000                      '高位货架库容-规划'
    ,sum(if(((t1.'库位编码' like 'H%') and t1.'正品库存' > 0), t1.'体积', 0)) / 1000000000    '高位货架库容' #使用 

    ,'1-2'
    ,count(DISTINCT (if(t1.'库位编码' like 'FA-B%', t1.'库位编码',0)))  -1             '轻型货架库位-规划'
    ,sum(if(((t1.'库位编码' like 'FA-B%') and t1.'正品库存' > 0 ), 1, 0))                 '轻型货架库位' #使用
    ,sum(if(t1.'库位编码' like 'FA-B%',t1.'体积',0)） / 1000000000                      '轻型货架库容-规划'
    ,sum(if(((t1.'库位编码' like 'FA-B%') and t1.'正品库存' > 0), t1.'体积', 0)) / 1000000000    '轻型货架库容' #使用
  
FROM 
(
    SELECT 
        'AGV' as '仓库'
        ,bes.`code` '库位编码'
        ,bes.`width`*bes.`height`*bes.`depth` '体积'
        ,if(bi.`num`>0,1,0)  '正品库存'
    FROM `was`.`base_entity_slot` bes 
    LEFT JOIN 
    (
        select
            slot_code
            ,MAX(num) 'num'
        FROM `was`.`base_inventory` i
        where i.`group_id` = '1180'   #一个库位有多个业务实体，园区code一致
        group by slot_code
    ) bi on bes.`code`= bi.`slot_code`
    LEFT JOIN `was`.`base_entity_bucket` beb on bes.`bucket_code`=beb.`code` and bes.park_id=beb.park_id
    LEFT JOIN `was`.`base_entity_opr_area` beoa on beb.`opr_area_code`=beoa.`code`  and beb.`park_id` = beoa.`park_id`  #+园区条件
    LEFT JOIN `was`.`base_entity_opr_area_group` beoag on beoa.`park_id`=beoag.`park_id`
    WHERE bes.`del_flag`= 0 
        AND bes.`enable_flag`= 1     #库位启用状态为启用
        AND beoa.`status`='SELLABLE'   #作业区为可售状态
        AND beoag.`name`='AGV WH GROUP 01'  #作业区组为AGV
        AND bes.park_id = 16          #园区为AGV
        AND (bes.`code` like 'H%' or bes.`code` like 'FA-B%')
)  t1
GROUP by 1