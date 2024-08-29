-- 菜鸟库存库位信息 娄雨的逻辑
SELECT 
    date
    ,s.name
    ,w.name
    ,lo.location_code
    ,sglas.`location_area_by_share` 
FROM `seller_goods_location_area_snapshot` sglas
left join `seller` s on s.id = sglas.`seller_id` 
left join `warehouse` w on w.id = sglas.`warehouse_id` 
left join `location` lo on lo.id = sglas.`location_id` 
  where 1=1
--   s.name in ('Global E-Commerce-SEA Operations-Integrated Marketing-Thailand', 'TikTok-SEA-Thailand-Content Programming', 'TikTok-SEA-Thailand-Sports & Gaming', 'TikTok-SEA-Thailand-Entertainment & Music', 'TikTok 产研算-部门助理-US', 'TTSadmin-Thailand', 'TikTok-SEA-Thailand-Lifestyle.Education &Emerging', 'Pico东南亚市场', 'lemon8-Thailand', 'OC-Thailand', 'TikTok Ops-Thailand', 'TTLIVE-Thailand', 'Ragnarok X-Thailand', 'ByteDance')
  and date(date) between date('2024-07-01') and date('2024-07-31')
  group by 1,2,3,4