SELECT 
 * 
from(
    SELECT 
        LEFT(created_time,10) 日期
        ,null paltform
        ,warehouse_name
        ,TYPE 单据
        ,'已审核单量' 指标
        ,COUNT(delivery_sn) 数值
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where  LEFT(created_time,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
    AND audit_time IS NOT NULL
  	and is_visible=1
    GROUP BY 1,2,3,4

    UNION all 
    SELECT 
        LEFT(created_time,10) 日期
        ,null paltform
        ,warehouse_name
        ,type 单据
        ,'未审核单量' 指标
        ,COUNT(delivery_sn)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where LEFT(created_time,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
    AND  audit_time IS  NULL
    and is_visible=1
    GROUP BY 1,2,3,4

    UNION all 
    SELECT 
        LEFT(if(type='B2C', delivery_time, pack_time),10) 日期
        ,null paltform
        ,warehouse_name
        ,type 单据
        ,'商品数量' 指标
        ,sum(goods_num)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where LEFT(created_time,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
    and audit_time is not null
  	and is_visible=1
    GROUP BY 1,2,3,4

    UNION all
    SELECT 
        LEFT(if(type='B2C', delivery_time, pack_time),10) 日期
        ,null paltform
        ,warehouse_name
        ,type 单据
        ,'出库单量' 指标
        ,COUNT(delivery_sn)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where LEFT(pack_time,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
  	and is_visible=1
    GROUP BY 1,2,3,4
  
	-- 20241011新增指标【交接单量】
    UNION all
    SELECT 
        LEFT(handover_time,10) 日期
        ,null paltform
        ,warehouse_name
        ,type 单据
        ,'交接单量' 指标
        ,COUNT(delivery_sn)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where LEFT(handover_time,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
  	and is_visible=1
    GROUP BY 1,2,3,4	
  
    -- Shopee
    UNION all 
    SELECT 
        LEFT(deadline,10) 日期
        ,'Shopee' paltform
        ,warehouse_name
        ,type 单据
        ,'及时发货' 指标
        ,sum(及时发货)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(deadline,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='Shopee'
  		and is_visible=1
  		and type='B2C'
    GROUP BY 1,2,3,4

    UNION all 
    SELECT 
        LEFT(deadline,10) 日期
        ,'Shopee' paltform
        ,warehouse_name
        ,type 单据
        ,'应发货' 指标
        ,sum(应发货)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(deadline,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='Shopee'
  		and is_visible=1
    	and type='B2C'
    GROUP BY 1,2,3,4

    UNION all 
    SELECT
        LEFT(deadline,10) 日期
        ,'Shopee' paltform
        ,warehouse_name
        ,type 单据
        ,'未及时发货' 指标
        ,count(delivery_sn)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where  1=1
        and 应发货=1 
        and 及时发货=0
        and is_time=1
        and platform_source='Shopee'
  		and is_visible=1
      	and type='B2C'
        and LEFT(deadline,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
    GROUP BY 1,2,3,4

    -- Tik Tok
    UNION all 
    SELECT 
        LEFT(deadline,10) 日期
        ,'Tik Tok' paltform
        ,warehouse_name
        ,type 单据
        ,'及时发货' 指标
        ,sum(及时发货)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(deadline,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='Tik Tok'
  		and is_visible=1
      	and type='B2C'
    GROUP BY 1,2,3,4

    UNION all 
    SELECT 
        LEFT(deadline,10) 日期
        ,'Tik Tok' paltform
        ,warehouse_name
        ,type 单据
        ,'应发货' 指标
        ,sum(应发货)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(deadline,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='Tik Tok'
  		and is_visible=1
      	and type='B2C'
    GROUP BY 1,2,3,4

    UNION all 
    SELECT
        LEFT(deadline,10) 日期
        ,'Tik Tok' paltform
        ,warehouse_name
        ,type 单据
        ,'未及时发货' 指标
        ,count(delivery_sn)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where  1=1
        and 应发货=1 
        and 及时发货=0
        and is_time=1
        and platform_source='Tik Tok'
      	and type='B2C'
        and LEFT(deadline,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
  		and is_visible=1
    GROUP BY 1,2,3,4


    -- LAZADA
    UNION all 
    SELECT 
        LEFT(deadline,10) 日期
        ,'LAZADA' paltform
        ,warehouse_name
        ,type 单据
        ,'及时发货' 指标
        ,sum(及时发货)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(deadline,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='LAZADA'
  		and is_visible=1
      	and type='B2C'
    GROUP BY 1,2,3,4

    UNION all 
    SELECT 
        LEFT(deadline,10) 日期
        ,'LAZADA' paltform
        ,warehouse_name
        ,type 单据
        ,'应发货' 指标
        ,sum(应发货)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(deadline,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='LAZADA'
  		and is_visible=1
      	and type='B2C'
    GROUP BY 1,2,3,4

    UNION all 
    SELECT
        LEFT(deadline,10) 日期
        ,'LAZADA' paltform
        ,warehouse_name
        ,type 单据
        ,'未及时发货' 指标
        ,count(delivery_sn)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where  1=1
        and 应发货=1 
        and 及时发货=0
        and is_time=1
        and platform_source='LAZADA'
      	and type='B2C'
        and LEFT(deadline,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
  		and is_visible=1
    GROUP BY 1,2,3,4

        -- Other
    UNION all 
    SELECT 
        LEFT(deadline,10) 日期
        ,'Other' paltform
        ,warehouse_name
        ,type 单据
        ,'及时发货' 指标
        ,sum(及时发货)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(deadline,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='Other'
  		and is_visible=1
      	and type='B2C'
    GROUP BY 1,2,3,4

    UNION all 
    SELECT 
        LEFT(deadline,10) 日期
        ,'Other' paltform
        ,warehouse_name
        ,type 单据
        ,'应发货' 指标
        ,sum(应发货)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(deadline,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='Other'
  		and is_visible=1
      	and type='B2C'
    GROUP BY 1,2,3,4

    UNION all 
    SELECT
        LEFT(deadline,10) 日期
        ,'Other' paltform
        ,warehouse_name
        ,type 单据
        ,'未及时发货' 指标
        ,count(delivery_sn)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where  1=1
        and 应发货=1 
        and 及时发货=0
        and is_time=1
        and platform_source='Other'
        and LEFT(deadline,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
  		and is_visible=1
      	and type='B2C'
    GROUP BY 1,2,3,4

    -- 打包
    -- Shopee
    UNION all 
    SELECT 
        LEFT(pack_deadline,10) 日期
        ,'Shopee' paltform
        ,warehouse_name
        ,type 单据
        ,'及时打包' 指标
        ,sum(及时打包)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(pack_deadline,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='Shopee'
  		and is_visible=1
    GROUP BY 1,2,3,4

    UNION all 
    SELECT 
        LEFT(pack_deadline,10) 日期
        ,'Shopee' paltform
        ,warehouse_name
        ,type 单据
        ,'应打包' 指标
        ,sum(应打包)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(pack_deadline,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='Shopee'
  		and is_visible=1
    GROUP BY 1,2,3,4

    UNION all 
    SELECT
        LEFT(pack_deadline,10) 日期
        ,'Shopee' paltform
        ,warehouse_name
        ,type 单据
        ,'未及时打包' 指标
        ,count(delivery_sn)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where  1=1
        and 应打包=1 
        and 及时打包=0
        and is_time=1
        and platform_source='Shopee'
        and LEFT(pack_deadline,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
  		and is_visible=1
    GROUP BY 1,2,3,4

    -- Tik Tok
    UNION all 
    SELECT 
        LEFT(pack_deadline,10) 日期
        ,'Tik Tok' paltform
        ,warehouse_name
        ,type 单据
        ,'及时打包' 指标
        ,sum(及时打包)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(pack_deadline,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='Tik Tok'
  		and is_visible=1
    GROUP BY 1,2,3,4

    UNION all 
    SELECT 
        LEFT(pack_deadline,10) 日期
        ,'Tik Tok' paltform
        ,warehouse_name
        ,type 单据
        ,'应打包' 指标
        ,sum(应打包)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(pack_deadline,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='Tik Tok'
  		and is_visible=1
    GROUP BY 1,2,3,4

    UNION all 
    SELECT
        LEFT(pack_deadline,10) 日期
        ,'Tik Tok' paltform
        ,warehouse_name
        ,type 单据
        ,'未及时打包' 指标
        ,count(delivery_sn)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where  1=1
        and 应打包=1 
        and 及时打包=0
        and is_time=1
        and platform_source='Tik Tok'
        and LEFT(pack_deadline,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
  		and is_visible=1
    GROUP BY 1,2,3,4


    -- LAZADA
    UNION all 
    SELECT 
        LEFT(pack_deadline,10) 日期
        ,'LAZADA' paltform
        ,warehouse_name
        ,type 单据
        ,'及时打包' 指标
        ,sum(及时打包)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(pack_deadline,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='LAZADA'
  		and is_visible=1
    GROUP BY 1,2,3,4

    UNION all 
    SELECT 
        LEFT(pack_deadline,10) 日期
        ,'LAZADA' paltform
        ,warehouse_name
        ,type 单据
        ,'应打包' 指标
        ,sum(应打包)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(pack_deadline,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='LAZADA'
  		and is_visible=1
    GROUP BY 1,2,3,4

    UNION all 
    SELECT
        LEFT(pack_deadline,10) 日期
        ,'LAZADA' paltform
        ,warehouse_name
        ,type 单据
        ,'未及时打包' 指标
        ,count(delivery_sn)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where  1=1
        and 应打包=1 
        and 及时打包=0
        and is_time=1
        and platform_source='LAZADA'
        and LEFT(pack_deadline,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
  		and is_visible=1
    GROUP BY 1,2,3,4

    -- Other
    UNION all 
    SELECT 
        LEFT(pack_deadline,10) 日期
        ,'Other' paltform
        ,warehouse_name
        ,type 单据
        ,'及时打包' 指标
        ,sum(及时打包)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(pack_deadline,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='Other'
  		and is_visible=1
    GROUP BY 1,2,3,4

    UNION all 
    SELECT 
        LEFT(pack_deadline,10) 日期
        ,'Other' paltform
        ,warehouse_name
        ,type 单据
        ,'应打包' 指标
        ,sum(应打包)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where 1=1
        and LEFT(pack_deadline,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
        and is_time=1
        and platform_source='Other'
  		and is_visible=1
    GROUP BY 1,2,3,4

    UNION all
    SELECT
        LEFT(pack_deadline,10) 日期
        ,'Other' paltform
        ,warehouse_name
        ,type 单据
        ,'未及时打包' 指标
        ,count(delivery_sn)
    FROM dwm.dwd_th_ffm_outbound_dayV2
    where  1=1
        and 应打包=1 
        and 及时打包=0
        and is_time=1
        and platform_source='Other'
        and LEFT(pack_deadline,10) BETWEEN LEFT(now() - INTERVAL 100 day,10) AND LEFT(now()- INTERVAL 1 day,10)
  		and is_visible=1
    GROUP BY 1,2,3,4

) t_out
order by 1,2,3,4
