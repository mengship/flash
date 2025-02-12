
select
    ''
    ,sku.warehouse_name
    ,sku.seller_id
    ,sku.bar_code
    ,sku.goods_name
    ,sku.LENGTH
    ,sku.width
    ,sku.height
    -- ,sku.avg_line
    ,sku.longest
    ,sku.shortest
    ,std_longest
    ,avg_longest
    ,std_shortest
    ,avg_shortest
    ,if(sku.longest >= avg_std.avglong_minus1std and sku.longest <= avg_std.avglong_add1std, 0, 1) 最长边异常一倍
    ,if(sku.longest >= avg_std.avglong_minus2std and sku.longest <= avg_std.avglong_add2std, 0, 1) 最长边异常二倍
    ,if(sku.longest >= avg_std.avglong_minus3std and sku.longest <= avg_std.avglong_add3std, 0, 1) 最长边异常三倍
    ,if(sku.shortest >= avg_std.avgshort_minus1std and sku.shortest <= avg_std.avgshort_add1std, 0, 1) 最短边异常一倍
    ,if(sku.shortest >= avg_std.avgshort_minus2std and sku.shortest <= avg_std.avgshort_add2std, 0, 1) 最短边异常二倍
    ,if(sku.shortest >= avg_std.avgshort_minus3std and sku.shortest <= avg_std.avgshort_add3std, 0, 1) 最短边异常三倍
    ,if(sku.longest >= avg_std.avglong_minus1std and sku.longest <= avg_std.avglong_add1std and sku.shortest >= avg_std.avgshort_minus1std and sku.shortest <= avg_std.avgshort_add1std, 0, 1) 异常一倍
    ,if(sku.longest >= avg_std.avglong_minus2std and sku.longest <= avg_std.avglong_add2std and sku.shortest >= avg_std.avgshort_minus2std and sku.shortest <= avg_std.avgshort_add2std, 0, 1) 异常二倍
    ,if(sku.longest >= avg_std.avglong_minus3std and sku.longest <= avg_std.avglong_add3std and sku.shortest >= avg_std.avgshort_minus3std and sku.shortest <= avg_std.avgshort_add3std, 0, 1) 异常三倍
from
(
    select
        warehouse_name
        ,seller_id
        ,seller_goods_id
        ,bar_code
        ,goods_name
        ,LENGTH
        ,width
        ,height
        ,(LENGTH + width + height)/3 avg_line
        ,volume
        ,greatest(LENGTH, width, height) longest
        ,LEAST(LENGTH, width, height) shortest
        from
    (
        select 
                sl.warehouse_name
            ,sl.seller_id
            ,sl.seller_goods_id
            ,sl.LENGTH
            ,sl.width
            ,sl.height
            ,sl.volume
            ,sg.bar_code
            ,sg.name goods_name
        from
        dwm.dwd_th_ffm_sellergoodslocation sl
        left join seller_goods sg on sl.seller_goods_id = sg.id
        where statc_date='2024-10-30' 
        group by 1,2,3,4,5,6,7,8,9
    ) t0
) sku
left join
(
    select
        ''
        ,warehouse_name
        ,seller_id
        ,'最长边'
        ,std_longest
        ,avg_longest
        ,avg_longest - 1*std_longest avglong_minus1std
        ,avg_longest - 2*std_longest avglong_minus2std
        ,avg_longest - 3*std_longest avglong_minus3std
        ,avg_longest + 1*std_longest avglong_add1std
        ,avg_longest + 2*std_longest avglong_add2std
        ,avg_longest + 3*std_longest avglong_add3std
        ,'最短边'
        ,std_shortest
        ,avg_shortest
        ,avg_shortest - 1*std_shortest  avgshort_minus1std
        ,avg_shortest - 2*std_shortest  avgshort_minus2std
        ,avg_shortest - 3*std_shortest  avgshort_minus3std
        ,avg_shortest + 1*std_shortest  avgshort_add1std
        ,avg_shortest + 2*std_shortest  avgshort_add2std
        ,avg_shortest + 3*std_shortest  avgshort_add3std
    from
    (
        select
            '标准差与平均值'
            ,warehouse_name
            ,seller_id
            -- ,STD(avg_line) std_avg_line
            -- ,AVG(avg_line) avg_avg_line
            ,STD(longest) std_longest
            ,AVG(longest) avg_longest
            ,STD(shortest) std_shortest
            ,AVG(shortest) avg_shortest
        from
        (
            select
                warehouse_name
                ,seller_id
                ,seller_goods_id
                ,LENGTH
                ,width
                ,height
                ,(LENGTH + width + height)/3 avg_line
                ,volume
                ,greatest(LENGTH, width, height) longest
                ,LEAST(LENGTH, width, height) shortest
                from
            (
                select 
                    warehouse_name
                    ,seller_id
                    ,seller_goods_id
                    ,LENGTH
                    ,width
                    ,height
                    ,volume
                from
                dwm.dwd_th_ffm_sellergoodslocation
                where statc_date='2024-10-30' 
                group by 1,2,3,4,5,6,7
            ) t0
        ) t1
        group by 1,2,3
    ) t0
) avg_std
on sku.warehouse_name = avg_std.warehouse_name 
and sku.seller_id = avg_std.seller_id