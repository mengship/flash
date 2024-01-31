/*=====================================================================+
 表名称：  dwd_th_flashbox_problem_detail
 功能描述： 这是执行中间表的示例脚本

 需求来源：FlashBox的对话记录，包括提问、回复、移交、发起关闭
 编写人员: tiance
 设计日期：2023/12/08
 修改日期:2024/01/09
 修改人员:
 修改原因: 增加对话数量的标记
 -----------------------------------------------------------------------
 ---存在问题：
 -----------------------------------------------------------------------
 +=====================================================================*/

WITH Problem_datail AS (
    -- 这里最终取到的是每个工单的每个提问、回复、转交、发起关闭的记录，包含分类、回复tab字段，但是只要有转移，转移后的问题分类、回复tab就是错的，因为只记录了开始发起的分类和tab。后面再改，这里还没改。
    SELECT
        p_detail.*,
        ROW_NUMBER() OVER (
            PARTITION BY p_detail.problem_no
            ORDER BY
                tstamp,
                row_type
        ) AS row_num,
        # 有转移和回复在同一时间的，所以排序加上了row_type，保证先reply后transfer，提问在前在后无所谓
        sum(IF(row_type = 'transfer', 1, 0)) OVER (
            PARTITION BY p_detail.problem_no
            ORDER BY
                tstamp # 创建时间
        ) AS `transfer_num` -- row_number() OVER (PARTITION BY related_tab_name ORDER BY tstamp) AS `tab_num` #这里排序没意义，要在重置成真实部门再排
        -- if(row_type = 'transfer',cate_name_b_after,NULL ) AS `transfer_cate_name_b`
    FROM
        ( # 移交 提问 回复 发起关闭 union all 到一起
            SELECT
                qa_union.*,
                cate_s.`category_name_en` AS `cate_name_s_after`,
                cate_b.`category_name_en` AS `cate_name_b_after`,
                #这2行表示当前分类，没考虑转移，后面要用转移的分类给覆盖掉
                IF(
                    cate_s.related_tab_name = '',
                    cate_b.related_tab_name,
                    cate_s.related_tab_name
                ) AS `related_tab_name`
            FROM
                (
                    -- 提问、回复、发起关闭，全部union到一起，每一条提问、回复、发起关闭一行
                    SELECT
                        problem_no,
                        'question' AS `row_type`,
                        staff_id,
                        create_time AS `tstamp`
                    FROM
                        `backyard_pro`.`mail_to_ceo` t1
                    WHERE
                        1 = 1
                        AND create_time >= '2022-01-01'
                        AND problem_no != '' # 剔除一些垃圾数据，这些没编号的数据前台没展示，要剔除
                        -- 	AND problem_no = 'PNO3586320230417111232275077'
                    UNION
                    ALL -- 回复
                    SELECT
                        problem_no,
                        'reply' AS `row_type`,
                        staff_id,
                        create_time AS `tstamp`
                    FROM
                        `backyard_pro`.`mail_reply_from_ceo`
                    WHERE
                        1 = 1
                        AND create_time >= '2022-01-01' -- 	AND problem_no = 'PNO3586320230417111232275077'
                    UNION
                    ALL -- 关闭
                    SELECT
                        problem_no,
                        'close' AS `row_type`,
                        staff_id,
                        create_time AS `tstamp`
                    FROM
                        `backyard_pro`.`ceo_mail_sys_notice` cs
                    WHERE
                        1 = 1
                        AND create_time >= '2022-01-01'
                        AND status IN (0, 1, 3) -- 	AND problem_no = 'PNO6929220231207204018167084'
                ) AS qa_union -- 	下面关联，是为了取问题大类、小类、处理tab，只要有问题转交，转交后的就是错的，后面会把转交后的分类、处理tab替换为真实值。
                left JOIN `backyard_pro`.`ceo_mail_staff_problem_order` AS problem_order ON problem_order.problem_no = qa_union.problem_no
                left join `backyard_pro`.`ceo_mail_problem_category` AS cate_s ON cate_s.`id` = problem_order.`problem_category_v2` #员工侧小类
                left join `backyard_pro`.`ceo_mail_problem_category` AS cate_b ON cate_s.parent_id = cate_b.`id` #员工侧大类
                -- 	上面是提问、回复、发起关闭的记录，下面是要union转交的记录
            UNION
            ALL -- 修改问题分类,移交,有的移交还是同一个部门同一个回复人
            SELECT
                problem_no,
                'transfer' AS `row_type`,
                staff_id,
                transfer_time AS `tstamp`,
                cate_s.`category_name_en` AS `cate_name_s_after`,
                cate_b.`category_name_en` AS `cate_name_b_after`,
                IF(
                    cate_s.related_tab_name = '',
                    cate_b.related_tab_name,
                    cate_s.related_tab_name
                ) AS `related_tab_name`
            FROM
                `backyard_pro`.`ceo_mail_problem_transfer_record` transfer_record
                LEFT JOIN `backyard_pro`.`ceo_mail_problem_category` cate_s ON cate_s.`id` = transfer_record.`transfer_category_id` #移交后的问题分类id
                LEFT JOIN `backyard_pro`.`ceo_mail_problem_category` cate_b ON cate_s.parent_id = cate_b.`id` #移交后的问题分类id大类
            WHERE
                1 = 1
                AND transfer_time >= '2022-01-01'
                AND transfer_interval_time > 0 #发现同一时间有两条移交记录的，通过这个间隔时间>0来剔除第二条，PNO6485420230930192315874262
                -- 	AND problem_no = 'PNO3586320230417111232275077'
        ) p_detail -- 下面这个关联，是为了剔除那些跨越开始时间的，问题发起在时间点前，回复或者其他操作在时间点后的工单，不这么做剔除的话就会保留下来后面的操作，影响数据结果。
        INNER JOIN (
            SELECT
                problem_no
            FROM
                `backyard_pro`.`mail_to_ceo`
            WHERE
                create_time >= '2022-01-01'
            GROUP BY
                1
        ) p_interval ON p_detail.problem_no = p_interval.problem_no
    WHERE
        1 = 1 -- and p_detail.problem_no = 'PNO7837420231110165559157551'
        -- ORDER BY p_detail.problem_no,tstamp
)
,
Problem_datail_v2 AS (-- 前面的中间表取到的是每个工单的每个提问、回复、转交、发起关闭的记录，包含分类、回复tab字段，但是只要有转移，转移后的问题分类、回复tab就是错的，因为只记录了开始发起的分类和tab。
    -- 这个中间表做了修正，对于转移记录，保留转移前的回复tab和问题分类，和转移后的回复tab和问题分类，其他操作，前后不变，记录的是同样的回复tab和问题分类
    SELECT
        problem_no,
        row_type,
        staff_id,
        tstamp,
        max(if(row_num = 1, staff_id, NULL)) over(PARTITION BY problem_no) AS `1st_question_staff_id`,
        #提问员工
        max(if(row_num = 1, tstamp, NULL)) over(PARTITION BY problem_no) AS `1st_question_tstamp`,
        # 首次提问时间
        max(IF(row_type = 'reply', tstamp, NULL)) over(PARTITION BY problem_no) AS `last_reply_tstamp`,
        # 整个工单的最后回复时间
        IF(
            row_num = 1,
            cate_name_s_after_new,
            LAG(cate_name_s_after_new, 1) OVER (
                PARTITION BY problem_no
                ORDER BY
                    row_num
            )
        ) AS `op_cate_s`,
        # 当前处理的问题小类
        # 移交钱的问题小类
        IF(
            row_num = 1,
            cate_name_b_after_new,
            LAG(cate_name_b_after_new, 1) OVER (
                PARTITION BY problem_no
                ORDER BY
                    row_num
            )
        ) AS `op_cate_b`,
        # 当前处理的问题大类
        # 移交钱的问题大类
        IF(
            row_num = 1,
            related_tab_name_new,
            LAG(related_tab_name_new, 1) OVER (
                PARTITION BY problem_no
                ORDER BY
                    row_num
            )
        ) AS `op_tab`,
        # 当前处理的tab
        # 移交钱的处理tab
        cate_name_s_after_new,
        cate_name_b_after_new,
        related_tab_name_new,
        # 如果是转移，记录的是转移后的应回复tab和问题分类，如果不是转移操作，记录的就是处理的tab和问题分类
        row_num,
        # 按照时间顺序排序
        row_number() OVER (
            PARTITION BY problem_no
            ORDER BY
                row_num DESC
        ) AS `row_num_desc`, # 按照时间顺序倒序
         transfer_num# 转移的sum，有1次转移，就+1
    FROM
        (
            -- 		下面这些处理，就是为了修正回复tab和问题分类。
            SELECT
                *,
                COALESCE(
                    cate_name_s_tmp,
                    max(cate_name_s_tmp) OVER(PARTITION BY problem_no, transfer_num),
                    cate_name_s_after
                ) AS `cate_name_s_after_new`,
                COALESCE(
                    cate_name_b_tmp,
                    max(tab_name_tmp) OVER(PARTITION BY problem_no, transfer_num),
                    cate_name_b_after
                ) AS `cate_name_b_after_new`,
                COALESCE(
                    tab_name_tmp,# transfer时会有值，其他情况为null
                    max(tab_name_tmp) OVER(PARTITION BY problem_no, transfer_num), #  transfer时会有值，其他情况为null，移交后续的tab
                    related_tab_name # 如果为第一条数据，取原始的tab
                ) AS `related_tab_name_new`
            FROM
                (
                    SELECT
                        *,
                        if(row_type = 'transfer', cate_name_s_after, null) AS `cate_name_s_tmp`,
                        if(row_type = 'transfer', cate_name_b_after, null) AS `cate_name_b_tmp`,
                        if(row_type = 'transfer', related_tab_name, null) AS `tab_name_tmp`
                    FROM
                        Problem_datail
                     WHERE problem_no = 'PNO7837420231110165559157551'
                ) t1  		ORDER BY problem_no,tstamp
        ) t0  -- WHERE problem_no = 'PNO7837420231110165559157551'
        ORDER BY problem_no,tstamp)
-- 下面只做一件事，对话排序，dialogue_num记录，第一个对话都标记1，第二个对话都标记2，
SELECT
    problem_no,
    row_type,
    staff_id,
    tstamp,
    1st_question_staff_id,
    1st_question_tstamp,
    last_reply_tstamp,
    op_cate_s,
    op_cate_b,
    -- Incorrect of Incentive amount这个小类特殊，要单独摘出来，所以这里替换了部门名字
    IF(
        op_cate_s = 'Incorrect of Incentive amount',
        'Incentive_Virtual',
        op_tab
    ) AS `op_tab`,
    cate_name_s_after_new,
    cate_name_b_after_new,
    IF(
        cate_name_s_after_new = 'Incorrect of Incentive amount',
        'Incentive_Virtual',
        related_tab_name_new
    ) AS `related_tab_name_new`,
    row_num,
    row_num_desc,
    last_row_type,
    sum(dialogue_mark) OVER (
        PARTITION BY problem_no
        ORDER BY
            row_num
    ) AS `dialogue_num` # 累加对话数
FROM
    (
        SELECT
            *,
            -- 新对话的标记：
            -- 提问，前面是回复或者空值，做标记
            -- 转移，前面是转移，做标记
            CASE
                WHEN row_type = 'question'
                AND last_row_type IS NULL
                OR last_row_type = 'reply' THEN 1
                WHEN row_type = 'transfer'
                AND last_row_type = 'transfer' THEN 1
                ELSE NULL
            END AS `dialogue_mark`
        FROM
            (
                SELECT
                    *,
                    lag(row_type, 1) over(
                        PARTITION BY problem_no
                        ORDER BY
                            row_num
                    ) AS `last_row_type` # 取上一条数据的类型：提问 回复 移交 发起关闭
                FROM
                    Problem_datail_v2 -- 		WHERE problem_no IN ('PNO7837420231110165559157551','PNO1871420231127123641572984')
            ) t0
    ) t0 -- ORDER BY problem_no,row_num
