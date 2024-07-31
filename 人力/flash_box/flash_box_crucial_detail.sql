/*=====================================================================+
 表名称：  dwd_th_flashbox_problem_crucial_detail
 功能描述： 这是执行中间表的示例脚本

 需求来源：FlashBox工单关键记录，每个环节一行，有开始和结束时间
 编写人员: tiance
 设计日期：2023/12/08
 修改日期: 2024/01/31
 修改原因:
 -----------------------------------------------------------------------
 ---存在问题：
 -----------------------------------------------------------------------
 +=====================================================================*/
/* drop table if exists dwm.dwd_th_flashbox_problem_crucial_detail;

create table dwm.dwd_th_flashbox_problem_crucial_detail
as  */
# 整体思路为，取出回复和移交的数据，再分别取出最早的一条数据和最晚的一条数据，

WITH Problem_no_question_datail AS (
    -- 取所有回复和转移的记录
    SELECT *,
           row_number() OVER (
               PARTITION BY problem_no,
                   op_tab,
                   tab_num_diff
               ORDER BY
                   rk_no_q
               ) AS `rk_tab_no_q`,
           row_number() OVER (
               PARTITION BY problem_no,
                   op_tab,
                   tab_num_diff
               ORDER BY
                   rk_no_q desc
               ) AS `rk_tab_no_q_desc`
    FROM (
             SELECT problem_no,
                    row_type,
                    staff_id,
                    tstamp,
                    1st_question_staff_id,
                    1st_question_tstamp,
                    last_reply_tstamp,
                    op_cate_s,
                    op_cate_b,
                    op_tab,
                    cate_name_s_after_new,
                    cate_name_b_after_new,
                    related_tab_name_new,
                    row_num,
                    row_number() OVER (
                        PARTITION BY problem_no
                        ORDER BY
                            row_num
                        ) AS `rk_no_q`,
                    # 按时间排序
                    # 这里是为了取环节，同一个tab在不同环节要能区分开，用tab排序和时间排序的差，可以做这个区分
                    row_number() OVER (
                        PARTITION BY problem_no
                        ORDER BY
                            row_num
                        ) - row_number() OVER (
                        PARTITION BY problem_no,
                            op_tab
                        ORDER BY
                            row_num
                        ) AS `tab_num_diff`,
                    dialogue_num
             FROM dwm.dwd_th_flashbox_problem_detail p_detail
                      INNER JOIN backyard_pro.hr_staff_info hsi
                                 ON p_detail.1st_question_staff_id = hsi.staff_info_id # 关联员工表，本来要剔除外协，发现不用剔除。
             WHERE 1 = 1
               AND row_type IN ('transfer', 'reply') # 剔除提问，保留回复和转移
         ) t0
    ORDER BY problem_no, row_num
),
-- 每个工单每个环节一行，操作保留回复、转移
# 取出环节的第一条数据和最后一条数据
     Problem_no_question_crucial_datail AS (
         SELECT tab_asc1.problem_no,
                tab_desc1.dialogue_num - tab_asc1.dialogue_num + 1 AS `dialogue_cnt`,    # 这是什么意思呢？
                row_number() OVER (
                    PARTITION BY tab_asc1.problem_no
                    ORDER BY
                        tab_asc1.row_num
                    )                                              AS `t_rank`,
                # 环节倒序
                row_number() OVER (
                    PARTITION BY tab_asc1.problem_no
                    ORDER BY
                        tab_asc1.row_num DESC
                    )                                              AS `t_rank_desc`,
                # 环节倒序
                # 这里如果是第一个环节，这三个字段用首次提问来补。除了第一个环节，理论上没有空值，所以就直接ifnull补了
                ifnull(
                        lag(tab_desc1.row_type, 1) OVER (
                            PARTITION BY tab_asc1.problem_no
                            ORDER BY
                                tab_asc1.row_num
                            ),
                        'question'
                    )                                              AS `demand_type`,     # 环节的提问类型
                ifnull(
                        lag(tab_desc1.staff_id, 1) OVER (
                            PARTITION BY tab_asc1.problem_no
                            ORDER BY
                                tab_asc1.row_num
                            ),
                        tab_desc1.1st_question_staff_id
                    )                                              AS `demand_staff_id`, # 环节提问人id
                ifnull(
                        lag(tab_desc1.tstamp, 1) OVER (
                            PARTITION BY tab_asc1.problem_no
                            ORDER BY
                                tab_asc1.row_num
                            ),
                        tab_desc1.1st_question_tstamp
                    )                                              AS `demand_tstamp`,   # 环节提问时间
                tab_asc1.row_type                                  AS `first_row_type`,
                tab_asc1.staff_id                                  AS `first_staff_id`,
                tab_asc1.tstamp                                    AS `first_tstamp`,
                tab_asc1.op_cate_s                                 AS `first_cate_s`,
                tab_asc1.op_cate_b                                 AS `first_cate_b`,
                tab_asc1.op_tab,
                tab_desc1.row_type                                 AS `last_row_type`,
                tab_desc1.staff_id                                 AS `last_staff_id`,
                tab_desc1.tstamp                                   AS `last_tstamp`,
                tab_desc1.cate_name_s_after_new                    AS `last_cate_s`,
                tab_desc1.cate_name_b_after_new                    AS `last_cate_b`,
                tab_desc1.related_tab_name_new                     AS `last_tab`,
                tab_desc1.1st_question_staff_id,
                tab_desc1.1st_question_tstamp,
                tab_desc1.last_reply_tstamp
         FROM (
                  SELECT *
                  FROM Problem_no_question_datail
                  WHERE rk_tab_no_q = 1 # 取每个环节的第一行
                  -- 	ORDER BY problem_no,tstamp
              ) tab_asc1
                  LEFT JOIN (
             SELECT *
             FROM Problem_no_question_datail
             WHERE rk_tab_no_q_desc = 1 # 取每个环节的最后一行，对于最后一个操作是转移，那这部分只能取到转移前，转移后的 应回复的环节没有体现。后面要补。
             -- 	ORDER BY problem_no,tstamp
         ) tab_desc1 ON tab_asc1.problem_no = tab_desc1.problem_no
             AND tab_asc1.op_tab = tab_desc1.op_tab
             AND tab_asc1.tab_num_diff =
                 tab_desc1.tab_num_diff -- WHERE tab_asc1.problem_no = 'PNO7767020231225103459669167'
         -- ORDER BY tab_asc1.problem_no,tab_asc1.tstamp
     ),
-- SELECT * FROM Problem_no_question_crucial_datail WHERE problem_no = 'PNO7837420231110165559157551'
-- 每个工单每个部门一行，回复、转移，第一行用提问开始，只有提问没有后续的保证也能出现一次，最后一行如果是转移就额外增加一行
     Problem_crutial_detail AS (
         -- 有些工单只有提问，没有后续回复，所以要用第一次提问作为主表去关联，才保证能取到
         -- 第一个环节
         SELECT question.problem_no,
                'question'                   AS `demand_type`,
                question.staff_id            AS `demand_staff_id`,
                question.tstamp              AS `demand_tstamp`,
                first_row_type,
                first_staff_id,
                first_tstamp,
                question.op_cate_s           AS `first_cate_s`,
                question.op_cate_b           AS `first_cate_b`,
                question.op_tab,
                no_q_datail_1st.dialogue_cnt AS `dialogue_cnt`,
                # 没有回复，那就是空值
                last_row_type,
                last_staff_id,
                last_tstamp,
                last_cate_s,
                last_cate_b,
                last_tab,
                question.1st_question_staff_id,
                question.1st_question_tstamp,
                last_reply_tstamp
         FROM (
                  SELECT problem_no,
                         staff_id,
                         tstamp,
                         op_cate_s,
                         op_cate_b,
                         op_tab,
                         1st_question_staff_id,
                         1st_question_tstamp
                  FROM dwm.dwd_th_flashbox_problem_detail
                  WHERE row_num = 1 -- 		AND problem_no = 'PNO7767020231225103459669167'
                  -- 		ORDER BY problem_no,tstamp
              ) question
                  LEFT JOIN (
             SELECT *
             FROM Problem_no_question_crucial_datail
             WHERE 1 = 1
               AND t_rank = 1 -- 		AND problem_no = 'PNO7767020231225103459669167'
         ) no_q_datail_1st ON question.problem_no = no_q_datail_1st.problem_no
         UNION
             ALL
         SELECT problem_no,
                demand_type,
                demand_staff_id,
                demand_tstamp,
                first_row_type,
                first_staff_id,
                first_tstamp,
                first_cate_s,
                first_cate_b,
                op_tab,
                dialogue_cnt,
                last_row_type,
                last_staff_id,
                last_tstamp,
                last_cate_s,
                last_cate_b,
                last_tab,
                1st_question_staff_id,
                1st_question_tstamp,
                last_reply_tstamp
         FROM Problem_no_question_crucial_datail
         WHERE 1 = 1
           AND t_rank > 1 -- 	AND problem_no = 'PNO6485420230930192315874262'
         UNION
             ALL
         -- 最后一个环节如果是转移给新部门，那还要再补一行，最新部门待回复要在环节表里有体现。
         SELECT problem_no,
                'transfer'    AS `demand_type`,
                last_staff_id AS `demand_staff_id`,
                last_tstamp   AS `demand_tstamp`,
                NULL          AS `first_row_type`,
                NULL          AS `first_staff_id`,
                NULL          AS `first_tstamp`,
                last_cate_s   AS `first_cate_s`,
                last_cate_b   AS `first_cate_b`,
                last_tab      AS `op_tab`,
                NULL          AS `dialogue_cnt`,
                NULL          AS `last_row_type`,
                NULL          AS `last_staff_id`,
                NULL          AS `last_tstamp`,
                NULL          AS `last_cate_s`,
                NULL          AS `last_cate_b`,
                NULL          AS `last_tab`,
                1st_question_staff_id,
                1st_question_tstamp,
                last_reply_tstamp
         FROM Problem_no_question_crucial_datail
         WHERE 1 = 1
           AND t_rank_desc = 1
           AND last_row_type = 'transfer'
           AND last_tab != op_tab # 转移且换部门了，才需要取

         -- 	AND problem_no = 'PNO6485420230930192315874262'
     ) -- SELECT * FROM Problem_crutial_detail
-- WHERE problem_no = 'PNO7837420231110165559157551'
# Problem_stage_detail AS (
-- 工单*环节级，最终结果
SELECT now()   AS `update_time`,
       *,
       IF(
               环节首次回复时间 IS NOT NULL,
               timestampdiff(second, 环节需求时间, 环节首次回复时间) / 3600,
               NULL
           )   AS `环节首次回复时长h`,
       #不包含没处理完的，如果只有转移用的是转移时长
       IF(
                   环节状态 IN ('完成_已转移', '完成_已关闭', '接近完成_已发起关闭'),
                   timestampdiff(second, 环节首次回复时间, 环节最后回复时间) / 3600,
                   NULL
           )   AS `环节首次回复到完成时长h`,
       #不包含没处理完的，有部分完成也是NULL，因为转移到这个部门，没回复提问员工直接评价的
       IF(
                   环节状态 IN ('完成_已转移', '完成_已关闭', '接近完成_已发起关闭'),
                   timestampdiff(second, 环节需求时间, 环节最后回复时间) / 3600,
                   NULL
           )   AS `环节总处理时长h`,
       #对于完成工单，取开始到最后回复。不包含没处理完的
       CASE
           WHEN 环节状态 = '完成_已转移' THEN timestampdiff(second, 环节需求时间, 环节最后回复时间) / 3600
           WHEN 环节状态 IN ('完成_已关闭', '超时_已关闭') THEN timestampdiff(second, 环节需求时间, 实际关闭时间) / 3600
           ELSE NULL
           END AS `环节开始到关闭h`,
       #不包含没处理完的，发起关闭也不包含
       IF(
                   环节状态 IN ('进行中_已回复', '进行中_未回复', '进行中_待回复'),
                   timestampdiff(second, 环节需求时间, now()) / 3600,
                   NULL
           )   AS `未完成_已处理时长h` # 从最后一次提问或者上个部门的转移时间开始算
       -- IF(环节状态 IN ('进行中_已回复','进行中_未回复','进行中_待回复'), timestampdiff(second,greatest(环节需求时间,最后提问时间),now())/3600,NULL) AS `未完成_沉睡时长h`# 从最后一次提问或者上个部门的转移时间开始算
FROM (
         SELECT p_crutial_detail_v2.*,
                last_question_tstamp                   AS `最后提问时间`,
                IF(
                            p_crutial_detail_v2.环节倒序 = 1,
                            p_close.intiate_close_time,
                            NULL
                    )                                  AS `发起关闭时间`,
                # 这里最后一个环节关联了发起关闭时间和实际关闭时间
                # 三个梯度：p_finish的时间，p_close的时间，最后一条回复后没有提问且时间已经超过72小时
                COALESCE(
                        p_finish.close_time,
                        p_close.close_time,
                        IF(
                                        last_question_tstamp < intiate_close_time
                                    AND timestampdiff(second, intiate_close_time, now()) / 3600 > 48,
                                        date_add(最后回复时间, 3),
                                        NULL
                            ),
                        IF(
                                        last_question_tstamp < 最后回复时间
                                    AND timestampdiff(second, 最后回复时间, now()) / 3600 > 72,
                                        date_add(最后回复时间, 3),
                                        NULL
                            )
                    )                                  AS `实际关闭时间`,
                IF(staff_score > 0, staff_score, NULL) AS `工单评分`,
                # 有评分为0分的，实际应该是没打分，PNO2077020231207102202977955
                CASE
                    WHEN 环节倒序 > 1 THEN '完成_已转移' # 不是最后一个环节，那就是已经转移了
                    WHEN p_finish.close_time IS NOT NULL THEN '完成_已关闭' # 已完结的工单，则工单已经关闭了
                    WHEN p_datail_last.row_type != 'question'
                        AND (
                                 p_finish.problem_no IS NOT NULL # 已完成工单
                                 OR p_close.close_time IS NOT NULL # 已关闭工单
                                 OR (
                                         intiate_close_time IS NOT NULL # 已经发起关闭，
                                         AND last_question_tstamp < intiate_close_time # 发起关闭时间 大于 最后一个工单的提问时间
                                         AND timestampdiff(second, intiate_close_time, now()) / 3600 > 48 # 发起关闭的时间到现在的时间，大于48小时，则默认关闭
                                     )
                                 OR # 发起关闭，超48小时
                                 (
                                         intiate_close_time IS NULL # 已经发起关闭
                                         AND last_question_tstamp < 最后回复时间 # 最新的回复时间 大于 最新的提问时间
                                         AND timestampdiff(second, 最后回复时间, now()) / 3600 > 72 # 最后回复时间到现在的时间，大于72小时，则默认关闭
                                     ) # 最后一次回复后，超72小时
                             ) THEN '完成_已关闭'
                    WHEN 环节首次回复时间 IS NULL THEN '进行中_未回复' # 首次回复时间为空
                    WHEN p_datail_last.row_type = 'question' THEN '进行中_待回复' # 最新的类型是提问，那就是待回复，如果是未回复，会被上面的逻辑卡住
                    WHEN p_datail_last.row_type != 'question' # 最新的类型是 回复 关闭 移交
                        AND intiate_close_time IS NOT NULL THEN '接近完成_已发起关闭' # 有发起关闭时间 这里应该是有点儿问题，这里的发起关闭时间
                    WHEN p_datail_last.row_type != 'question' # 最新的类型是 回复 关闭 移交
                        AND intiate_close_time IS NULL THEN '进行中_已回复' # 没有发起关闭
                    ELSE '其他'
                    END                                AS `环节状态`
         FROM (
                  SELECT problem_no,
                         row_number() over (
                             PARTITION BY problem_no
                             ORDER BY
                                 demand_tstamp
                             )                 AS `环节顺序`,
                         row_number() over (
                             PARTITION BY problem_no
                             ORDER BY
                                 demand_tstamp DESC
                             )                 AS `环节倒序`,
                         demand_type           AS `环节需求类型`,
                         demand_tstamp         AS `环节需求时间`,
                         demand_staff_id       AS `环节需求发起人`,
                         op_tab                AS `回复部门`,
                         first_staff_id        AS `环节首次回复人`,
                         first_cate_b          AS `环节首次回复问题大类`,
                         first_cate_s          AS `环节首次回复问题小类`,
                         first_tstamp          AS `环节首次回复时间`,
                         first_row_type        AS `环节首次回复类型`,
                         last_staff_id         AS `环节最后回复人`,
                         last_cate_b           AS `环节最后回复问题大类`,
                         last_cate_s           AS `环节最后回复问题小类`,
                         last_tstamp           AS `环节最后回复时间`,
                         last_row_type         AS `环节最后回复类型`,
                         last_tab              AS `环节最后部门`,
                         dialogue_cnt          AS `回复次数`,
                         1st_question_staff_id AS `首次提问员工`,
                         1st_question_tstamp   AS `首次提问时间`,
                         last_reply_tstamp     AS `最后回复时间` # 这里没有转移，就纯回复
                  FROM Problem_crutial_detail -- 	WHERE problem_no = 'PNO9611220231216201321832166'
             where problem_no='PNO2254320230315151034202031'
                  -- ORDER BY problem_no,demand_tstamp
              ) p_crutial_detail_v2
                  LEFT JOIN (
             -- 取数据状态为已完成的工单，
             -- 	问题工单状态: 0-未回复; 1- 已回复; 2已完成; 3 已超时
             SELECT problem_no,
                    evaluate_time AS `close_time`,
                    staff_score,
                    problem_status
             FROM backyard_pro.ceo_mail_staff_problem_order
             WHERE 1 = 1
               AND problem_status = 2 -- 	AND problem_no = 'PNO9611220231216201321832166'
         ) p_finish ON p_crutial_detail_v2.problem_no = p_finish.problem_no
                  LEFT JOIN (
             -- 	发起关闭和确认关闭的表
             SELECT problem_no,
                    staff_id,
                    status,
                    create_time AS `intiate_close_time`,
                    IF(
                                status = 1, # 已关闭
                                date_add(update_time, INTERVAL 8 HOUR),
                                NULL
                        )       AS `close_time`
             FROM `backyard_pro`.`ceo_mail_sys_notice`
             WHERE status IN (0, 1, 3)
#                AND problem_no = 'PNO9611220231216201321832166'
         ) p_close ON p_crutial_detail_v2.problem_no = p_close.problem_no
                  LEFT JOIN (
             -- 这里是取所有明细的最后一条，如果是回复，那把状态算成已回复，如果是转移或者提问，那就把状态算成待回复
             -- 这样处理有个小BUG，当最后一个部门先回复，然后修改问题分类，转移给自己部门，那实际上是已回复，但是这种情况先忽略，不然太难写。
             SELECT problem_no,
                    row_type
             FROM dwm.dwd_th_flashbox_problem_detail
             WHERE row_num_desc = 1 -- 	AND problem_no = 'PNO9611220231216201321832166'
         ) p_datail_last ON p_crutial_detail_v2.problem_no = p_datail_last.problem_no
                  LEFT JOIN (
             -- 每个工单的最后一个提问时间
             SELECT problem_no,
                    max(tstamp) AS `last_question_tstamp`
             FROM dwm.dwd_th_flashbox_problem_detail p_detail
             WHERE row_type = 'question' -- AND problem_no = 'PNO9611220231216201321832166'
             GROUP BY 1
         ) last_question
                            ON p_crutial_detail_v2.problem_no = last_question.problem_no -- WHERE p_crutial_detail_v2.problem_no = 'PNO9291320240110120932133813'
     ) t0;
