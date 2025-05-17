一、发现问题与数据分析思路

    核心思路：通过对比各关卡通过率、玩家行为路径和流失节点，识别异常关卡。
    关键数据：
        关卡尝试次数、成功次数、平均耗时、失败原因
        玩家属性（等级、装备、付费情况）
        失败后行为（重试/退出/流失）


    SQL 核心代码示例：
        -- 计算各关卡通过率与流失率
        WITH level_stats AS (
            SELECT
                level_id,
                COUNT(DISTINCT user_id) AS total_players,
                SUM(CASE WHEN is_success = 1 THEN 1 ELSE 0 END) AS success_count,
                SUM(CASE WHEN is_success = 0 THEN 1 ELSE 0 END) AS fail_count,
                -- 流失率：失败后3天内未登录的比例
                SUM(CASE WHEN is_success = 0 AND last_login_time < CURRENT_DATE - 3 THEN 1 ELSE 0 END) * 1.0 / SUM(CASE WHEN is_success = 0 THEN 1 ELSE 0 END) AS churn_rate
            FROM game_level_attempts
            JOIN user_sessions USING (user_id)
            GROUP BY level_id
        )
        SELECT
            level_id,
            success_count * 1.0 / (success_count + fail_count) AS pass_rate,
            churn_rate
        FROM level_stats
        WHERE pass_rate < (SELECT AVG(pass_rate) FROM level_stats) - 2 * STDDEV(pass_rate)  -- 通过率低于平均值2个标准差
        ORDER BY pass_rate ASC;




