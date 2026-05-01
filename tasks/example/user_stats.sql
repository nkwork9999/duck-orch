-- @task name=user_stats
-- @description 国別アクティブユーザー数
-- @outputs analytics.user_stats
-- @schedule "0 6 * * *"
-- @test "SELECT COUNT(*) FROM analytics.user_stats WHERE users < 0" expect 0

CREATE OR REPLACE TABLE analytics.user_stats AS
SELECT country, COUNT(*) AS users
FROM analytics.clean_users
GROUP BY country;
