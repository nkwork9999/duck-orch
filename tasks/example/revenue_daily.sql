-- @task name=revenue_daily
-- @description 日次売上集計
-- @inputs analytics.clean_orders, analytics.clean_users
-- @outputs analytics.revenue_daily

CREATE OR REPLACE TABLE analytics.revenue_daily AS
SELECT DATE_TRUNC('day', o.ordered_at) AS day,
       u.country,
       SUM(o.amount) AS revenue
FROM analytics.clean_orders o
JOIN analytics.clean_users u ON u.id = o.user_id
GROUP BY 1, 2;
