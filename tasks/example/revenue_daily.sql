-- @task name=revenue_daily
-- @description 日次売上集計
-- @outputs analytics.revenue_daily
-- @incremental_by ordered_at

INSERT INTO analytics.revenue_daily
SELECT DATE_TRUNC('day', o.ordered_at) AS day,
       u.country,
       SUM(o.amount) AS revenue
FROM analytics.clean_orders o
JOIN analytics.clean_users u ON u.id = o.user_id
WHERE o.ordered_at > {{ last_processed_at }}
  AND o.ordered_at <= {{ now }}
GROUP BY 1, 2;
