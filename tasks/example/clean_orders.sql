-- @task name=clean_orders
-- @description raw_orders をクレンジング
-- @outputs analytics.clean_orders

CREATE OR REPLACE TABLE analytics.clean_orders AS
SELECT id, user_id, amount, ordered_at
FROM raw.orders
WHERE amount > 0;
