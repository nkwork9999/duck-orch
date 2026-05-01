-- @task name=clean_users
-- @description raw_users から論理削除済みを除外
-- @outputs analytics.clean_users
-- @retries 2

CREATE OR REPLACE TABLE analytics.clean_users AS
SELECT id, name, country, created_at
FROM raw.users
WHERE deleted_at IS NULL;
