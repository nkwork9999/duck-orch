-- @task name=clean_users
-- @description Filter active users for E2E coverage
-- @outputs analytics.clean_users
-- @asset name=analytics.clean_users
-- @asset_kind table
-- @asset_group e2e
-- @asset_owner data@example.com

CREATE OR REPLACE TABLE analytics.clean_users AS
SELECT id, country
FROM raw.users
WHERE deleted_at IS NULL;
