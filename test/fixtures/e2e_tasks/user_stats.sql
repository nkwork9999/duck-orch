-- @task name=user_stats
-- @description Count active users by country for E2E coverage
-- @inputs analytics.clean_users
-- @outputs analytics.user_stats
-- @asset name=analytics.user_stats
-- @asset_kind table
-- @asset_group e2e
-- @asset_owner data@example.com
-- @automation on_missing()
-- @freshness max_lag=60min
-- @check name=positive_users "SELECT MIN(users) FROM ${asset}" expect gt 0

CREATE OR REPLACE TABLE analytics.user_stats AS
SELECT country, COUNT(*) AS users
FROM analytics.clean_users
GROUP BY country;
