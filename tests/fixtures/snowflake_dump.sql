-- Phase 17 fixture: a tiny Snowflake-style dynamic-table dump.
--
-- `duck-orch dynamic create-from-sql tests/fixtures/snowflake_dump.sql`
-- should parse three blocks and register one Asset per block. The
-- regex-style parser (orch_common::snowflake) tolerates `CREATE OR REPLACE`,
-- ignored Snowflake-only options (WAREHOUSE, REFRESH_MODE), and both
-- `DYNAMIC TABLE` (Snowflake spelling) and `DYNAMIC ASSET` (duckOrch).

CREATE DYNAMIC TABLE analytics.daily_total
  TARGET_LAG = '5 minutes'
  WAREHOUSE  = 'compute_wh'
  AS
  SELECT date, SUM(amount) AS total
  FROM raw.events
  GROUP BY date;

CREATE OR REPLACE DYNAMIC TABLE analytics.region_sum
  TARGET_LAG = '1 hour'
  REFRESH_MODE = 'AUTO'
  AS
  SELECT region, SUM(total) AS rt
  FROM analytics.daily_total
  GROUP BY region;

CREATE DYNAMIC ASSET analytics.hourly_users
  TARGET_LAG = '15m'
  AS
  SELECT date_trunc('hour', ts) AS hr, COUNT(DISTINCT user_id) AS users
  FROM raw.events
  GROUP BY 1;
