-- BigQuery validation queries for ASIN ingestion.
-- Adjust snapshot_date and scope as needed.
-- Replace dataset and project names if they differ from amazon_ops.
-- This script assumes views are created with scripts/bq_views.sql.
-- Checks include raw item deduplication, ASIN daily deduplication, orders agg deduplication, EU summary row presence, and latest all view row count.

DECLARE p_scope STRING DEFAULT 'EU';
DECLARE p_snapshot_date DATE DEFAULT DATE '2026-01-17';

-- Raw items table, latest ingested_at, row_count vs distinct key count
WITH latest_items AS (
  SELECT
    MAX(ingested_at) AS max_ingested_at
  FROM `amazon_ops.probe_order_items_raw_v1`
  WHERE scope = p_scope AND snapshot_date = p_snapshot_date
)
SELECT
  p_scope AS scope,
  p_snapshot_date AS snapshot_date,
  li.max_ingested_at AS latest_ingested_at,
  COUNT(*) AS row_count,
  COUNT(DISTINCT CONCAT(amazon_order_id, '|', asin, '|', marketplace_id, '|', country)) AS distinct_key_count
FROM `amazon_ops.probe_order_items_raw_v1` oi
JOIN latest_items li
  ON oi.ingested_at = li.max_ingested_at
WHERE oi.scope = p_scope AND oi.snapshot_date = p_snapshot_date
GROUP BY 1,2,3;

-- ASIN daily table, latest ingested_at, row_count vs distinct key count
WITH latest_asin AS (
  SELECT
    MAX(ingested_at) AS max_ingested_at
  FROM `amazon_ops.probe_sales_asin_daily_v1`
  WHERE scope = p_scope AND snapshot_date = p_snapshot_date
)
SELECT
  p_scope AS scope,
  p_snapshot_date AS snapshot_date,
  la.max_ingested_at AS latest_ingested_at,
  COUNT(*) AS row_count,
  COUNT(DISTINCT CONCAT(country, '|', marketplace_id, '|', asin)) AS distinct_key_count
FROM `amazon_ops.probe_sales_asin_daily_v1` ad
JOIN latest_asin la
  ON ad.ingested_at = la.max_ingested_at
WHERE ad.scope = p_scope AND ad.snapshot_date = p_snapshot_date
GROUP BY 1,2,3;

-- Orders daily agg latest ingested_at duplicate check
WITH latest_orders_agg AS (
  SELECT
    MAX(ingested_at) AS max_ingested_at
  FROM `amazon_ops.probe_orders_daily_agg_v1`
  WHERE scope = p_scope AND snapshot_date = p_snapshot_date
)
SELECT
  IF(COUNT(*) = 0, 'PASS', 'FAIL') AS result,
  COUNT(*) AS duplicate_group_count
FROM (
  SELECT
    scope,
    snapshot_date,
    country_code,
    marketplace_id
  FROM `amazon_ops.probe_orders_daily_agg_v1` oa
  JOIN latest_orders_agg lo
    ON oa.ingested_at = lo.max_ingested_at
  WHERE oa.scope = p_scope AND oa.snapshot_date = p_snapshot_date
  GROUP BY 1,2,3,4
  HAVING COUNT(*) > 1
);

-- Orders daily agg EU summary row existence for latest ingested_at
WITH latest_orders_agg AS (
  SELECT
    MAX(ingested_at) AS max_ingested_at
  FROM `amazon_ops.probe_orders_daily_agg_v1`
  WHERE scope = p_scope AND snapshot_date = p_snapshot_date
)
SELECT
  IF(COUNT(*) = 1, 'PASS', 'FAIL') AS result,
  COUNT(*) AS summary_rows
FROM `amazon_ops.probe_orders_daily_agg_v1` oa
JOIN latest_orders_agg lo
  ON oa.ingested_at = lo.max_ingested_at
WHERE oa.scope = p_scope
  AND oa.snapshot_date = p_snapshot_date
  AND oa.country_code = 'EU'
  AND oa.marketplace_id = '__ALL__';

-- Latest all view returns a single row for scope and snapshot_date
SELECT
  IF(COUNT(*) = 1, 'PASS', 'FAIL') AS result,
  COUNT(*) AS summary_rows
FROM `amazon_ops.v_probe_sales_daily_latest_all`
WHERE scope = p_scope AND snapshot_date = p_snapshot_date;
