-- BigQuery validation queries for ASIN ingestion
-- Adjust snapshot_date and scope as needed.

DECLARE p_scope STRING DEFAULT 'EU';
DECLARE p_snapshot_date DATE DEFAULT DATE '2026-01-17';

-- Basic sanity checks
SELECT
  p_scope AS scope,
  p_snapshot_date AS snapshot_date,
  COUNT(1) AS order_items_rows
FROM `amazon_ops.probe_order_items_raw_v1`
WHERE scope = p_scope AND snapshot_date = p_snapshot_date;

SELECT
  p_scope AS scope,
  p_snapshot_date AS snapshot_date,
  COUNT(1) AS asin_daily_rows,
  COUNT(DISTINCT CONCAT(country,'|',marketplace_id,'|',asin)) AS asin_keys_distinct
FROM `amazon_ops.probe_sales_asin_daily_v1`
WHERE scope = p_scope AND snapshot_date = p_snapshot_date;

-- Duplicate key check (should return 0 rows)
SELECT
  country,
  marketplace_id,
  asin,
  COUNT(1) AS dup_cnt
FROM `amazon_ops.probe_sales_asin_daily_v1`
WHERE scope = p_scope AND snapshot_date = p_snapshot_date
GROUP BY 1,2,3
HAVING COUNT(1) > 1;
