-- BigQuery validation queries for ASIN ingestion.
-- Adjust snapshot_date and scope as needed.
-- Replace dataset/project names if they differ from amazon_ops.

DECLARE p_scope STRING DEFAULT 'EU';
DECLARE p_snapshot_date DATE DEFAULT DATE '2026-01-17';

-- 1) Raw items table: latest ingested_at, row_count vs distinct key count
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
  COUNT(1) AS row_count,
  COUNT(DISTINCT CONCAT(amazon_order_id, '|', asin, '|', marketplace_id, '|', country)) AS distinct_key_count
FROM `amazon_ops.probe_order_items_raw_v1` oi
JOIN latest_items li
  ON oi.ingested_at = li.max_ingested_at
WHERE oi.scope = p_scope AND oi.snapshot_date = p_snapshot_date
GROUP BY 1,2,3;

-- 2) ASIN daily table: latest ingested_at, row_count vs distinct key count
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
  COUNT(1) AS row_count,
  COUNT(DISTINCT CONCAT(country, '|', marketplace_id, '|', asin)) AS distinct_key_count
FROM `amazon_ops.probe_sales_asin_daily_v1` ad
JOIN latest_asin la
  ON ad.ingested_at = la.max_ingested_at
WHERE ad.scope = p_scope AND ad.snapshot_date = p_snapshot_date
GROUP BY 1,2,3;
