-- BigQuery views for sales daily latest snapshots.
-- Update dataset and project names if they differ from amazon_ops.

CREATE OR REPLACE VIEW `amazon_ops.v_probe_sales_daily_latest_country` AS
SELECT
  ingested_at,
  run_id,
  scope,
  snapshot_date,
  country_code AS country,
  marketplace_id,
  orders_count,
  units_sold,
  filter_mode,
  excluded_canceled_orders,
  excluded_non_amazon_orders
FROM `amazon_ops.probe_orders_daily_agg_v1`
WHERE country_code != "EU" AND marketplace_id != "__ALL__"
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY snapshot_date, scope, country_code, marketplace_id
  ORDER BY ingested_at DESC
) = 1;

CREATE OR REPLACE VIEW `amazon_ops.v_probe_sales_daily_latest_all` AS
SELECT
  ingested_at,
  run_id,
  scope,
  snapshot_date,
  country_code AS country,
  marketplace_id,
  orders_count,
  units_sold,
  filter_mode,
  excluded_canceled_orders,
  excluded_non_amazon_orders
FROM `amazon_ops.probe_orders_daily_agg_v1`
WHERE country_code = "EU" AND marketplace_id = "__ALL__"
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY snapshot_date, scope
  ORDER BY ingested_at DESC
) = 1;
