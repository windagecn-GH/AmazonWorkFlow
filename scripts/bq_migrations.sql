-- Idempotent schema alignment for ASIN order-item ingestion
-- Safe to re-run; uses ADD COLUMN IF NOT EXISTS.

-- Replace these if your dataset/project differs
-- Default dataset: amazon_ops
-- Tables assumed:
-- amazon_ops.probe_order_items_raw_v1
-- amazon_ops.probe_sales_asin_daily_v1

ALTER TABLE `amazon_ops.probe_order_items_raw_v1`
ADD COLUMN IF NOT EXISTS seller_sku STRING;

ALTER TABLE `amazon_ops.probe_order_items_raw_v1`
ADD COLUMN IF NOT EXISTS quantity_ordered INT64;

ALTER TABLE `amazon_ops.probe_order_items_raw_v1`
ADD COLUMN IF NOT EXISTS country STRING;

ALTER TABLE `amazon_ops.probe_order_items_raw_v1`
ADD COLUMN IF NOT EXISTS marketplace_id STRING;

CREATE TABLE IF NOT EXISTS `amazon_ops.probe_sales_asin_daily_v1` (
  run_id STRING,
  scope STRING,
  snapshot_date DATE,
  country STRING,
  marketplace_id STRING,
  asin STRING,
  orders_count INT64,
  units_sold INT64,
  canceled_orders INT64,
  excluded_non_amazon_orders INT64,
  ingested_at TIMESTAMP
);
