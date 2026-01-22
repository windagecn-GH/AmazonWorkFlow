# Start Here

Project goal and current stage
- Goal is to ingest orders and order items from SP-API, compute ASIN daily sales, and validate idempotent ingestion
- Current stage has verified EU data for 2026-01-17 and focuses on hardening validation and documentation

Entry points and parameters
- Primary endpoint is /cron/daily
- Key parameters include scope, snapshot_date, dry, compact, debugItems, maxPages, pageSize, maxOrders

Stable Overview
Data flow
- Pull ListOrders and ListOrderItems from SP-API
- Write raw orders and raw items to BigQuery
- Aggregate ASIN daily metrics and write to BigQuery
- Validate latest ingested_at counts and duplicate keys for idempotency

Idempotency principle
- Use latest ingested_at per table and verify row_count equals distinct_key_count
- Duplicate-key checks must return no rows

Acceptance Criteria
- When orders_count is greater than zero, items_rows_count and asin_stats_count must not be zero
- If items_rows_count is zero for a non-zero orders_count, response must return ok false with status, error, run_id, and stage
- When debugItems is true or compact is false, response must include debug.order_items_by_country with required fields
- units_sold is computed from items_after_filter and must match the summed items_after_filter
- HTTP validation must return structured errors on empty URL, network failure, non-2xx status, empty body, or JSON decode failure
- Heredoc stdin parsing can cause JSON to be treated as code, avoid that pattern to prevent SyntaxError

Progress Log
- EU 2026-01-17 latest ingested_at validation
  - probe_order_items_raw_v1 latest ingested_at 2026-01-23 04:55:17 row_count 57 distinct_key_count 57
  - probe_sales_asin_daily_v1 latest ingested_at 2026-01-23 04:55:17 row_count 33 distinct_key_count 33
  - Duplicate-key checks PASS with duplicate_key_rows 0 for both tables
- verify-items behavior confirmed with non-zero orders_count, units_sold, items_rows_count, and asin_stats_count

Next Steps
- Merge fixes to main and validate with primary traffic
- Extend validation to other scopes and snapshot_date values
- Validate inventory ingestion and reserved_effective calculations
