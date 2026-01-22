# Project overview

This project ingests Amazon orders and items, aggregates ASIN daily sales, and validates idempotent ingestion in BigQuery. The authoritative, up to date documentation lives in `scripts/README.md`.

Acceptance summary
- When orders_count is greater than zero, items_rows_count and asin_stats_count must not be zero
- Failure responses must return ok false with status, error, run_id, and stage
- Debug output must include order_items_by_country when debugItems is true or compact is false
- latest ingested_at validation must show row_count equals distinct_key_count and duplicate-key checks must return no rows
