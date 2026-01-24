# Project overview

This project ingests Amazon orders and items, aggregates ASIN daily sales, and validates idempotent ingestion in BigQuery.

Requirements pointer
- Requirements and acceptance criteria live in docs/requirements.md
- Validation SQL lives in scripts/bq_validation.sql
- Scripts overview and usage notes live in scripts/README.md
- Endpoint verification entrypoint lives in scripts/verify_endpoint.py and acceptance criteria live in docs/requirements.md

BigQuery Views Single Source of Truth
- v_probe_sales_daily_latest_country provides country detail rows with country field derived from country_code
- v_probe_sales_daily_latest_all provides the EU total row with country EU and marketplace_id __ALL__
- View definitions live in scripts/bq_views.sql

Validation
- Create or update BigQuery views using scripts/bq_views.sql before running validation
- Validate ingested data and view correctness using scripts/bq_validation.sql
