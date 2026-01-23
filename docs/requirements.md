# Background
This project builds a reliable data foundation for Amazon operations by collecting orders, items, and sales aggregates into BigQuery.

# Goal
Automate Amazon data ingestion into BigQuery to support inventory management analysis and reporting.

# Scope
Module one includes data acquisition and storage.
Coverage is by scope and snapshot_date, with EU as a required baseline.
Country detail is derived from marketplace and is split by marketplace_id.

# Non-goals
No dashboarding, visualization, or alerting system is included in scope.
No deployment platform or runtime workflow is mandated here.

# Data Definitions
EU daily sales and order summary definitions are authoritative in this section.
Country detail view uses amazon_ops.v_probe_sales_daily_latest_country.
Exclude country EU and marketplace_id __ALL__ from country detail.
Select latest ingested_at by snapshot_date, scope, country, and marketplace_id.
Total view uses amazon_ops.v_probe_sales_daily_latest_all.
Include country EU and marketplace_id __ALL__ for total rows.
Select latest ingested_at by snapshot_date and scope.

# Inputs and Outputs
Inputs include scope and snapshot_date.
Outputs include order summary, order detail, and ASIN daily aggregates available in BigQuery.

# Acceptance Criteria
Idempotency requires latest ingested_at results to be reproducible for the same scope and snapshot_date.
Deduplication requires duplicate keys to be zero in raw items and ASIN daily for latest ingested_at.
Observability requires failure when orders_count is greater than zero but items_rows_count is zero, including status, error, run_id, and stage fields.
Debugging requires per-country item fetch statistics when debug is enabled, with clear field semantics.

# Single Source of Truth
docs/requirements.md is authoritative for requirements and acceptance criteria.
README.md is authoritative for how to run and validate in a human readable way.
scripts/bq_validation.sql is authoritative for BigQuery validation logic.
Any other document should only point to these sources without duplicating rules.
