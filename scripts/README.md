# Scripts overview

This folder contains helper SQL files for BigQuery schema alignment and validation.

BigQuery migration:
- Open `scripts/bq_migrations.sql` in the BigQuery console (or your SQL runner).
- Adjust the dataset/project names if they differ from your environment.
- Run the statements to align schema changes.

BigQuery validation:
- Open `scripts/bq_validation.sql` in the BigQuery console (or your SQL runner).
- Set `p_scope` and `p_snapshot_date` to the run you want to validate.
- Review the latest-ingested counts and distinct key counts for raw items and ASIN daily.
