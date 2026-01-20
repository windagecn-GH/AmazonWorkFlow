# Scripts overview

This folder contains helper SQL files for BigQuery schema alignment and validation.

Before running, confirm your gcloud project is set and you have access to BigQuery:
gcloud config get-value project
bq ls

BigQuery migration:
bq query --use_legacy_sql=false < scripts/bq_migrations.sql

BigQuery validation:
bq query --use_legacy_sql=false < scripts/bq_validation.sql
