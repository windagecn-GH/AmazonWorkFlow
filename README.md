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

Deployment Prerequisites
Required Cloud Run environment variables and secrets
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- AWS_REGION
- AWS_SESSION_TOKEN
- LWA_CLIENT_ID
- LWA_CLIENT_SECRET
- LWA_REFRESH_TOKEN_EU
- LWA_REFRESH_TOKEN_NA

Deployment and verification
```bash
set -euo pipefail
cd /PATH/TO/AmazonWorkFlow

PROJECT_ID=<PUT_YOUR_PROJECT_ID>
REGION=<PUT_YOUR_REGION>
SERVICE_NAME=<PUT_YOUR_SERVICE_NAME>
SCOPE=EU
SNAPSHOT_DATE=2026-01-17

gcloud run services update "$SERVICE_NAME" \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --set-env-vars AWS_ACCESS_KEY_ID=<PUT_YOUR_AWS_ACCESS_KEY_ID>,AWS_SECRET_ACCESS_KEY=<PUT_YOUR_AWS_SECRET_ACCESS_KEY>,AWS_REGION=<PUT_YOUR_AWS_REGION>,AWS_SESSION_TOKEN=<PUT_YOUR_AWS_SESSION_TOKEN>,LWA_CLIENT_ID=<PUT_YOUR_LWA_CLIENT_ID>,LWA_CLIENT_SECRET=<PUT_YOUR_LWA_CLIENT_SECRET> \
  --set-secrets LWA_REFRESH_TOKEN_EU=<PUT_YOUR_LWA_REFRESH_TOKEN_EU>,LWA_REFRESH_TOKEN_NA=<PUT_YOUR_LWA_REFRESH_TOKEN_NA>

SERVICE_URL=$(gcloud run services describe "$SERVICE_NAME" --project "$PROJECT_ID" --region "$REGION" --format 'value(status.url)')
REVISION=$(gcloud run revisions list --project "$PROJECT_ID" --region "$REGION" --service "$SERVICE_NAME" --format 'value(metadata.name)' --limit 1)
REVISION_URL=$(gcloud run revisions describe "$REVISION" --project "$PROJECT_ID" --region "$REGION" --format 'value(status.url)')

export AUTH_TOKEN=$(gcloud auth print-identity-token)
python3 scripts/verify_endpoint.py --url "$REVISION_URL" --path /cron/daily --scope "$SCOPE" --snapshot-date "$SNAPSHOT_DATE" --dry 0 --debug-items 1 --compact 0

RUN_ID=$(python3 scripts/verify_endpoint.py --url "$SERVICE_URL" --path /cron/daily --scope "$SCOPE" --snapshot-date "$SNAPSHOT_DATE" --dry 0 --debug-items 1 --compact 0 | python3 -c 'import json,sys;print(json.load(sys.stdin).get("response_run_id",""))')
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=$SERVICE_NAME AND jsonPayload.run_id=$RUN_ID" --project "$PROJECT_ID" --limit 50

bq query --use_legacy_sql=false < scripts/bq_views.sql
bq query --use_legacy_sql=false < scripts/bq_validation.sql
```

End-to-End Verification
Use the following sequence to validate end to end behavior for a specific scope and snapshot date.
You can change the snapshot date as needed.

Call Cloud Run and capture data
```
export AUTH_TOKEN=$(gcloud auth print-identity-token)
python3 scripts/verify_endpoint.py \
  --url https://YOUR_SERVICE_URL \
  --path /cron/daily \
  --scope EU \
  --snapshot-date 2026-01-17 \
  --dry 0 \
  --debug-items 1 \
  --compact 0
```

Create or update BigQuery views
```
bq query --use_legacy_sql=false < scripts/bq_views.sql
```

Run BigQuery validation
```
bq query --use_legacy_sql=false < scripts/bq_validation.sql
```

Validation parameters
Edit the top of scripts/bq_validation.sql to adjust p_scope and p_snapshot_date to match your run.
