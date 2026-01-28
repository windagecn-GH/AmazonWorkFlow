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
- LWA_CLIENT_ID
- LWA_CLIENT_SECRET
- LWA_REFRESH_TOKEN_EU
- LWA_REFRESH_TOKEN_NA
Optional environment variables
- AWS_SESSION_TOKEN

Secret auto-detection on macOS
macOS BSD grep does not support some extended or PCRE patterns and may fail with repetition operator errors. Use scripts/detect_spapi_secrets.py to discover secret names in a portable way

End-to-End Verification (copy/paste)
Use the following sequence to validate deployment, data ingestion, and BigQuery validation for a specific scope and snapshot date.
You can change the snapshot date as needed.

```bash
set -euo pipefail
cd /Users/melvin/work/AmazonWorkFlow

PROJECT_ID=<PUT_YOUR_PROJECT_ID>
REGION=<PUT_YOUR_REGION>
SERVICE_NAME=<PUT_YOUR_SERVICE_NAME>
SCOPE=EU
SNAPSHOT_DATE=2026-01-17
AWS_ACCESS_KEY_ID_SECRET=<PUT_YOUR_AWS_ACCESS_KEY_ID_SECRET>
AWS_SECRET_ACCESS_KEY_SECRET=<PUT_YOUR_AWS_SECRET_ACCESS_KEY_SECRET>

{
  read -r LWA_CLIENT_ID_SECRET
  read -r LWA_CLIENT_SECRET_SECRET
  read -r LWA_REFRESH_SECRET
} < <(python3 scripts/detect_spapi_secrets.py --scope "$SCOPE")

if [ -z "$LWA_CLIENT_ID_SECRET" ] || [ -z "$LWA_CLIENT_SECRET_SECRET" ] || [ -z "$LWA_REFRESH_SECRET" ]; then
  echo "Missing required secret names"
  exit 1
fi

if [ "$SCOPE" = "EU" ]; then
  REFRESH_ENV=LWA_REFRESH_TOKEN_EU
elif [ "$SCOPE" = "NA" ]; then
  REFRESH_ENV=LWA_REFRESH_TOKEN_NA
else
  REFRESH_ENV=LWA_REFRESH_TOKEN
fi

gcloud run services update "$SERVICE_NAME" \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --update-secrets "LWA_CLIENT_ID=$LWA_CLIENT_ID_SECRET:latest,LWA_CLIENT_SECRET=$LWA_CLIENT_SECRET_SECRET:latest,$REFRESH_ENV=$LWA_REFRESH_SECRET:latest,AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID_SECRET:latest,AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY_SECRET:latest" \
  --set-env-vars "AWS_REGION=<PUT_YOUR_AWS_REGION>,BQ_PROJECT=$PROJECT_ID"

SERVICE_URL=$(gcloud run services describe "$SERVICE_NAME" --project "$PROJECT_ID" --region "$REGION" --format 'value(status.url)')
export AUTH_TOKEN=$(gcloud auth print-identity-token)
VERIFY_OUT=$(python3 scripts/verify_endpoint.py --url "$SERVICE_URL" --path /cron/daily --scope "$SCOPE" --snapshot-date "$SNAPSHOT_DATE" --dry 0 --debug-items 1 --compact 0)
echo "$VERIFY_OUT"
RUN_ID=$(python3 -c 'import json,sys;print(json.loads(sys.stdin.read()).get("response_run_id",""))' <<<"$VERIFY_OUT")
gcloud logging read 'resource.type="cloud_run_revision" AND resource.labels.service_name="'"$SERVICE_NAME"'" AND jsonPayload.run_id="'"$RUN_ID"'"' --project "$PROJECT_ID" --limit 50

bq query --use_legacy_sql=false < scripts/bq_views.sql
bq query --use_legacy_sql=false < scripts/bq_validation.sql
```

Validation parameters
Edit the top of scripts/bq_validation.sql to adjust p_scope and p_snapshot_date to match your run.
