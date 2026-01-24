# Scripts overview

Purpose
This folder contains support scripts and SQL used for validation and tooling.

Where to look
- Requirements and acceptance criteria live in `docs/requirements.md`
- Run and validation instructions live in `README.md`
- BigQuery view definitions live in `scripts/bq_views.sql`
- BigQuery validation logic lives in `scripts/bq_validation.sql`
- Endpoint verification helper lives in `scripts/verify_endpoint.py`

Notes
- `scripts/bq_validation.sql` is the single source of truth for BigQuery validation queries.
- Use `scripts/README.md` only as a pointer to authoritative sources.
- Baseline reference commit: `e9cf983928da8614d0844f8e9323efaf5b81f71c`.
