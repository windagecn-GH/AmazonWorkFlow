from __future__ import annotations

import uuid
import logging
import os
import sys
import json
from datetime import date as date_type
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

from .config import require_env, tz_for_scope
from .utils_env import get_missing_required_envs
from .spapi_core import SpapiRequestError
from .utils_time import yesterday_local
from .orders_agg import run_daily, fetch_orders_for_scope
from .inventory_probe import run_inventory

# Configure startup logging
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger("spapi_main")
logger.info("Service Initializing...")

app = FastAPI()

LOCAL_BLOCKED_STATUS = "LOCAL_EXEC_BLOCKED"
DRY_RUN_STATUS = "DRY_RUN"

def _is_cloud_run() -> bool:
    return bool((os.getenv("K_SERVICE") or "").strip())

def _local_block_response(run_id: str, stage: str) -> JSONResponse:
    return JSONResponse(
        {
            "ok": False,
            "status": LOCAL_BLOCKED_STATUS,
            "error": "Local execution blocked. Use Cloud Run for SP-API calls.",
            "run_id": run_id,
            "stage": stage,
        },
        status_code=200,
    )

def _dry_run_response(payload: dict) -> JSONResponse:
    payload["ok"] = True
    payload["status"] = DRY_RUN_STATUS
    payload["stage"] = "dry_run"
    return JSONResponse(payload, status_code=200)

@app.on_event("startup")
async def startup_event():
    logger.info("Service Startup Complete")

@app.get("/debug/import_health")
def import_health():
    ok, checks = require_env()
    return {"ok": ok, "env": checks}

@app.get("/cron/daily")
def cron_daily(
    scope: str = Query(..., description="EU | UK | NA"),
    snapshot_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    dry: int = Query(1, description="1=dry run (no BQ write)"),
    debugItems: int = Query(0, description="1=embed per-order debug into raw_json_str"),
    compact: int = Query(1, description="1=compact response"),
    filterMode: str = Query("Created", description="Created or LastUpdated"),
    maxPages: int = Query(50),
    pageSize: int = Query(100),
    maxOrders: int = Query(5000),
):
    scope_u = scope.upper()
    tz = tz_for_scope(scope_u)
    run_id = str(uuid.uuid4())

    if snapshot_date:
        y = int(snapshot_date[0:4])
        m = int(snapshot_date[5:7])
        d = int(snapshot_date[8:10])
        snap = date_type(y, m, d)
    else:
        snap = yesterday_local(tz)

    dry_int = int(dry)
    if dry_int != 1 and not _is_cloud_run():
        return _local_block_response(run_id, stage="orders_list")

    if dry_int == 1 and not _is_cloud_run():
        return _dry_run_response(
            {
                "run_id": run_id,
                "scope": scope_u,
                "snapshot_date": str(snap),
                "dry": True,
                "steps": [
                    "Fetch orders list (paginated)",
                    "Fetch order items for each order",
                    "Aggregate orders/items/ASIN stats",
                    "Write results to BigQuery (skipped in dry run)",
                ],
                "params": {
                    "filter_mode": filterMode,
                    "max_pages": int(maxPages),
                    "page_size": int(pageSize),
                    "max_orders": int(maxOrders),
                },
            }
        )
    # On Cloud Run, allow dry=1 to execute and compute aggregates (no BQ writes).

    missing_envs = get_missing_required_envs()
    if missing_envs:
        logger.error(json.dumps({
            "event": "cron_daily_env_missing",
            "run_id": run_id,
            "scope": scope_u,
            "snapshot_date": str(snap),
            "missing_envs": missing_envs,
        }))
        sys.stdout.flush()
        sys.stderr.flush()
        return JSONResponse(
            {
                "ok": False,
                "status": "ENV_MISSING",
                "stage": "bootstrap",
                "error": f"Missing required env var(s): {', '.join(missing_envs)}",
                "run_id": run_id,
                "scope": scope_u,
                "snapshot_date": str(snap),
                "missing_envs": missing_envs,
            },
            status_code=200,
        )

    try:
        out = run_daily(
            scope=scope_u,
            snapshot_date=snap,
            dry=bool(dry_int),
            debug_items=bool(int(debugItems)),
            compact=bool(int(compact)),
            filter_mode=filterMode,
            max_pages=int(maxPages),
            page_size=int(pageSize),
            max_orders=int(maxOrders),
        )
        return JSONResponse(out)
    except SpapiRequestError as e:
        payload = e.to_dict()
        payload.update({"scope": scope_u, "snapshot_date": str(snap)})
        logger.exception(json.dumps({
            "event": "cron_daily_spapi_exception",
            "scope": scope_u,
            "snapshot_date": str(snap),
            "run_id": e.run_id,
            "stage": e.stage,
            "status": e.status,
            "error": e.message,
            "debug": e.debug,
        }))
        sys.stdout.flush()
        sys.stderr.flush()
        return JSONResponse(payload, status_code=200)
    except Exception as e:
        import traceback
        logger.exception(json.dumps({
            "event": "cron_daily_unhandled_exception",
            "scope": scope_u,
            "snapshot_date": str(snap),
            "run_id": run_id,
            "exc_type": type(e).__name__,
            "exc": repr(e),
        }))
        include_trace = bool(int(debugItems)) or (int(compact) == 0)
        payload = {
            "ok": False,
            "response_stage": "unhandled_exception",
            "response_status": 0,
            "response_run_id": run_id,
            "error": {"type": type(e).__name__, "message": str(e)},
        }
        if include_trace:
            payload["trace"] = traceback.format_exc()
        sys.stdout.flush()
        sys.stderr.flush()
        return JSONResponse(
            payload,
            status_code=200,
        )

@app.get("/cron/inventory")
def cron_inventory(
    scope: str = Query(..., description="EU | UK | NA"),
    dry: int = Query(1, description="1=dry run")
):
    """
    Fetches current inventory (FBA + AWD) for the given scope.
    Snapshot date is UTC Today.
    """
    run_id = str(uuid.uuid4())
    dry_int = int(dry)

    if dry_int != 1 and not _is_cloud_run():
        return _local_block_response(run_id, stage="fba_summary")

    if dry_int == 1:
        return _dry_run_response(
            {
                "run_id": run_id,
                "scope": scope.upper(),
                "dry": True,
                "steps": [
                    "Fetch FBA inventory summaries",
                    "Fetch AWD inventory (NA only)",
                    "Write inventory snapshots to BigQuery (skipped in dry run)",
                ],
            }
        )

    try:
        out = run_inventory(scope=scope.upper(), dry=bool(dry_int))
        return JSONResponse(out)
    except SpapiRequestError as e:
        payload = e.to_dict()
        payload.update({"scope": scope.upper()})
        return JSONResponse(payload, status_code=200)
    except Exception as e:
        import traceback
        return JSONResponse(
            {
                "ok": False,
                "status": 0,
                "stage": "error",
                "scope": scope,
                "error": str(e),
                "run_id": "unknown",
                "trace": traceback.format_exc()
            },
            status_code=200
        )

@app.get("/debug/spapi_orders_probe")
def debug_spapi_orders_probe(
    scope: str = Query(..., description="EU | NA"),
    createdAfter: Optional[str] = Query(None, description="ISO Date string override e.g. 2024-05-01T00:00:00Z"),
    maxPages: int = 1,
    pageSize: int = 10
):
    """
    Read-only probe to check if SP-API returns orders.
    Does NOT write to BigQuery.
    """
    scope_u = scope.upper()
    run_id = f"debug-{uuid.uuid4()}"
    
    # We use fetch_orders_for_scope directly
    # Need a dummy snapshot date if not used, but fetch_orders_for_scope uses it to build window
    # if createdAfter is not provided.
    dummy_date = date_type.today()
    
    try:
        orders, debug_info = fetch_orders_for_scope(
            scope=scope_u,
            snapshot_date=dummy_date,
            max_pages=maxPages,
            page_size=pageSize,
            run_id=run_id,
            custom_created_after=createdAfter
        )
        
        # Serialize a few orders to see raw data
        sample_orders = []
        for o in orders[:5]:
            sample_orders.append({
                "AmazonOrderId": o.amazon_order_id,
                "Status": o.order_status,
                "Raw": o.raw
            })
            
        return {
            "ok": True,
            "run_id": run_id,
            "orders_found": len(orders),
            "debug_info": debug_info,
            "first_5_samples": sample_orders
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}
