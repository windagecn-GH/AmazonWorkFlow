from __future__ import annotations

import json
import uuid
import time
import logging
from datetime import datetime, date
from typing import Any, Dict, List, Optional

from google.cloud import bigquery

from .config import (
    bq_inv_fba_asin_table_id,
    bq_inv_awd_asin_table_id,
    marketplaces_for_scope,
    tz_for_scope,
)
from .spapi_core import spapi_request_json, SpapiRequestError

logger = logging.getLogger("spapi_inventory")

# Inventory Pools Configuration
# Maps internal pool ID to the specific Marketplace ID used to query it
INV_POOL_MAP = {
    # DE Pool supplies EU (except UK)
    "DE": "A1PA6795UKMFR9", 
    # UK Pool supplies UK
    "UK": "A1F83G8C2ARO7P",
    # US Pool supplies NA
    "US": "ATVPDKIKX0DER",
}

def _retry_spapi(fn, *, stage: str, run_id: str, max_tries: int = 4, base_sleep: float = 1.0):
    last_exc: Optional[Exception] = None
    for i in range(max_tries):
        try:
            resp = fn()
            if isinstance(resp, dict) and not resp.get("ok", False):
                status = int(resp.get("status") or 0)
                err = SpapiRequestError(
                    message=resp.get("error") or "SP-API request failed",
                    status=status,
                    stage=stage,
                    run_id=run_id,
                    debug=resp.get("debug") or {},
                )
                if status in (429, 503, 504):
                    last_exc = err
                    time.sleep(base_sleep * (2 ** i))
                    continue
                raise err
            return resp
        except SpapiRequestError as e:
            last_exc = e
            if e.status in (429, 503, 504) and i < max_tries - 1:
                time.sleep(base_sleep * (2 ** i))
                continue
            raise
        except Exception as e:
            last_exc = e
            raise
    if last_exc:
        raise last_exc
    raise RuntimeError("SP-API retry exhausted")

def fetch_fba_inventory(scope: str, run_id: str) -> List[Dict[str, Any]]:
    """
    Fetches FBA inventory summaries.
    Determines which Pools to fetch based on Scope.
    EU Scope -> Fetches DE and UK pools.
    NA Scope -> Fetches US pool.
    """
    rows = []
    
    pools_to_fetch = []
    if scope == "EU":
        pools_to_fetch = ["DE", "UK"]
    elif scope == "UK":
        pools_to_fetch = ["UK"]
    elif scope == "NA":
        pools_to_fetch = ["US"]
    
    logger.info(json.dumps({"event": "fetch_fba_start", "scope": scope, "pools": pools_to_fetch, "run_id": run_id}))

    for pool in pools_to_fetch:
        mp_id = INV_POOL_MAP.get(pool)
        if not mp_id:
            continue
            
        # Call FBA Inventory API
        # GET /fba/inventory/v1/summaries
        query = {
            "details": "true",
            "granularityType": "Marketplace",
            "granularityId": mp_id,
            "marketplaceIds": mp_id
        }
        
        # Determine API region from scope (EU/NA)
        api_scope = "EU" if pool in ("DE", "UK") else "NA"
        
        next_token = None
        page_count = 0
        
        while True:
            q = query.copy()
            if next_token:
                q["nextToken"] = next_token
                
            def _call():
                return spapi_request_json(
                    scope=api_scope,
                    method="GET",
                    path="/fba/inventory/v1/summaries",
                    query=q
                )
            
            try:
                resp = _retry_spapi(_call, stage="fba_summary", run_id=run_id)
                payload = resp.get("payload") or {}
                summaries = payload.get("inventorySummaries") or []
                
                for item in summaries:
                    asin = item.get("asin")
                    details = item.get("inventoryDetails") or {}
                    
                    reserved = details.get("reservedQuantity") or {}
                    
                    qty_total_reserved = int(reserved.get("totalReservedQuantity") or 0)
                    qty_reserved_cust = int(reserved.get("pendingCustomerOrderQuantity") or 0)
                    
                    # Effective reserved = Total - CustomerOrders
                    # (Logic: items reserved for orders are effectively sold, items reserved for transfer/processing are internal stock)
                    qty_reserved_eff = max(0, qty_total_reserved - qty_reserved_cust)
                    
                    qty_avail = int(details.get("fulfillableQuantity") or 0)
                    
                    inbound = details.get("inboundQuantity") or {} # Note: API structure might vary slightly, checking generic
                    # Actually API usually returns inboundWorkingQuantity, inboundShippedQuantity etc at top level of inventoryDetails?
                    # Let's check typical structure:
                    # inventoryDetails: { fulfillableQuantity, inboundWorkingQuantity, inboundShippedQuantity, inboundReceivingQuantity ... }
                    
                    qty_inbound = (
                        int(details.get("inboundWorkingQuantity") or 0) +
                        int(details.get("inboundShippedQuantity") or 0) +
                        int(details.get("inboundReceivingQuantity") or 0)
                    )

                    rows.append({
                        "inv_pool": pool,
                        "asin": asin,
                        "marketplace_id": mp_id,
                        "qty_available": qty_avail,
                        "qty_inbound": qty_inbound,
                        "qty_reserved_total": qty_total_reserved,
                        "qty_reserved_customer_orders": qty_reserved_cust,
                        "qty_reserved_effective": qty_reserved_eff,
                        "raw_json_str": json.dumps(item)
                    })
                
                next_token = payload.get("nextToken")
                page_count += 1
                if not next_token:
                    break
                time.sleep(0.1) # Pace
                
            except SpapiRequestError:
                raise
            except Exception as e:
                logger.error(json.dumps({"event": "fba_fetch_error", "pool": pool, "error": str(e), "run_id": run_id}))
                raise
                
    return rows

def fetch_awd_inventory(scope: str, run_id: str) -> List[Dict[str, Any]]:
    """
    Fetches AWD inventory (US only).
    """
    if scope != "NA":
        return []
        
    rows = []
    logger.info(json.dumps({"event": "fetch_awd_start", "run_id": run_id}))
    
    # AWD endpoint: /awd/2024-05-09/inventory
    query = {
        "details": "SHOW", # Often required to get breakdown
        "maxResults": "100"
    }
    
    next_token = None
    
    while True:
        q = query.copy()
        if next_token:
            q["nextToken"] = next_token
            
        def _call():
            return spapi_request_json(
                scope="NA",
                method="GET",
                path="/awd/2024-05-09/inventory",
                query=q
            )
            
        try:
            resp = _retry_spapi(_call, stage="awd_summary", run_id=run_id)
            payload = resp.get("payload") or {}
            listings = payload.get("listingInventory") or []
            
            for item in listings:
                asin = item.get("asin")
                # Structure: { asin, sku, totalQuantity, availableQuantity, ... }
                # Also check 'inventoryDetails' if present
                
                qty_avail = int(item.get("availableQuantity") or 0)
                qty_total = int(item.get("totalQuantity") or 0)
                # Inbound is not always explicit in AWD list response, might be derived
                # For now map available.
                
                rows.append({
                    "inv_pool": "US_AWD",
                    "asin": asin,
                    "qty_available": qty_avail,
                    "qty_inbound": 0, # Placeholder if not in resp
                    "raw_json_str": json.dumps(item)
                })
                
            next_token = payload.get("nextToken")
            if not next_token:
                break
            time.sleep(0.1)
            
        except SpapiRequestError:
            raise
        except Exception as e:
            # AWD might not be active or authorized, log and skip
            logger.error(json.dumps({"event": "awd_fetch_error", "error": str(e), "run_id": run_id}))
            raise

    return rows

def write_inventory_bq(
    run_id: str,
    snapshot_date: date,
    fba_rows: List[Dict],
    awd_rows: List[Dict],
    dry: bool
):
    if dry:
        return {"dry": True}
        
    client = bigquery.Client()
    ingested_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    results = {}
    
    # 1. FBA
    fba_bq = []
    for r in fba_rows:
        fba_bq.append({
            "run_id": run_id,
            "ingested_at": ingested_at,
            "snapshot_date": str(snapshot_date),
            "inv_pool": r["inv_pool"],
            "asin": r["asin"],
            "marketplace_id": r["marketplace_id"],
            "qty_available": r["qty_available"],
            "qty_inbound": r["qty_inbound"],
            "qty_reserved_total": r["qty_reserved_total"],
            "qty_reserved_customer_orders": r["qty_reserved_customer_orders"],
            "qty_reserved_effective": r["qty_reserved_effective"],
            "raw_json_str": r["raw_json_str"]
        })
    
    if fba_bq:
        errors = client.insert_rows_json(bq_inv_fba_asin_table_id(), fba_bq)
        results["fba"] = {"inserted": len(fba_bq), "errors": errors}
        
    # 2. AWD
    awd_bq = []
    for r in awd_rows:
        awd_bq.append({
            "run_id": run_id,
            "ingested_at": ingested_at,
            "snapshot_date": str(snapshot_date),
            "inv_pool": r["inv_pool"],
            "asin": r["asin"],
            "qty_available": r["qty_available"],
            "qty_inbound": r["qty_inbound"],
            "raw_json_str": r["raw_json_str"]
        })

    if awd_bq:
        errors = client.insert_rows_json(bq_inv_awd_asin_table_id(), awd_bq)
        results["awd"] = {"inserted": len(awd_bq), "errors": errors}
        
    return results

def run_inventory(
    scope: str,
    *,
    dry: bool = True
) -> Dict[str, Any]:
    run_id = str(uuid.uuid4())
    snapshot_date = datetime.utcnow().date() # Inventory is "Snapshot of Now"
    
    logger.info(json.dumps({"event": "run_inventory_start", "run_id": run_id, "scope": scope}))
    
    fba_rows = fetch_fba_inventory(scope, run_id)
    awd_rows = fetch_awd_inventory(scope, run_id)
    
    bq_res = write_inventory_bq(run_id, snapshot_date, fba_rows, awd_rows, dry)
    
    return {
        "run_id": run_id,
        "scope": scope,
        "ok": True,
        "fba_rows_count": len(fba_rows),
        "awd_rows_count": len(awd_rows),
        "bq": bq_res
    }
