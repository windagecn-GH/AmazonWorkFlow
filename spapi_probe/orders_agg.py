from __future__ import annotations

import json
import time
import uuid
import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from google.cloud import bigquery

from .config import (
    bq_orders_raw_table_id,
    bq_orders_agg_table_id,
    bq_order_items_raw_table_id,
    bq_sales_asin_daily_table_id,
    marketplace_ids_for_scope,
    marketplaces_for_scope,
    country_for_marketplace_id,
    tz_for_scope,
)
from .spapi_core import spapi_request_json, SpapiRequestError
from .utils_time import day_window_utc

# Configure structured logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("spapi_orders")

@dataclass
class OrderLite:
    amazon_order_id: str
    marketplace_id: str
    order_status: str
    sales_channel: str
    raw: Dict[str, Any]

def _retry_spapi(fn, *, stage: str, run_id: str, max_tries: int = 6, base_sleep: float = 0.8):
    """Retry SP-API calls on 429/503/504 with exponential backoff."""
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

def _truncate_text(value: Any, max_len: int) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        text = json.dumps(value, ensure_ascii=True)
    else:
        text = str(value)
    if len(text) > max_len:
        return text[:max_len]
    return text

def _extract_item_units(item: Dict[str, Any]) -> int:
    q = item.get("QuantityOrdered") or 0
    qc = item.get("QuantityCancelled") or 0
    try:
        q = int(q)
    except Exception:
        q = 0
    try:
        qc = int(qc)
    except Exception:
        qc = 0
    return max(0, q - qc)

def fetch_orders_for_scope(
    scope: str,
    snapshot_date: date,
    *,
    filter_mode: str = "Created",
    page_size: int = 100,
    max_pages: int = 50,
    max_orders: int = 5000,
    run_id: str = "debug",
    custom_created_after: Optional[str] = None,
    custom_created_before: Optional[str] = None,
    include_debug: bool = False,
    compact: bool = True,
) -> Tuple[List[OrderLite], Dict[str, Any]]:
    tz = tz_for_scope(scope)
    dt_start_utc, dt_end_utc = day_window_utc(tz, snapshot_date)
    
    # Allow override for debugging
    if custom_created_after:
        dt_start_utc = custom_created_after
    if custom_created_before:
        dt_end_utc = custom_created_before

    marketplace_ids = marketplace_ids_for_scope(scope)

    orders: List[OrderLite] = []
    pages = 0
    next_token: Optional[str] = None

    # Build query params
    def build_params(marketplace_ids_value: str, *, include_next_token: bool) -> Dict[str, Any]:
        p: Dict[str, Any] = {"MarketplaceIds": marketplace_ids_value, "PageSize": page_size}
        if include_next_token and next_token:
            p["NextToken"] = next_token
        else:
            # Using CreatedAfter/Before by default; caller can switch to LastUpdated
            if filter_mode.lower().startswith("last"):
                p["LastUpdatedAfter"] = dt_start_utc
                p["LastUpdatedBefore"] = dt_end_utc
            else:
                p["CreatedAfter"] = dt_start_utc
                p["CreatedBefore"] = dt_end_utc
        # Explicitly ask for statuses if needed, but default returns most useful ones except Pending sometimes
        # To be safe for "sales", we usually want everything that isn't Canceled, but raw orders should keep Canceled.
        # We fetch all by default (API default).
        return p

    debug = {
        "run_id": run_id,
        "timezone": tz,
        "dt_start_utc": dt_start_utc,
        "dt_end_utc_raw": dt_end_utc,
    }
    mp_map = marketplaces_for_scope(scope)

    logger.info(json.dumps({"event": "fetch_orders_start", "scope": scope, "params": debug}))

    while True:
        if pages >= max_pages or len(orders) >= max_orders:
            break

        params = build_params(",".join(marketplace_ids), include_next_token=True)

        def _call():
            return spapi_request_json(
                scope="EU" if scope.upper() in ("EU", "UK") else scope.upper(),
                method="GET",
                path="/orders/v0/orders",
                query=params,
            )

        resp = _retry_spapi(_call, stage="orders_list", run_id=run_id)
        if include_debug:
            resp_debug = resp.get("debug") or {}
            request_id = resp_debug.get("request_id") or resp_debug.get("rid")
            query_text = _truncate_text(params, 1000)
            logger.info(
                "event=list_orders_debug;run_id=%s;status_code=%s;request_id=%s;query=%s",
                run_id,
                resp.get("status"),
                request_id,
                query_text,
            )
            body_value = resp.get("payload")
            body_text = _truncate_text(body_value, 2000) if compact else _truncate_text(body_value, 200000)
            list_orders_debug = {
                "status_code": resp.get("status"),
                "request_id": request_id,
                "rid": resp_debug.get("rid"),
                "path": "/orders/v0/orders",
                "query": params,
                "query_keys": {
                    "CreatedAfter": params.get("CreatedAfter"),
                    "CreatedBefore": params.get("CreatedBefore"),
                    "MarketplaceIds": params.get("MarketplaceIds"),
                    "OrderStatuses": params.get("OrderStatuses"),
                },
                "error": resp.get("error"),
                "body": body_text,
            }
            debug["list_orders"] = list_orders_debug
            debug.setdefault("list_orders_by_country", {})
            if pages == 0:
                for cc, mid in mp_map.items():
                    country_params = build_params(mid, include_next_token=False)

                    def _call_country():
                        return spapi_request_json(
                            scope="EU" if scope.upper() in ("EU", "UK") else scope.upper(),
                            method="GET",
                            path="/orders/v0/orders",
                            query=country_params,
                        )

                    country_resp = _call_country()
                    country_debug = country_resp.get("debug") or {}
                    country_request_id = country_debug.get("request_id") or country_debug.get("rid")
                    country_body_value = country_resp.get("payload")
                    country_body_text = (
                        _truncate_text(country_body_value, 2000)
                        if compact
                        else _truncate_text(country_body_value, 200000)
                    )
                    logger.info(
                        "event=list_orders_debug;run_id=%s;status_code=%s;request_id=%s;query=%s",
                        run_id,
                        country_resp.get("status"),
                        country_request_id,
                        _truncate_text(country_params, 1000),
                    )
                    debug["list_orders_by_country"][cc] = {
                        "status_code": country_resp.get("status"),
                        "request_id": country_request_id,
                        "rid": country_debug.get("rid"),
                        "path": "/orders/v0/orders",
                        "query": country_params,
                        "query_keys": {
                            "CreatedAfter": country_params.get("CreatedAfter"),
                            "CreatedBefore": country_params.get("CreatedBefore"),
                            "MarketplaceIds": country_params.get("MarketplaceIds"),
                            "OrderStatuses": country_params.get("OrderStatuses"),
                        },
                        "error": country_resp.get("error"),
                        "body": country_body_text,
                        "country": cc,
                        "marketplace_id": mid,
                    }
        payload = resp.get("payload") or {}
        if isinstance(payload, dict) and isinstance(payload.get("payload"), dict):
            payload = payload.get("payload") or {}
        fetched_batch = payload.get("Orders") or []
        
        for o in fetched_batch:
            aoid = o.get("AmazonOrderId")
            mid = o.get("MarketplaceId")
            if not aoid or not mid:
                continue
            orders.append(
                OrderLite(
                    amazon_order_id=aoid,
                    marketplace_id=mid,
                    order_status=str(o.get("OrderStatus") or ""),
                    sales_channel=str(o.get("SalesChannel") or ""),
                    raw=o,
                )
            )
            if len(orders) >= max_orders:
                break

        pages += 1
        next_token = payload.get("NextToken")
        
        logger.info(json.dumps({
            "event": "fetch_page",
            "run_id": run_id,
            "page": pages, 
            "orders_in_batch": len(fetched_batch),
            "total_orders": len(orders),
            "has_next_token": bool(next_token)
        }))

        if not next_token:
            break

        # Gentle pacing to avoid throttles when looping
        time.sleep(0.15)

    debug["pages_fetched"] = pages
    debug["orders_fetched"] = len(orders)
    return orders, debug

def process_orders_and_items(
    scope: str,
    orders: List[OrderLite],
    *,
    debug_items: bool = False,
    run_id: str = "debug",
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Returns:
      totals: summary stats
      raw_orders_rows: BQ rows for probe_orders_raw
      raw_items_rows: BQ rows for probe_order_items_raw
      asin_daily_rows: BQ rows for probe_sales_asin_daily
    """
    mp_map = marketplaces_for_scope(scope)

    totals = {
        "orders_count": 0,
        "units_sold": 0,
        "canceled_orders": 0,
        "excluded_non_amazon_orders": 0,
        "breakdown": {},  # cc -> {marketplace_id, orders_count, units_sold}
    }

    # Prepare per-country aggregators
    for cc, mid in mp_map.items():
        totals["breakdown"][cc] = {"marketplace_id": mid, "orders_count": 0, "units_sold": 0}

    raw_orders_rows: List[Dict[str, Any]] = []
    raw_items_rows: List[Dict[str, Any]] = []
    
    # Aggregation buffer for ASIN daily: 
    # Key: (country, marketplace_id, asin)
    # Value: {orders_count, units_sold, canceled_orders}
    asin_agg: Dict[Tuple[str, str, str], Dict[str, int]] = {}

    for i, o in enumerate(orders):
        cc = country_for_marketplace_id(scope, o.marketplace_id)
        status = (o.order_status or "").lower()

        is_canceled = status == "canceled" or status == "cancelled"
        if is_canceled:
            totals["canceled_orders"] += 1

        sales_channel = (o.sales_channel or "").strip()
        is_non_amazon = bool(sales_channel) and sales_channel.lower() != "amazon"
        if is_non_amazon:
            totals["excluded_non_amazon_orders"] += 1

        # We assume we want to track ASIN stats even if canceled (recorded as canceled_orders)
        # But we exclude non-amazon from "units_sold" totals usually? 
        # For simplicity, we process items for ALL fetched orders to have complete raw data.
        
        # Determine if this order contributes to "Valid Sales"
        is_valid_sale = (not is_canceled) and (not is_non_amazon)

        units_in_order = 0
        per_order_asin_units: Dict[str, int] = {}
        
        # Fetch items
        def _call_items():
            return spapi_request_json(
                scope="EU" if scope.upper() in ("EU", "UK") else scope.upper(),
                method="GET",
                path=f"/orders/v0/orders/{o.amazon_order_id}/orderItems",
                query={},
            )
        
        try:
            items_resp = _retry_spapi(_call_items, stage="order_items", run_id=run_id)
            payload = items_resp.get("payload") or {}
            items_list = payload.get("OrderItems") or []

            for it in items_list:
                asin = it.get("ASIN")
                seller_sku = it.get("SellerSKU")
                qty_purchased = it.get("QuantityOrdered") or 0
                
                # Check cancellation at item level? Usually we use order status, 
                # but item level also has QuantityCancelled.
                units = _extract_item_units(it)
                
                # Add to Item Raw Rows
                raw_items_rows.append({
                    "amazon_order_id": o.amazon_order_id,
                    "asin": asin,
                    "seller_sku": seller_sku,
                    "quantity_ordered": int(qty_purchased),
                    "item_status": o.order_status, # Inherit order status
                    "raw_json_str": json.dumps(it, ensure_ascii=False),
                    "country": cc,
                    "marketplace_id": o.marketplace_id,
                })

                if is_valid_sale:
                    units_in_order += units
                if asin:
                    per_order_asin_units[asin] = per_order_asin_units.get(asin, 0) + units

        except SpapiRequestError:
            raise
        except Exception as e:
            logger.error(json.dumps({"event": "fetch_items_error", "order_id": o.amazon_order_id, "error": str(e), "run_id": run_id}))
            raise

        for asin, units in per_order_asin_units.items():
            key = (cc, o.marketplace_id, asin)
            if key not in asin_agg:
                asin_agg[key] = {"orders_count": 0, "units_sold": 0, "canceled_orders": 0}

            if is_canceled:
                asin_agg[key]["canceled_orders"] += 1
                continue
            if is_non_amazon:
                continue

            asin_agg[key]["orders_count"] += 1
            asin_agg[key]["units_sold"] += units

        # Update Totals
        if is_valid_sale:
            totals["orders_count"] += 1
            totals["units_sold"] += units_in_order
            if cc in totals["breakdown"]:
                totals["breakdown"][cc]["orders_count"] += 1
                totals["breakdown"][cc]["units_sold"] += units_in_order
            else:
                totals["breakdown"][cc] = {"marketplace_id": o.marketplace_id, "orders_count": 1, "units_sold": units_in_order}

        # Order Raw Row
        raw_payload = dict(o.raw)
        if debug_items:
            raw_payload["_debug_units_sold"] = units_in_order
            raw_payload["_debug_valid"] = is_valid_sale

        raw_orders_rows.append(
            {
                "amazon_order_id": o.amazon_order_id,
                "marketplace_id": o.marketplace_id,
                "country": cc,
                "order_status": o.order_status,
                "units_sold": units_in_order,
                "raw_json_str": json.dumps(raw_payload, ensure_ascii=False),
            }
        )

        # Mild pacing to reduce 429s on orderItems
        if i % 10 == 0:
            logger.info(json.dumps({"event": "progress", "processed": i + 1, "total": len(orders), "run_id": run_id}))
        time.sleep(0.08)

    # Convert Aggregation Buffer to Rows
    asin_daily_rows = []
    for (cc, mid, asin), stats in asin_agg.items():
        asin_daily_rows.append({
            "country": cc,
            "marketplace_id": mid,
            "asin": asin,
            "orders_count": stats["orders_count"],
            "units_sold": stats["units_sold"],
            "canceled_orders": stats["canceled_orders"]
        })

    return totals, raw_orders_rows, raw_items_rows, asin_daily_rows

def _bq_insert_with_fallback(
    client: bigquery.Client,
    table_id: str,
    rows: List[Dict[str, Any]],
    *,
    allow_drop_fields: bool = True,
) -> Dict[str, Any]:
    """
    Insert rows via streaming API.
    If BigQuery returns "no such field: X" right after a schema ALTER, retry once after dropping those fields.
    """
    if not rows:
        return {"table": table_id, "inserted": 0, "errors": []}

    errors = client.insert_rows_json(table_id, rows)
    if not errors:
        return {"table": table_id, "inserted": len(rows), "errors": []}

    if not allow_drop_fields:
        return {"table": table_id, "inserted": 0, "errors": errors}

    # Detect unknown fields
    unknown_fields = set()
    for e in errors:
        for ee in e.get("errors", []):
            msg = (ee.get("message") or "")
            loc = (ee.get("location") or "")
            if "no such field" in msg.lower() and loc:
                unknown_fields.add(loc)

    if not unknown_fields:
        return {"table": table_id, "inserted": 0, "errors": errors}

    # Retry once after dropping unknown fields (schema propagation lag workaround)
    rows2 = [{k: v for k, v in r.items() if k not in unknown_fields} for r in rows]
    errors2 = client.insert_rows_json(table_id, rows2)
    return {
        "table": table_id,
        "inserted": 0 if errors2 else len(rows2),
        "errors": errors2 if errors2 else [],
        "fallback_dropped_fields": sorted(list(unknown_fields)),
        "first_errors": errors[:3],
    }

def write_bigquery(
    scope: str,
    snapshot_date: date,
    run_id: str,
    *,
    totals: Dict[str, Any],
    raw_orders_rows: List[Dict[str, Any]],
    raw_items_rows: List[Dict[str, Any]],
    asin_daily_rows: List[Dict[str, Any]],
    filter_mode: str,
    dry: bool,
) -> Dict[str, Any]:
    if dry:
        return {"dry": True}

    client = bigquery.Client()
    ingested_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    results = {}

    # 1. Orders Raw
    bq_rows = []
    for r in raw_orders_rows:
        bq_rows.append({
            "scope": scope,
            "snapshot_date": str(snapshot_date),
            "amazon_order_id": r["amazon_order_id"],
            "marketplace_id": r["marketplace_id"],
            "raw_json_str": r["raw_json_str"],
            "ingested_at": ingested_at,
            "run_id": run_id,
            "country": r.get("country", ""),
            "units_sold": int(r.get("units_sold", 0) or 0),
            "order_status": r.get("order_status", ""),
        })
    results["orders_raw"] = _bq_insert_with_fallback(client, bq_orders_raw_table_id(), bq_rows)

    # 2. Daily Agg (Legacy Country Level)
    agg_rows = []
    for cc, v in (totals.get("breakdown") or {}).items():
        agg_rows.append({
            "ingested_at": ingested_at,
            "run_id": run_id,
            "scope": scope,
            "snapshot_date": str(snapshot_date),
            "country_code": cc,
            "marketplace_id": v.get("marketplace_id", ""),
            "orders_count": int(v.get("orders_count", 0) or 0),
            "units_sold": int(v.get("units_sold", 0) or 0),
            "filter_mode": filter_mode,
            "excluded_canceled_orders": int(totals.get("canceled_orders", 0) or 0),
            "excluded_non_amazon_orders": int(totals.get("excluded_non_amazon_orders", 0) or 0),
        })
    results["sales_daily_agg"] = _bq_insert_with_fallback(client, bq_orders_agg_table_id(), agg_rows, allow_drop_fields=False)

    # 3. Items Raw
    items_bq = []
    for r in raw_items_rows:
        items_bq.append({
            "run_id": run_id,
            "ingested_at": ingested_at,
            "scope": scope,
            "snapshot_date": str(snapshot_date),
            "amazon_order_id": r["amazon_order_id"],
            "asin": r["asin"],
            "seller_sku": r["seller_sku"],
            "quantity_ordered": r["quantity_ordered"],
            "item_status": r["item_status"],
            "raw_json_str": r["raw_json_str"],
            "country": r.get("country", ""),
            "marketplace_id": r.get("marketplace_id", "")
        })
    results["order_items_raw"] = _bq_insert_with_fallback(client, bq_order_items_raw_table_id(), items_bq)

    # 4. ASIN Daily Agg
    asin_bq = []
    for r in asin_daily_rows:
        asin_bq.append({
            "run_id": run_id,
            "ingested_at": ingested_at,
            "scope": scope,
            "snapshot_date": str(snapshot_date),
            "country": r["country"],
            "marketplace_id": r["marketplace_id"],
            "asin": r["asin"],
            "orders_count": r["orders_count"],
            "units_sold": r["units_sold"],
            "canceled_orders": r["canceled_orders"]
        })
    results["sales_asin_daily"] = _bq_insert_with_fallback(client, bq_sales_asin_daily_table_id(), asin_bq)

    return results

def run_daily(
    scope: str,
    snapshot_date: date,
    *,
    dry: bool = True,
    debug_items: bool = False,
    compact: bool = True,
    filter_mode: str = "Created",
    max_pages: int = 50,
    page_size: int = 100,
    max_orders: int = 5000,
) -> Dict[str, Any]:
    scope = scope.upper()
    run_id = str(uuid.uuid4())
    logger.info(json.dumps({"event": "run_daily_start", "run_id": run_id, "scope": scope, "date": str(snapshot_date)}))

    orders, tw_debug = fetch_orders_for_scope(
        scope=scope,
        snapshot_date=snapshot_date,
        filter_mode=filter_mode,
        page_size=page_size,
        max_pages=max_pages,
        max_orders=max_orders,
        run_id=run_id,
        include_debug=debug_items,
        compact=compact,
    )
    status_breakdown: Dict[str, int] = {}
    for o in orders:
        key = (o.order_status or "").strip() or "UNKNOWN"
        status_breakdown[key] = status_breakdown.get(key, 0) + 1

    totals, raw_orders, raw_items, asin_rows = process_orders_and_items(
        scope, orders, debug_items=debug_items, run_id=run_id
    )

    bq_res = write_bigquery(
        scope=scope,
        snapshot_date=snapshot_date,
        run_id=run_id,
        totals=totals,
        raw_orders_rows=raw_orders,
        raw_items_rows=raw_items,
        asin_daily_rows=asin_rows,
        filter_mode=filter_mode,
        dry=dry,
    )

    resp = {
        "run_id": run_id,
        "scope": scope,
        "ok": True,
        "stage": "complete",
        "orders_count": totals["orders_count"],
        "units_sold": totals["units_sold"],
        "breakdown": totals["breakdown"],
        "items_rows_count": len(raw_items),
        "asin_stats_count": len(asin_rows),
        "bq": bq_res,
        "time_window_debug": tw_debug,
    }

    if debug_items:
        resp["debug"] = {
            "list_orders": tw_debug.get("list_orders") or {},
            "list_orders_by_country": tw_debug.get("list_orders_by_country") or {},
            "parsed_orders_len": len(orders),
            "parsed_status_breakdown": status_breakdown,
        }

    return resp
