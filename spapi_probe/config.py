import os
from typing import Dict, List, Tuple

# ---- SP-API / LWA ----
LWA_CLIENT_ID = os.getenv("LWA_CLIENT_ID", "")
LWA_CLIENT_SECRET = os.getenv("LWA_CLIENT_SECRET", "")
LWA_REFRESH_TOKEN = os.getenv("LWA_REFRESH_TOKEN", "")
LWA_REFRESH_TOKEN_EU = os.getenv("LWA_REFRESH_TOKEN_EU", "")
LWA_REFRESH_TOKEN_NA = os.getenv("LWA_REFRESH_TOKEN_NA", "")

# ---- AWS SigV4 ----
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN", "")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# SP-API endpoints by scope (base domain only; region for SigV4 is AWS_REGION above)
SPAPI_ENDPOINTS = {
    "NA": "https://sellingpartnerapi-na.amazon.com",
    "EU": "https://sellingpartnerapi-eu.amazon.com",
    "FE": "https://sellingpartnerapi-fe.amazon.com",
}

# ---- Marketplace IDs ----
MARKETPLACES: Dict[str, Dict[str, str]] = {
    "EU": {
        "DE": "A1PA6795UKMFR9",
        "FR": "A13V1IB3VIYZZH",
        "IT": "APJ6JRA9NG5V4",
        "ES": "A1RKKUPIHCS9HS",
        "NL": "A1805IZSGTT6HS",
        "SE": "A2NODRKZP88ZB9",
        "PL": "A1C3SOZRARQ6R3",
    },
    "UK": {
        "UK": "A1F83G8C2ARO7P",
    },
    "NA": {
        "US": "ATVPDKIKX0DER",
        "CA": "A2EUQ1WTGCTBG2",
        "MX": "A1AM78C64UM0Y8",
    },
}

# Timezone used to build the daily window
SCOPE_TZ = {
    "EU": "Europe/Berlin",
    "UK": "Europe/London",
    "NA": "America/Los_Angeles",
}

# ---- BigQuery ----
BQ_PROJECT = os.getenv("BQ_PROJECT", "")
BQ_DATASET = os.getenv("BQ_DATASET", "amazon_ops")

# Existing tables
BQ_TABLE_ORDERS = os.getenv("BQ_TABLE_ORDERS", "probe_orders_raw_v1")
BQ_TABLE_ORDERS_AGG = os.getenv("BQ_TABLE_ORDERS_AGG", "probe_orders_daily_agg_v1")

# New tables for ASIN/Inventory
BQ_TABLE_ORDER_ITEMS = os.getenv("BQ_TABLE_ORDER_ITEMS", "probe_order_items_raw_v1")
BQ_TABLE_SALES_ASIN_DAILY = os.getenv("BQ_TABLE_SALES_ASIN_DAILY", "probe_sales_asin_daily_v1")
BQ_TABLE_INV_FBA_ASIN = os.getenv("BQ_TABLE_INV_FBA_ASIN", "probe_inventory_fba_asin_v1")
BQ_TABLE_INV_AWD_ASIN = os.getenv("BQ_TABLE_INV_AWD_ASIN", "probe_inventory_awd_asin_v1")


def get_bq_table_id(table_name: str) -> str:
    if not BQ_PROJECT:
        raise RuntimeError("BQ_PROJECT is empty; set env BQ_PROJECT")
    if not BQ_DATASET:
        raise RuntimeError("BQ_DATASET is empty; set env BQ_DATASET")
    return f"{BQ_PROJECT}.{BQ_DATASET}.{table_name}"

def bq_orders_raw_table_id() -> str:
    return get_bq_table_id(BQ_TABLE_ORDERS)

def bq_orders_agg_table_id() -> str:
    return get_bq_table_id(BQ_TABLE_ORDERS_AGG)

def bq_order_items_raw_table_id() -> str:
    return get_bq_table_id(BQ_TABLE_ORDER_ITEMS)

def bq_sales_asin_daily_table_id() -> str:
    return get_bq_table_id(BQ_TABLE_SALES_ASIN_DAILY)

def bq_inv_fba_asin_table_id() -> str:
    return get_bq_table_id(BQ_TABLE_INV_FBA_ASIN)

def bq_inv_awd_asin_table_id() -> str:
    return get_bq_table_id(BQ_TABLE_INV_AWD_ASIN)

def marketplaces_for_scope(scope: str) -> Dict[str, str]:
    s = scope.upper()
    if s not in MARKETPLACES:
        raise ValueError(f"Unknown scope: {scope}")
    return MARKETPLACES[s]

def marketplace_ids_for_scope(scope: str) -> List[str]:
    mp = marketplaces_for_scope(scope)
    return list(mp.values())

def country_for_marketplace_id(scope: str, marketplace_id: str) -> str:
    mp = marketplaces_for_scope(scope)
    for cc, mid in mp.items():
        if mid == marketplace_id:
            return cc
    return "UNK"

def endpoint_for_scope(scope: str) -> str:
    s = scope.upper()
    if s == "UK":
        # UK is still on EU endpoint for SP-API
        return SPAPI_ENDPOINTS["EU"]
    if s not in SPAPI_ENDPOINTS:
        raise ValueError(f"Unknown scope: {scope}")
    return SPAPI_ENDPOINTS[s]

def tz_for_scope(scope: str) -> str:
    return SCOPE_TZ.get(scope.upper(), "UTC")

def require_env() -> Tuple[bool, Dict[str, bool]]:
    refresh_ready = bool(LWA_REFRESH_TOKEN_EU or LWA_REFRESH_TOKEN_NA or LWA_REFRESH_TOKEN)
    checks = {
        "LWA_CLIENT_ID": bool(LWA_CLIENT_ID),
        "LWA_CLIENT_SECRET": bool(LWA_CLIENT_SECRET),
        "LWA_REFRESH_TOKEN_EU": bool(LWA_REFRESH_TOKEN_EU),
        "LWA_REFRESH_TOKEN_NA": bool(LWA_REFRESH_TOKEN_NA),
        "LWA_REFRESH_TOKEN": bool(LWA_REFRESH_TOKEN),
        "LWA_REFRESH_TOKEN_READY": refresh_ready,
        "AWS_ACCESS_KEY_ID": bool(AWS_ACCESS_KEY_ID),
        "AWS_SECRET_ACCESS_KEY": bool(AWS_SECRET_ACCESS_KEY),
        "BQ_PROJECT": bool(BQ_PROJECT),
        "BQ_DATASET": bool(BQ_DATASET),
        "BQ_TABLE_ORDERS": bool(BQ_TABLE_ORDERS),
    }
    return all(checks.values()), checks
