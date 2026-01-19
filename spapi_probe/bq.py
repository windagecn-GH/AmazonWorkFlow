from __future__ import annotations

from typing import Iterable, List, Dict, Set
from datetime import date
from google.cloud import bigquery


DATASET = "amazon_ops"
TABLE_CHECKPOINT = f"{DATASET}.etl_orders_checkpoint"
TABLE_FACT_ORDER_ASIN = f"{DATASET}.fact_sales_order_asin"


def bq_client() -> bigquery.Client:
    # Cloud Run 默认用服务账号的 ADC
    return bigquery.Client()


def fetch_processed_order_ids(snapshot_date: date, scope: str) -> Set[str]:
    client = bq_client()
    q = f"""
      SELECT order_id
      FROM `{TABLE_CHECKPOINT}`
      WHERE snapshot_date = @d AND scope = @s
    """
    job = client.query(
        q,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("d", "DATE", snapshot_date.isoformat()),
                bigquery.ScalarQueryParameter("s", "STRING", scope),
            ]
        ),
    )
    return {row["order_id"] for row in job.result()}


def mark_orders_processed(rows: List[Dict]) -> None:
    """
    rows: [{snapshot_date, scope, region, order_id, processed_at}]
    """
    if not rows:
        return
    client = bq_client()
    errors = client.insert_rows_json(TABLE_CHECKPOINT, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert checkpoint errors: {errors}")


def insert_fact_sales_order_asin(rows: List[Dict]) -> None:
    """
    rows: [{snapshot_date, scope, region, marketplace_id, order_id, asin, ordered_units, item_revenue, currency, ingested_at}]
    """
    if not rows:
        return
    client = bq_client()
    errors = client.insert_rows_json(TABLE_FACT_ORDER_ASIN, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert fact_sales_order_asin errors: {errors}")