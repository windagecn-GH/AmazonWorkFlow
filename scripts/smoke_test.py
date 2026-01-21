import importlib.util
import json
import os
import sys
import types
from datetime import date

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from spapi_probe import config  # noqa: E402

if importlib.util.find_spec("requests") is None:
    sys.modules["requests"] = types.SimpleNamespace()

from spapi_probe import spapi_core  # noqa: E402


def _run_spapi_shape_tests() -> None:
    original = spapi_core.spapi_request
    try:
        spapi_core.spapi_request = lambda **_: (200, {"hello": "world"}, {"rid": "x"})
        ok_resp = spapi_core.spapi_request_json(scope="EU", method="GET", path="/__test__")
        assert isinstance(ok_resp, dict)
        assert ok_resp.get("ok") is True
        assert ok_resp.get("payload", {}).get("hello") == "world"
        assert "debug" in ok_resp and "status" in ok_resp

        spapi_core.spapi_request = lambda **_: (
            403,
            {"errors": [{"message": "Forbidden", "code": "Unauthorized"}]},
            {"rid": "y"},
        )
        bad_resp = spapi_core.spapi_request_json(scope="EU", method="GET", path="/__test__")
        assert bad_resp.get("ok") is False
        assert bad_resp.get("error")
    finally:
        spapi_core.spapi_request = original

def _run_refresh_token_selection_tests() -> None:
    from spapi_probe import spapi_client  # noqa: E402

    keys = [
        "LWA_REFRESH_TOKEN_EU",
        "LWA_REFRESH_TOKEN_NA",
        "LWA_REFRESH_TOKEN",
    ]
    saved = {k: os.environ.get(k) for k in keys}
    try:
        for k in keys:
            os.environ.pop(k, None)

        os.environ["LWA_REFRESH_TOKEN_EU"] = "eu-token"
        assert spapi_client._select_refresh_token("EU") == "eu-token"

        os.environ.pop("LWA_REFRESH_TOKEN_EU", None)
        os.environ["LWA_REFRESH_TOKEN_NA"] = "na-token"
        assert spapi_client._select_refresh_token("US") == "na-token"

        os.environ.pop("LWA_REFRESH_TOKEN_NA", None)
        os.environ["LWA_REFRESH_TOKEN"] = "fallback-token"
        assert spapi_client._select_refresh_token("EU") == "fallback-token"

        os.environ.pop("LWA_REFRESH_TOKEN", None)
        try:
            spapi_client._select_refresh_token("EU")
            assert False, "expected missing refresh token error"
        except RuntimeError as exc:
            msg = str(exc)
            assert "scope EU" in msg
            assert "LWA_REFRESH_TOKEN_EU" in msg
            assert "LWA_REFRESH_TOKEN" in msg
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

def _run_orders_parse_tests() -> None:
    from spapi_probe import orders_agg  # noqa: E402

    original = orders_agg.spapi_request_json
    try:
        seen_marketplace_ids = []

        def _mock_spapi_request_json(*, scope: str, method: str, path: str, query=None, **_kwargs):
            if path == "/orders/v0/orders":
                seen_marketplace_ids.append((query or {}).get("MarketplaceIds"))
            if path == "/orders/v0/orders":
                return {
                    "ok": True,
                    "status": 200,
                    "payload": {
                        "payload": {
                            "Orders": [
                                {
                                    "AmazonOrderId": "ORDER1",
                                    "MarketplaceId": "A1PA6795UKMFR9",
                                    "OrderStatus": "Shipped",
                                    "SalesChannel": "Amazon",
                                }
                            ],
                            "NextToken": None,
                        }
                    },
                    "debug": {"rid": "x"},
                }
            if path.endswith("/orderItems"):
                return {
                    "ok": True,
                    "status": 200,
                    "payload": {
                        "payload": {
                            "OrderItems": [
                                {
                                    "ASIN": "B000TEST",
                                    "SellerSKU": "SKU1",
                                    "QuantityOrdered": 2,
                                }
                            ]
                        }
                    },
                    "debug": {"rid": "y"},
                }
            return {"ok": False, "status": 404, "payload": {}, "error": "not found", "debug": {}}

        orders_agg.spapi_request_json = _mock_spapi_request_json
        out = orders_agg.run_daily(
            scope="EU",
            snapshot_date=date(2026, 1, 17),
            dry=True,
            debug_items=True,
            compact=True,
            filter_mode="Created",
            max_pages=1,
            page_size=1,
            max_orders=10,
        )
        debug = out.get("debug") or {}
        assert debug.get("parsed_orders_len", 0) > 0
        assert out.get("orders_count", 0) > 0
        assert out.get("units_sold", 0) > 0
        by_country = debug.get("list_orders_by_country") or {}
        de_query = (by_country.get("DE") or {}).get("query") or {}
        assert "," not in (de_query.get("MarketplaceIds") or "")
    finally:
        orders_agg.spapi_request_json = original


def main() -> None:
    fastapi_spec = importlib.util.find_spec("fastapi")
    if fastapi_spec is None:
        print("fastapi_missing", True)
    else:
        from spapi_probe.main import app, cron_daily, cron_inventory  # noqa: E402
        print("app_title", getattr(app, "title", "unknown"))
    print("bq_dataset", config.BQ_DATASET)
    _run_spapi_shape_tests()
    print("spapi_shape_tests", "ok")
    _run_refresh_token_selection_tests()
    print("refresh_token_tests", "ok")
    _run_orders_parse_tests()
    print("orders_parse_tests", "ok")

    if fastapi_spec is not None:
        prev_k_service = os.environ.pop("K_SERVICE", None)
        try:
            daily_resp = cron_daily(
                scope="EU",
                snapshot_date="2026-01-17",
                dry=0,
                debugItems=0,
                compact=1,
                filterMode="Created",
                maxPages=1,
                pageSize=1,
                maxOrders=1,
            )
            daily_body = getattr(daily_resp, "body", b"{}")
            daily_data = json.loads(daily_body.decode("utf-8"))
            assert daily_data.get("status") == "LOCAL_EXEC_BLOCKED"
            assert daily_data.get("run_id")
            assert daily_data.get("stage")
            assert daily_data.get("error")

            daily_dry_resp = cron_daily(
                scope="EU",
                snapshot_date="2026-01-17",
                dry=1,
                debugItems=0,
                compact=1,
                filterMode="Created",
                maxPages=1,
                pageSize=1,
                maxOrders=1,
            )
            daily_dry_body = getattr(daily_dry_resp, "body", b"{}")
            daily_dry_data = json.loads(daily_dry_body.decode("utf-8"))
            assert daily_dry_data.get("status") == "DRY_RUN"
            assert daily_dry_data.get("run_id")

            inv_resp = cron_inventory(scope="EU", dry=0)
            inv_body = getattr(inv_resp, "body", b"{}")
            inv_data = json.loads(inv_body.decode("utf-8"))
            assert inv_data.get("status") == "LOCAL_EXEC_BLOCKED"
            assert inv_data.get("run_id")
            assert inv_data.get("stage")

            inv_dry_resp = cron_inventory(scope="EU", dry=1)
            inv_dry_body = getattr(inv_dry_resp, "body", b"{}")
            inv_dry_data = json.loads(inv_dry_body.decode("utf-8"))
            assert inv_dry_data.get("status") == "DRY_RUN"
            assert inv_dry_data.get("run_id")
            print("local_block_tests", "ok")
        finally:
            if prev_k_service is not None:
                os.environ["K_SERVICE"] = prev_k_service


if __name__ == "__main__":
    main()
