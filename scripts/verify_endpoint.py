import argparse
import json
import os
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlencode, urlparse

try:
    import requests  # type: ignore
except Exception:
    requests = None  # type: ignore


DEFAULT_TIMEOUT = 120
DEFAULT_RETRIES = 3


def _emit(payload: Dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False))


def _error_result(error_type: str, message: str, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = {
        "ok": False,
        "error": {"type": error_type, "message": message},
    }
    if extra:
        payload.update(extra)
    return payload


def _coerce_int_status(value: Any) -> Optional[int]:
    if isinstance(value, int):
        return int(value)
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _validate_url(base_url: str) -> Optional[str]:
    if not base_url:
        return "URL is empty"
    parsed = urlparse(base_url)
    if parsed.scheme not in ("http", "https"):
        return "URL scheme must be http or https"
    if not parsed.netloc:
        return "URL must include host"
    return None


def _get_auth_token() -> Optional[str]:
    env_token = os.getenv("AUTH_TOKEN", "")
    return env_token or None


def _build_url(base_url: str, path: str, query_params: Dict[str, Any]) -> str:
    base = base_url.rstrip("/") + "/"
    full_path = path.lstrip("/")
    url = urljoin(base, full_path)
    qs = urlencode(query_params)
    return f"{url}?{qs}" if qs else url


def _request_with_requests(
    url: str,
    headers: Dict[str, str],
    timeout: int,
) -> Dict[str, Any]:
    response = requests.get(url, headers=headers, timeout=timeout)
    return {
        "http_status": response.status_code,
        "headers": dict(response.headers),
        "body": response.text,
    }


def _request_with_urllib(
    url: str,
    headers: Dict[str, str],
    timeout: int,
) -> Dict[str, Any]:
    from urllib.request import Request, urlopen
    from urllib.error import HTTPError, URLError

    req = Request(url, headers=headers, method="GET")
    try:
        with urlopen(req, timeout=timeout) as resp:
            status = getattr(resp, "status", 0) or 0
            body = resp.read().decode("utf-8", errors="replace")
            return {
                "http_status": status,
                "headers": dict(resp.headers),
                "body": body,
            }
    except HTTPError as exc:
        return {
            "http_status": exc.code,
            "headers": dict(getattr(exc, "headers", {}) or {}),
            "body": exc.read().decode("utf-8", errors="replace") if exc.fp else "",
            "error": str(exc),
        }
    except URLError as exc:
        raise RuntimeError(str(exc)) from exc


def _make_request(
    url: str,
    headers: Dict[str, str],
    timeout: int,
    retries: int,
) -> Dict[str, Any]:
    backoff = 1.0
    last_error: Optional[str] = None
    for attempt in range(retries):
        try:
            if requests is not None:
                return _request_with_requests(url, headers, timeout)
            return _request_with_urllib(url, headers, timeout)
        except Exception as exc:
            last_error = str(exc)
            if attempt == retries - 1:
                break
            time.sleep(backoff)
            backoff *= 2
    raise RuntimeError(last_error or "Request failed")


def _summarize_headers(headers: Dict[str, Any]) -> Dict[str, Any]:
    summary = {}
    for key in list(headers.keys())[:20]:
        summary[key] = headers.get(key)
    return summary


def _assertion(name: str, passed: bool, detail: str) -> Dict[str, Any]:
    return {"name": name, "passed": passed, "detail": detail}


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify Cloud Run endpoint responses")
    parser.add_argument("--url", required=True, help="Base URL to verify")
    parser.add_argument("--path", default="/cron/daily", help="Endpoint path")
    parser.add_argument("--scope", default="EU", help="Scope parameter")
    parser.add_argument("--snapshot-date", default="2026-01-17", help="Snapshot date")
    parser.add_argument("--dry", type=int, default=0, help="Dry run flag")
    parser.add_argument("--debug-items", type=int, default=1, help="Debug items flag")
    parser.add_argument("--compact", type=int, default=0, help="Compact response flag")
    parser.add_argument("--max-pages", type=int, default=50, help="Max pages")
    parser.add_argument("--page-size", type=int, default=100, help="Page size")
    parser.add_argument("--max-orders", type=int, default=5000, help="Max orders")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="Timeout seconds")
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES, help="Retry attempts")
    parser.add_argument(
        "--expect-orders-gt-zero",
        type=int,
        default=1,
        help="Enable items_rows_count check when orders_count is positive",
    )
    args = parser.parse_args()

    url_error = _validate_url(args.url)
    if url_error:
        _emit(_error_result("URL_INVALID", url_error, {"checked_url": args.url}))
        return 1

    query_params = {
        "scope": args.scope,
        "snapshot_date": args.snapshot_date,
        "dry": args.dry,
        "debugItems": args.debug_items,
        "compact": args.compact,
        "maxPages": args.max_pages,
        "pageSize": args.page_size,
        "maxOrders": args.max_orders,
    }
    full_url = _build_url(args.url, args.path, query_params)
    token = _get_auth_token()
    headers = {"Authorization": f"Bearer {token}"} if token else {}

    try:
        response = _make_request(full_url, headers, args.timeout, args.retries)
    except Exception as exc:
        _emit(
            _error_result(
                "HTTP_FAILED",
                str(exc),
                {
                    "checked_url": args.url,
                    "checked_path": args.path,
                    "query_params": query_params,
                },
            )
        )
        return 1

    http_status = response.get("http_status", 0)
    headers_summary = _summarize_headers(response.get("headers") or {})
    body = response.get("body") or ""

    if http_status < 200 or http_status >= 300:
        _emit(
            _error_result(
                "HTTP_FAILED",
                f"HTTP status {http_status}",
                {
                    "checked_url": args.url,
                    "checked_path": args.path,
                    "query_params": query_params,
                    "http_status": http_status,
                    "headers": headers_summary,
                },
            )
        )
        return 1

    if not body:
        _emit(
            _error_result(
                "EMPTY_BODY",
                "Response body is empty",
                {
                    "checked_url": args.url,
                    "checked_path": args.path,
                    "query_params": query_params,
                    "http_status": http_status,
                },
            )
        )
        return 1

    try:
        data = json.loads(body)
    except Exception:
        _emit(
            _error_result(
                "JSON_DECODE_FAILED",
                "Failed to parse JSON response",
                {
                    "checked_url": args.url,
                    "checked_path": args.path,
                    "query_params": query_params,
                    "http_status": http_status,
                    "snippet": body[:2000],
                },
            )
        )
        return 1

    response_ok = bool(data.get("ok"))
    response_status = data.get("status")
    response_stage = data.get("stage")
    response_run_id = data.get("run_id")
    response_error = data.get("error")
    status_int = _coerce_int_status(response_status)

    if (response_ok is False) or (status_int is not None and status_int != 200):
        _emit(
            _error_result(
                "RESPONSE_NOT_OK",
                f"Endpoint returned non-ok response; status={response_status} stage={response_stage}",
                {
                    "checked_url": args.url,
                    "checked_path": args.path,
                    "query_params": query_params,
                    "http_status": http_status,
                    "response_ok": response_ok,
                    "response_status": response_status,
                    "response_stage": response_stage,
                    "response_run_id": response_run_id,
                    "response_error": response_error,
                    "assertions": [],
                },
            )
        )
        return 1
    orders_count = data.get("orders_count")
    units_sold = data.get("units_sold")
    items_rows_count = data.get("items_rows_count")
    asin_stats_count = data.get("asin_stats_count")

    assertions: List[Dict[str, Any]] = []
    debug_required = bool(args.debug_items) or bool(args.compact == 0)
    debug = data.get("debug") or {}
    order_items_by_country = debug.get("order_items_by_country")

    if debug_required:
        has_debug = isinstance(order_items_by_country, dict)
        assertions.append(
            _assertion(
                "debug_order_items_by_country_present",
                has_debug,
                "present" if has_debug else "missing or not a dict",
            )
        )
        if has_debug:
            required_fields = {
                "orders_in_batch",
                "items_fetched",
                "items_after_filter",
                "first_error",
                "http_status",
                "spapi_status",
            }
            for country, entry in order_items_by_country.items():
                missing = [k for k in required_fields if k not in (entry or {})]
                assertions.append(
                    _assertion(
                        f"debug_fields_{country}",
                        not missing,
                        "ok" if not missing else f"missing {', '.join(missing)}",
                    )
                )

    if isinstance(order_items_by_country, dict):
        items_sum = 0
        for entry in order_items_by_country.values():
            try:
                items_sum += int(entry.get("items_after_filter") or 0)
            except Exception:
                pass
        if units_sold is not None:
            assertions.append(
                _assertion(
                    "units_sold_matches_items_after_filter",
                    int(units_sold) == items_sum,
                    f"units_sold={units_sold} sum_items_after_filter={items_sum}",
                )
            )

    expect_items = bool(args.expect_orders_gt_zero) and int(args.dry) == 0
    if expect_items and isinstance(orders_count, int) and orders_count > 0:
        passed = isinstance(items_rows_count, int) and items_rows_count > 0
        assertions.append(
            _assertion(
                "items_rows_count_non_zero",
                passed,
                f"orders_count={orders_count} items_rows_count={items_rows_count}",
            )
        )

    ok = all(a.get("passed") for a in assertions) if assertions else True
    result = {
        "ok": ok,
        "checked_url": args.url,
        "checked_path": args.path,
        "query_params": query_params,
        "http_status": http_status,
        "response_ok": response_ok,
        "response_status": response_status,
        "response_stage": response_stage,
        "response_run_id": response_run_id,
        "response_error": response_error,
        "orders_count": orders_count,
        "units_sold": units_sold,
        "items_rows_count": items_rows_count,
        "asin_stats_count": asin_stats_count,
        "assertions": assertions,
    }

    if not ok:
        result["error"] = {
            "type": "ASSERTION_FAILED",
            "message": "One or more assertions failed",
        }

    _emit(result)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
