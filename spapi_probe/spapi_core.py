# spapi_core.py
from __future__ import annotations

from typing import Any, Dict, Optional

from .spapi_client import spapi_request


def spapi_request_json(
    region: str,
    method: str,
    path: str,
    query: Optional[Dict[str, Any]] = None,
    body: Optional[Any] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 60,
) -> Dict[str, Any]:
    """
    Wrap spapi_client.spapi_request() into a stable dict return shape.

    Always returns a dict with keys:
      - ok: bool
      - status / status_code: int
      - payload / body: parsed JSON (dict/list) or text
      - debug: dict
      - error: str (only when ok is False or exception)
    """
    try:
        status, resp_body, debug = spapi_request(
            region=region,
            method=method,
            path=path,
            query=query,
            body=body,
            headers=headers,
            timeout=timeout,
        )

        # Build normalized response dict
        out: Dict[str, Any] = {
            "ok": 200 <= int(status) < 300,
            "status": int(status),
            "status_code": int(status),
            "payload": resp_body,
            "body": resp_body,  # alias for convenience
            "debug": debug if isinstance(debug, dict) else {"debug_raw": debug},
        }

        if not out["ok"]:
            out["error"] = _extract_error_message(resp_body, status)
        return out

    except Exception as e:
        return {
            "ok": False,
            "status": 0,
            "status_code": 0,
            "payload": None,
            "body": None,
            "debug": {"exception": repr(e)},
            "error": repr(e),
        }


def _extract_error_message(resp_body: Any, status: Any) -> str:
    """
    Try to extract a useful error string from SP-API style error bodies.
    """
    try:
        st = int(status)
    except Exception:
        st = status

    if isinstance(resp_body, dict):
        # SP-API sometimes returns {"errors":[{"message":...,"code":...}]}
        if "errors" in resp_body and isinstance(resp_body["errors"], list) and resp_body["errors"]:
            first = resp_body["errors"][0] or {}
            msg = first.get("message") or first.get("details") or str(first)
            code = first.get("code")
            if code:
                return f"HTTP {st}: {code} - {msg}"
            return f"HTTP {st}: {msg}"

        # Or {"message": "..."} or {"error": "..."}
        if "message" in resp_body:
            return f"HTTP {st}: {resp_body.get('message')}"
        if "error" in resp_body:
            return f"HTTP {st}: {resp_body.get('error')}"

        return f"HTTP {st}: {str(resp_body)[:500]}"

    if isinstance(resp_body, (list, tuple)):
        return f"HTTP {st}: {str(resp_body)[:500]}"

    # text / empty
    s = (resp_body or "")
    if isinstance(s, str) and s.strip():
        return f"HTTP {st}: {s[:500]}"
    return f"HTTP {st}: (empty response)"