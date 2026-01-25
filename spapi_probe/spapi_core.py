# spapi_core.py
from __future__ import annotations

import json
from typing import Any, Dict, Optional
from urllib.parse import urlencode

from .spapi_client import spapi_request


class SpapiRequestError(RuntimeError):
    def __init__(
        self,
        *,
        message: str,
        status: int,
        stage: str,
        run_id: str,
        debug: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.status = status
        self.stage = stage
        self.run_id = run_id
        self.debug = debug or {}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ok": False,
            "status": self.status,
            "error": self.message,
            "stage": self.stage,
            "run_id": self.run_id,
            "debug": self.debug,
        }


def _normalize_scope(scope: str) -> str:
    scope_u = (scope or "").upper()
    if scope_u == "UK":
        return "EU"
    return scope_u


def spapi_request_json(
    scope: str,
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
      - status: int
      - payload: parsed JSON (dict/list) or text
      - debug: dict
      - error: str (only when ok is False or exception)
    """
    try:
        normalized_scope = _normalize_scope(scope)
        status, resp_body, debug = spapi_request(
            scope=normalized_scope,
            method=method,
            path=path,
            query=query,
            body=body,
            headers=headers,
            timeout=timeout,
        )

        try:
            status_int = int(status)
        except Exception:
            status_int = 0

        # Build normalized response dict
        out: Dict[str, Any] = {
            "ok": 200 <= status_int < 300,
            "status": status_int,
            "payload": resp_body,
            "debug": debug if isinstance(debug, dict) else {"debug_raw": debug},
        }

        if not out["ok"]:
            if status_int in (401, 403):
                resp_text = resp_body
                if not isinstance(resp_text, str):
                    resp_text = json.dumps(resp_body, ensure_ascii=True)
                out["debug"]["response_text_trunc"] = (resp_text or "")[:2000]
            out["error"] = _extract_error_message(resp_body, status)
            out["payload"] = {}
        return out

    except Exception as e:
        http_status = 0
        response = getattr(e, "response", None)
        try:
            http_status = int(getattr(response, "status_code", 0) or 0)
        except Exception:
            http_status = 0
        request_url = path
        if query:
            request_url = f"{path}?{urlencode(query)}"
        query_keys = sorted(query.keys()) if isinstance(query, dict) else []
        trace_id = None
        if response is not None:
            headers_map = getattr(response, "headers", None)
            if headers_map:
                trace_id = (
                    headers_map.get("x-amzn-requestid")
                    or headers_map.get("x-amz-request-id")
                    or headers_map.get("x-request-id")
                )
        response_text_trunc = None
        if http_status in (401, 403) and response is not None:
            try:
                response_text_trunc = (getattr(response, "text", "") or "")[:2000]
            except Exception:
                response_text_trunc = None
        return {
            "ok": False,
            "status": http_status or 0,
            "payload": {},
            "debug": {
                "exc_type": type(e).__name__,
                "exc": repr(e),
                "where": "spapi_request_json",
                "path": path,
                "method": method,
                "scope": scope,
                "query_keys": query_keys,
                "trace_id": trace_id,
                "request_url": request_url,
                "http_status": http_status or 0,
                "response_text_trunc": response_text_trunc,
            },
            "error": str(e),
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
