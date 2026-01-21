import base64
import hashlib
import hmac
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import requests


# -----------------------------
# Region config
# -----------------------------
_REGION_TO_HOST = {
    "NA": "sellingpartnerapi-na.amazon.com",
    "EU": "sellingpartnerapi-eu.amazon.com",
    "FE": "sellingpartnerapi-fe.amazon.com",
}

# SP-API SigV4 regions (common defaults)
_REGION_TO_AWS_REGION = {
    "NA": "us-east-1",
    "EU": "eu-west-1",
    "FE": "us-west-2",
}

# LWA refresh token env per region (your envcheck shows *_NA and *_EU exist)
_REGION_TO_REFRESH_ENV = {
    "NA": "LWA_REFRESH_TOKEN_NA",
    "EU": "LWA_REFRESH_TOKEN_EU",
    "FE": "LWA_REFRESH_TOKEN_FE",
}


def _utc_amz_date() -> Tuple[str, str]:
    """
    Returns:
      amz_date: YYYYMMDD'T'HHMMSS'Z'
      date_stamp: YYYYMMDD
    """
    now = datetime.now(timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")
    return amz_date, date_stamp


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _hmac_sha256(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _sigv4_signing_key(secret_key: str, date_stamp: str, region: str, service: str) -> bytes:
    k_date = _hmac_sha256(("AWS4" + secret_key).encode("utf-8"), date_stamp)
    k_region = hmac.new(k_date, region.encode("utf-8"), hashlib.sha256).digest()
    k_service = hmac.new(k_region, service.encode("utf-8"), hashlib.sha256).digest()
    k_signing = hmac.new(k_service, b"aws4_request", hashlib.sha256).digest()
    return k_signing


@dataclass
class LwaToken:
    access_token: str
    expires_at: float  # epoch seconds


_LWA_TOKEN_CACHE: Dict[str, LwaToken] = {}


def _get_env_required(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v

def _normalize_scope(scope: str) -> str:
    scope_u = (scope or "NA").upper()
    if scope_u == "UK":
        return "EU"
    if scope_u in ("US", "CA", "MX", "BR"):
        return "NA"
    return scope_u

def _refresh_token_candidates(scope: str) -> List[str]:
    scope_u = _normalize_scope(scope)
    if scope_u == "EU":
        return ["LWA_REFRESH_TOKEN_EU", "LWA_REFRESH_TOKEN"]
    if scope_u == "NA":
        return ["LWA_REFRESH_TOKEN_NA", "LWA_REFRESH_TOKEN"]
    return ["LWA_REFRESH_TOKEN"]

def _select_refresh_token(scope: str) -> str:
    candidates = _refresh_token_candidates(scope)
    for name in candidates:
        val = os.getenv(name)
        if val:
            return val
    scope_u = _normalize_scope(scope)
    raise RuntimeError(
        f"Missing refresh token for scope {scope_u}. Checked envs: {', '.join(candidates)}"
    )


def _get_lwa_access_token(region: str) -> Tuple[str, Dict[str, Any]]:
    """
    Returns (access_token, debug_dict)
    Caches per-region token in memory (Cloud Run instance).
    """
    debug: Dict[str, Any] = {}
    region = _normalize_scope(region)

    cache_key = f"lwa:{region}"
    cached = _LWA_TOKEN_CACHE.get(cache_key)
    now = time.time()

    if cached and cached.expires_at - now > 60:
        debug["cached"] = True
        debug["expires_in"] = int(cached.expires_at - now)
        debug["status_code"] = 200
        debug["token_type"] = "bearer"
        debug["token_url"] = "https://api.amazon.com/auth/o2/token"
        debug["access_token_len"] = len(cached.access_token)
        return cached.access_token, debug

    client_id = _get_env_required("LWA_CLIENT_ID")
    client_secret = _get_env_required("LWA_CLIENT_SECRET")

    refresh_token = _select_refresh_token(region)

    token_url = "https://api.amazon.com/auth/o2/token"
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
    }

    r = requests.post(token_url, data=payload, timeout=30)
    debug["cached"] = False
    debug["status_code"] = r.status_code
    debug["token_url"] = token_url

    if r.status_code != 200:
        debug["body"] = r.text[:2000]
        raise RuntimeError(f"LWA token request failed {r.status_code}: {r.text}")

    data = r.json()
    access_token = data.get("access_token")
    expires_in = int(data.get("expires_in", 3600))
    token_type = data.get("token_type", "bearer")

    if not access_token:
        raise RuntimeError(f"LWA token response missing access_token: {data}")

    _LWA_TOKEN_CACHE[cache_key] = LwaToken(
        access_token=access_token,
        expires_at=now + expires_in,
    )

    debug["expires_in"] = expires_in
    debug["token_type"] = token_type
    debug["access_token_len"] = len(access_token)
    return access_token, debug


def _canonical_query_string(query: Optional[Dict[str, Any]]) -> str:
    if not query:
        return ""
    # SigV4 requires query params sorted by key, and values URL-encoded
    items = []
    for k in sorted(query.keys()):
        v = query[k]
        if v is None:
            continue
        # SP-API expects repeated params sometimes; keep simple: stringify
        items.append((str(k), str(v)))
    return urlencode(items, safe="-_.~")


def _canonical_headers(headers: Dict[str, str]) -> Tuple[str, str]:
    """
    Returns:
      canonical_headers: 'key:val\n...'
      signed_headers: 'key;key;...'
    """
    cleaned = {}
    for k, v in headers.items():
        if v is None:
            continue
        lk = k.strip().lower()
        lv = " ".join(str(v).strip().split())
        cleaned[lk] = lv

    keys = sorted(cleaned.keys())
    canonical = "".join([f"{k}:{cleaned[k]}\n" for k in keys])
    signed = ";".join(keys)
    return canonical, signed


def spapi_request(
    scope: str,
    method: str,
    path: str,
    query: Optional[Dict[str, Any]] = None,
    body: Optional[Any] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 60,
    **_ignored_kwargs: Any,
) -> Tuple[int, Any, Dict[str, Any]]:
    """
    Low-level SP-API request.
    
    Args:
        scope: "NA", "EU", or "FE". Maps to region.
    """
    region = _normalize_scope(scope)
    method = (method or "GET").upper()

    host = _REGION_TO_HOST.get(region, _REGION_TO_HOST["NA"])
    aws_region = _REGION_TO_AWS_REGION.get(region, _REGION_TO_AWS_REGION["NA"])
    service = "execute-api"

    access_key = _get_env_required("AWS_ACCESS_KEY_ID")
    secret_key = _get_env_required("AWS_SECRET_ACCESS_KEY")

    lwa_token, lwa_debug = _get_lwa_access_token(region)

    # Build URL
    if not path.startswith("/"):
        path = "/" + path
    qs = _canonical_query_string(query)
    url = f"https://{host}{path}" + (f"?{qs}" if qs else "")

    # Prepare body
    if body is None or body == "":
        payload_bytes = b""
        content_type = None
    else:
        if isinstance(body, (dict, list)):
            payload_bytes = json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            content_type = "application/json"
        elif isinstance(body, (bytes, bytearray)):
            payload_bytes = bytes(body)
            content_type = "application/octet-stream"
        else:
            payload_bytes = str(body).encode("utf-8")
            content_type = "text/plain; charset=utf-8"

    amz_date, date_stamp = _utc_amz_date()

    req_headers: Dict[str, str] = {
        "host": host,
        "x-amz-date": amz_date,
        "x-amz-access-token": lwa_token,
        "user-agent": "spapi-probe/2.x",
    }
    if content_type:
        req_headers["content-type"] = content_type

    # Merge caller headers (caller wins)
    if headers:
        for k, v in headers.items():
            if v is None:
                continue
            req_headers[str(k)] = str(v)

    payload_hash = _sha256_hex(payload_bytes)

    canonical_headers, signed_headers = _canonical_headers(req_headers)
    canonical_request = "\n".join(
        [
            method,
            path,
            qs,
            canonical_headers,
            signed_headers,
            payload_hash,
        ]
    )

    algorithm = "AWS4-HMAC-SHA256"
    credential_scope = f"{date_stamp}/{aws_region}/{service}/aws4_request"
    string_to_sign = "\n".join(
        [
            algorithm,
            amz_date,
            credential_scope,
            _sha256_hex(canonical_request.encode("utf-8")),
        ]
    )

    signing_key = _sigv4_signing_key(secret_key, date_stamp, aws_region, service)
    signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

    authorization_header = (
        f"{algorithm} "
        f"Credential={access_key}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, "
        f"Signature={signature}"
    )
    req_headers["authorization"] = authorization_header

    debug: Dict[str, Any] = {
        "region": region,
        "spapi_host": host,
        "aws_region": aws_region,
        "method": method,
        "path": path,
        "query": query or {},
        "status_code": None,
        "request_id": None,
        "rid": None,
        "lwa": lwa_debug,
    }

    try:
        r = requests.request(method, url, headers=req_headers, data=payload_bytes, timeout=timeout)
    except Exception as e:
        debug["status_code"] = 0
        debug["error"] = repr(e)
        raise

    debug["status_code"] = r.status_code
    # Common request id headers
    debug["request_id"] = r.headers.get("x-amzn-RequestId") or r.headers.get("x-amz-request-id")
    debug["rid"] = r.headers.get("x-amz-rid") or r.headers.get("x-amzn-rid")

    # Parse response
    resp_body: Any
    text = r.text if r.text is not None else ""
    if text:
        try:
            resp_body = r.json()
        except Exception:
            resp_body = text
    else:
        resp_body = ""

    return r.status_code, resp_body, debug
