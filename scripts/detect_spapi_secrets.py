from __future__ import annotations

import argparse
import subprocess
import sys
from typing import List, Tuple


def _collect_names() -> List[str]:
    try:
        result = subprocess.run(
            ["gcloud", "secrets", "list", "--format=value(name)"],
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception as exc:
        print(f"Failed to list secrets: {exc}", file=sys.stderr)
        raise
    return [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]


def _pick_first(names: List[Tuple[str, str]], predicate) -> str:
    for raw, lowered in names:
        if predicate(lowered):
            return raw
    return ""


def _select_client_id(names: List[Tuple[str, str]]) -> str:
    candidate = _pick_first(names, lambda s: "lwa" in s and "client" in s and "id" in s)
    if candidate:
        return candidate
    for raw, lowered in names:
        if lowered == "spapi-lwa-client-id" or "spapi-lwa-client-id" in lowered:
            return raw
    return ""


def _select_client_secret(names: List[Tuple[str, str]]) -> str:
    candidate = _pick_first(names, lambda s: "lwa" in s and "client" in s and "secret" in s)
    if candidate:
        return candidate
    for raw, lowered in names:
        if lowered == "spapi-lwa-client-secret" or "spapi-lwa-client-secret" in lowered:
            return raw
    return ""


def _pick_refresh(
    names: List[Tuple[str, str]],
    *,
    include_tokens: List[str],
    exclude_tokens: List[str],
) -> str:
    def matches(lowered: str) -> bool:
        if "refresh" not in lowered:
            return False
        for token in include_tokens:
            if token not in lowered:
                return False
        for token in exclude_tokens:
            if token in lowered:
                return False
        return True

    return _pick_first(names, matches)


def _select_refresh(names: List[Tuple[str, str]], scope: str) -> str:
    scope_norm = (scope or "").strip().lower() or "eu"
    if scope_norm == "eu":
        picked = _pick_refresh(names, include_tokens=["eu"], exclude_tokens=[])
        if picked:
            return picked
        picked = _pick_refresh(names, include_tokens=["na"], exclude_tokens=[])
        if picked:
            return picked
    elif scope_norm == "na":
        picked = _pick_refresh(names, include_tokens=["na"], exclude_tokens=[])
        if picked:
            return picked
        picked = _pick_refresh(names, include_tokens=["eu"], exclude_tokens=[])
        if picked:
            return picked

    return _pick_refresh(names, include_tokens=[], exclude_tokens=["eu", "na"])


def _select_secrets(names: List[str], scope: str) -> List[str]:
    lowered = [(n, n.lower()) for n in names]
    client_id = _select_client_id(lowered)
    client_secret = _select_client_secret(lowered)
    refresh = _select_refresh(lowered, scope)
    return [client_id, client_secret, refresh]


def main() -> int:
    parser = argparse.ArgumentParser(description="Detect SP-API secret names.")
    parser.add_argument("--scope", default="EU", help="Scope for refresh token selection.")
    args = parser.parse_args()

    try:
        names = _collect_names()
    except Exception:
        return 1

    selections = _select_secrets(names, args.scope)
    for value in selections:
        print(value)

    missing = []
    if not selections[0]:
        missing.append("LWA_CLIENT_ID_SECRET")
    if not selections[1]:
        missing.append("LWA_CLIENT_SECRET_SECRET")
    if not selections[2]:
        missing.append("LWA_REFRESH_SECRET")

    if missing:
        for item in missing:
            print(item, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
