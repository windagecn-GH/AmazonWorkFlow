from __future__ import annotations

import subprocess
import sys
from typing import List


def _select_secrets(names: List[str]) -> List[str]:
    lower_names = [(n, n.lower()) for n in names]

    def pick(predicate) -> str:
        for raw, lower in lower_names:
            if predicate(lower):
                return raw
        return ""

    client_id = pick(lambda s: "lwa" in s and "client" in s and "id" in s)
    if not client_id:
        client_id = pick(lambda s: "spapi-lwa-client-id" in s)

    client_secret = pick(lambda s: "lwa" in s and "client" in s and "secret" in s)
    if not client_secret:
        client_secret = pick(lambda s: "spapi-lwa-client-secret" in s)

    refresh_eu = pick(lambda s: "refresh" in s and "eu" in s)
    refresh_na = pick(lambda s: "refresh" in s and "na" in s)

    def is_generic_refresh(s: str) -> bool:
        return "refresh" in s and "eu" not in s and "na" not in s

    refresh_generic = pick(is_generic_refresh)
    if not refresh_generic:
        refresh_generic = pick(lambda s: "refresh" in s and s not in {refresh_eu.lower(), refresh_na.lower()})

    return [client_id, client_secret, refresh_eu, refresh_na, refresh_generic]


def main() -> int:
    try:
        result = subprocess.run(
            ["gcloud", "secrets", "list", "--format=value(name)"],
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception as exc:
        print(f"Failed to list secrets: {exc}", file=sys.stderr)
        return 1

    names = [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]
    selections = _select_secrets(names)
    for value in selections:
        print(value)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
