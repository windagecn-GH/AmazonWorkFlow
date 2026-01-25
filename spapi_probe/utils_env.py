from __future__ import annotations

import os
from typing import List


def get_missing_required_envs() -> List[str]:
    required = [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_REGION",
        "LWA_CLIENT_ID",
        "LWA_CLIENT_SECRET",
    ]
    missing = [name for name in required if not os.getenv(name)]

    refresh_candidates = [
        "LWA_REFRESH_TOKEN_EU",
        "LWA_REFRESH_TOKEN_NA",
        "LWA_REFRESH_TOKEN",
    ]
    if not any(os.getenv(name) for name in refresh_candidates):
        missing.append("LWA_REFRESH_TOKEN_EU|LWA_REFRESH_TOKEN_NA|LWA_REFRESH_TOKEN")

    return missing
