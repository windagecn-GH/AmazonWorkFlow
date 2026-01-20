import importlib.util
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from spapi_probe import config  # noqa: E402


def main() -> None:
    fastapi_spec = importlib.util.find_spec("fastapi")
    if fastapi_spec is None:
        print("fastapi_missing", True)
    else:
        from spapi_probe.main import app  # noqa: E402
        print("app_title", getattr(app, "title", "unknown"))
    print("bq_dataset", config.BQ_DATASET)


if __name__ == "__main__":
    main()
