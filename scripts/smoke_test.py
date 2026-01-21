import importlib.util
import os
import sys
import types

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


def main() -> None:
    fastapi_spec = importlib.util.find_spec("fastapi")
    if fastapi_spec is None:
        print("fastapi_missing", True)
    else:
        from spapi_probe.main import app  # noqa: E402
        print("app_title", getattr(app, "title", "unknown"))
    print("bq_dataset", config.BQ_DATASET)
    _run_spapi_shape_tests()
    print("spapi_shape_tests", "ok")


if __name__ == "__main__":
    main()
