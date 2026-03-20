#!/usr/bin/env python3

import json
import sys
import time
from pathlib import Path


class _User:
    def __init__(self, payload):
        payload = payload or {}
        self.realm = payload.get("realm", "bits")
        self.username = payload.get("username", payload.get("id", "worker"))
        self.attributes = payload.get("attributes", {})


class _Request:
    def __init__(self, request_payload, user_payload):
        self.coerced_request = request_payload
        self.user = _User(user_payload)
        self.id = "remote-worker"


def _load_config(path):
    text = Path(path).read_text()
    if path.endswith(".json"):
        raw = json.loads(text)
    else:
        import yaml

        raw = yaml.safe_load(text)
    if "polytope" in raw:
        return raw["polytope"]
    return raw


_datasource = None
_config_path = None


def _get_datasource(config_path):
    global _datasource, _config_path
    if _datasource is None or _config_path != config_path:
        config = _load_config(config_path)
        from polytope import PolytopeDataSource

        _datasource = PolytopeDataSource(config)
        _config_path = config_path
    return _datasource


def process(payload_json):
    """Called from Rust via PyO3. Returns (output_bytes, timings_json)."""
    import traceback

    t0 = time.monotonic()

    payload = json.loads(payload_json)
    datasource = _get_datasource(payload["config_path"])
    t_init = time.monotonic()

    request = _Request(payload["request"], payload.get("user"))

    try:
        timings = datasource.retrieve(request)
        t_retrieve = time.monotonic()

        output = b"".join(
            chunk.encode("utf-8") if isinstance(chunk, str) else chunk
            for chunk in datasource.result(request)
        )
        t_result = time.monotonic()

        timings.update(
            {
                "init_ms": round((t_init - t0) * 1000, 1),
                "serialize_ms": round((t_result - t_retrieve) * 1000, 1),
                "total_ms": round((t_result - t0) * 1000, 1),
            }
        )
        return (output, json.dumps(timings))
    except Exception:
        raise RuntimeError(traceback.format_exc())
    finally:
        try:
            datasource.destroy(request)
        except Exception:
            pass


def main():
    payload = json.load(sys.stdin)
    config = _load_config(payload["config_path"])

    from polytope import PolytopeDataSource

    datasource = PolytopeDataSource(config)
    request = _Request(payload["request"], payload.get("user"))

    try:
        datasource.retrieve(request)
        for chunk in datasource.result(request):
            if isinstance(chunk, str):
                chunk = chunk.encode("utf-8")
            sys.stdout.buffer.write(chunk)
        sys.stdout.flush()
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)
    finally:
        try:
            datasource.destroy(request)
        except Exception:
            pass


if __name__ == "__main__":
    main()
