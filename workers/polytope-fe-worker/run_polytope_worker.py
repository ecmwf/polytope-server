#!/usr/bin/env python3

import json
import sys
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
        return json.loads(text)
    import yaml

    return yaml.safe_load(text)


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
