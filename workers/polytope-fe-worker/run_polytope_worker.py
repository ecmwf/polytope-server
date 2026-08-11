#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import os
import sys
import threading
import time
import traceback
from pathlib import Path


# Thread-local storage for per-call log capture.
# Each worker thread (when worker_concurrency > 1) gets its own buffer.
_log_buffer = threading.local()

# Install-once guard for the persistent root logger handler
_handler_installed = False


class _LogCapturingHandler(logging.Handler):
    """Captures log records into a thread-local buffer during process() calls.

    Appends records as {"level": str, "logger": str, "message": str} dicts
    to the current thread's buffer, if one is set. Does nothing otherwise.
    """

    def emit(self, record):
        if not hasattr(_log_buffer, "records"):
            return
        try:
            message = record.getMessage()
        except Exception:
            message = str(record.msg)
        _log_buffer.records.append(
            {
                "level": record.levelname,
                "logger": record.name,
                "message": message,
            }
        )


def _python_log_level() -> int:
    """Parse RUST_LOG env var to determine Python logging level.

    Takes the FIRST bare token (no '=') from comma-separated RUST_LOG,
    lowercases it, and maps:
      trace/debug -> logging.DEBUG
      info        -> logging.INFO
      warn/warning-> logging.WARNING
      error       -> logging.ERROR
      off         -> logging.CRITICAL + 1
    Default: logging.INFO when unset or unknown.
    """
    rust_log = os.environ.get("RUST_LOG", "").strip()
    if not rust_log:
        return logging.INFO

    # Split by comma and find the first bare token (no '=')
    for token in rust_log.split(","):
        token = token.strip()
        if "=" not in token:
            token_lower = token.lower()
            if token_lower in ("trace", "debug"):
                return logging.DEBUG
            elif token_lower == "info":
                return logging.INFO
            elif token_lower in ("warn", "warning"):
                return logging.WARNING
            elif token_lower == "error":
                return logging.ERROR
            elif token_lower == "off":
                return logging.CRITICAL + 1
            break

    return logging.INFO


def _install_log_handler():
    """Install a persistent log handler on the root logger (idempotent)."""
    global _handler_installed
    if _handler_installed:
        return

    handler = _LogCapturingHandler()
    handler.setLevel(logging.DEBUG)  # Capture all levels; root logger controls filtering
    logging.root.addHandler(handler)
    _handler_installed = True


class _User:
    def __init__(self, payload):
        payload = payload or {}
        self.realm = payload.get("realm", "bits")
        self.username = payload.get("username", payload.get("id", "worker"))
        self.attributes = payload.get("attributes", {})


class _Request:
    def __init__(self, request_payload, user_payload, metadata_payload=None):
        self.coerced_request = request_payload
        self.user = _User(user_payload)
        self.metadata = metadata_payload if metadata_payload is not None else {}
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


def process(payload_json: str) -> tuple:
    """Called from Rust via PyO3.

    Returns: (body_bytes: bytes, status_json: str)

    status_json is json.dumps(...) of an object with:
      - ok (bool)
      - timings (object) — the existing timings dict when ok=true; {} otherwise
      - logs (array) — list of {"level": str, "logger": str, "message": str}
        for every Python log record emitted during THIS call.
      - error — null when ok=true; when ok=false, {"message": str} with a
        clean message (never a traceback).

    On success: ok=true, body_bytes = output bytes, timings populated, error=null.
    On job-level exception: ok=false, body_bytes=b"", error.message = clean message,
      and the full traceback is appended to logs as a synthetic ERROR record.

    process() may still raise only for catastrophic/protocol errors (e.g.
    json.loads(payload_json) failing) — leave that unhandled as today.
    """
    # Install the log handler idempotently and set the level
    _install_log_handler()
    log_level = _python_log_level()
    logging.root.setLevel(log_level)

    # Initialize per-call log buffer for this thread
    _log_buffer.records = []

    t0 = time.monotonic()

    # Payload parsing may raise — let that propagate as a catastrophic error
    payload = json.loads(payload_json)
    datasource = _get_datasource(payload["config_path"])
    t_init = time.monotonic()

    request = _Request(
        payload["request"],
        payload.get("user"),
        payload.get("metadata", {})
    )

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

        # Success path: ok=true, logs collected, error=null
        logs = _log_buffer.records
        _log_buffer.records = []
        status = {
            "ok": True,
            "timings": timings,
            "logs": logs,
            "error": None,
        }
        return (output, json.dumps(status))

    except Exception as exc:
        # Job-level exception: ok=false, clean message in error.message,
        # full traceback appended to logs as a synthetic ERROR record.
        clean_message = getattr(exc, "message", None) or str(exc) or exc.__class__.__name__
        full_traceback = traceback.format_exc()

        # Append the traceback as a synthetic log record
        _log_buffer.records.append(
            {
                "level": "ERROR",
                "logger": "polytope-fe-worker",
                "message": full_traceback,
            }
        )

        logs = _log_buffer.records
        _log_buffer.records = []
        status = {
            "ok": False,
            "timings": {},
            "logs": logs,
            "error": {"message": str(clean_message)},
        }
        return (b"", json.dumps(status))

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
    request = _Request(
        payload["request"],
        payload.get("user"),
        payload.get("metadata", {})
    )

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
