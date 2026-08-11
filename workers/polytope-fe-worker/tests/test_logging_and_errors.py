# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

"""
Test logging capture and error handling in run_polytope_worker.

Proves that:
1. Success path captures Python logs and returns ok=true with body bytes
2. Error path captures clean message in error.message and traceback in logs
3. Exception .message attribute is preferred over str(exc) when present
4. _python_log_level() correctly parses RUST_LOG environment variable
"""

import json
import logging
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import run_polytope_worker


class FakeDatasource:
    """Fake datasource for testing without polytope_mars/FDB dependencies."""

    def __init__(self, retrieve_behavior=None, result_behavior=None):
        """
        Args:
            retrieve_behavior: callable(request) -> timings dict, or raises
            result_behavior: callable(request) -> iterable of chunks
        """
        self.retrieve_behavior = retrieve_behavior
        self.result_behavior = result_behavior

    def retrieve(self, request):
        if self.retrieve_behavior:
            return self.retrieve_behavior(request)
        return {"test_ms": 1.0}

    def result(self, request):
        if self.result_behavior:
            return self.result_behavior(request)
        return [b"test output"]

    def destroy(self, request):
        pass


class ExceptionWithMessage(Exception):
    """Exception class with a .message attribute but empty str()."""

    def __init__(self, message):
        self.message = message
        super().__init__()  # Don't pass message to super, so str(exc) is empty


@pytest.fixture
def reset_log_buffer():
    """Reset thread-local log buffer between tests to prevent cross-contamination."""
    # Clear any existing buffer
    if hasattr(run_polytope_worker._log_buffer, "records"):
        delattr(run_polytope_worker._log_buffer, "records")
    yield
    # Clear again after test
    if hasattr(run_polytope_worker._log_buffer, "records"):
        delattr(run_polytope_worker._log_buffer, "records")


def test_success_path_captures_logs(monkeypatch, tmp_path, reset_log_buffer):
    """Success: fake datasource emits logs, returns timings and body bytes."""

    def retrieve_with_logs(request):
        logging.info("Starting retrieval")
        logging.warning("This is a warning during retrieval")
        return {"retrieve_ms": 123.4}

    def result_with_logs(request):
        logging.debug("Generating result chunks")
        yield b"chunk1"
        yield b"chunk2"

    fake_ds = FakeDatasource(
        retrieve_behavior=retrieve_with_logs,
        result_behavior=result_with_logs,
    )

    monkeypatch.setattr(
        run_polytope_worker, "_get_datasource", lambda config_path: fake_ds
    )

    # Create minimal config file
    config_file = tmp_path / "config.yaml"
    config_file.write_text("type: polytope")

    payload = {
        "config_path": str(config_file),
        "request": {"class": "od"},
        "user": {"username": "test"},
        "metadata": {},
    }

    body_bytes, status_json = run_polytope_worker.process(json.dumps(payload))
    status = json.loads(status_json)

    # Assert success
    assert status["ok"] is True
    assert body_bytes == b"chunk1chunk2"
    assert status["error"] is None
    assert "timings" in status
    assert status["timings"]["retrieve_ms"] == 123.4

    # Assert logs were captured (at least the info and warning)
    assert "logs" in status
    logs = status["logs"]
    assert len(logs) >= 2

    # Find the specific logs we emitted
    log_messages = [log["message"] for log in logs]
    assert "Starting retrieval" in log_messages
    assert "This is a warning during retrieval" in log_messages

    # Check log levels
    info_log = [log for log in logs if "Starting retrieval" in log["message"]][0]
    assert info_log["level"] == "INFO"

    warning_log = [
        log for log in logs if "This is a warning during retrieval" in log["message"]
    ][0]
    assert warning_log["level"] == "WARNING"


def test_error_path_clean_message_traceback_in_logs(
    monkeypatch, tmp_path, reset_log_buffer
):
    """Error: fake datasource raises ValueError, assert clean message and traceback in logs."""

    def retrieve_raises(request):
        raise ValueError("boom")

    fake_ds = FakeDatasource(retrieve_behavior=retrieve_raises)

    monkeypatch.setattr(
        run_polytope_worker, "_get_datasource", lambda config_path: fake_ds
    )

    config_file = tmp_path / "config.yaml"
    config_file.write_text("type: polytope")

    payload = {
        "config_path": str(config_file),
        "request": {"class": "od"},
        "user": {"username": "test"},
        "metadata": {},
    }

    body_bytes, status_json = run_polytope_worker.process(json.dumps(payload))
    status = json.loads(status_json)

    # Assert failure
    assert status["ok"] is False
    assert body_bytes == b""
    assert status["timings"] == {}

    # Assert clean error message (no traceback)
    assert status["error"] is not None
    assert status["error"]["message"] == "boom"
    assert "Traceback" not in status["error"]["message"]

    # Assert traceback is in logs
    assert "logs" in status
    logs = status["logs"]
    assert len(logs) >= 1

    # Find the ERROR log with the traceback
    error_logs = [log for log in logs if log["level"] == "ERROR"]
    assert len(error_logs) >= 1

    traceback_log = error_logs[-1]  # The synthetic traceback log
    assert "Traceback" in traceback_log["message"]
    assert "ValueError" in traceback_log["message"]
    assert "boom" in traceback_log["message"]
    assert traceback_log["logger"] == "polytope-fe-worker"


def test_exception_with_message_attribute(monkeypatch, tmp_path, reset_log_buffer):
    """Error: exception with .message attribute but empty str() -> use .message."""

    def retrieve_raises_message_exc(request):
        raise ExceptionWithMessage("custom message attribute")

    fake_ds = FakeDatasource(retrieve_behavior=retrieve_raises_message_exc)

    monkeypatch.setattr(
        run_polytope_worker, "_get_datasource", lambda config_path: fake_ds
    )

    config_file = tmp_path / "config.yaml"
    config_file.write_text("type: polytope")

    payload = {
        "config_path": str(config_file),
        "request": {"class": "od"},
        "user": {"username": "test"},
        "metadata": {},
    }

    body_bytes, status_json = run_polytope_worker.process(json.dumps(payload))
    status = json.loads(status_json)

    # Assert failure with clean message from .message attribute
    assert status["ok"] is False
    assert status["error"]["message"] == "custom message attribute"


def test_python_log_level_empty(monkeypatch):
    """Test _python_log_level with empty/unset RUST_LOG -> INFO."""
    monkeypatch.setenv("RUST_LOG", "")
    assert run_polytope_worker._python_log_level() == logging.INFO

    monkeypatch.delenv("RUST_LOG", raising=False)
    assert run_polytope_worker._python_log_level() == logging.INFO


def test_python_log_level_debug(monkeypatch):
    """Test _python_log_level with 'debug' -> DEBUG."""
    monkeypatch.setenv("RUST_LOG", "debug")
    assert run_polytope_worker._python_log_level() == logging.DEBUG


def test_python_log_level_trace(monkeypatch):
    """Test _python_log_level with 'trace' -> DEBUG."""
    monkeypatch.setenv("RUST_LOG", "trace")
    assert run_polytope_worker._python_log_level() == logging.DEBUG


def test_python_log_level_info(monkeypatch):
    """Test _python_log_level with 'info' -> INFO."""
    monkeypatch.setenv("RUST_LOG", "info")
    assert run_polytope_worker._python_log_level() == logging.INFO


def test_python_log_level_warn(monkeypatch):
    """Test _python_log_level with 'warn' -> WARNING."""
    monkeypatch.setenv("RUST_LOG", "warn")
    assert run_polytope_worker._python_log_level() == logging.WARNING


def test_python_log_level_warning(monkeypatch):
    """Test _python_log_level with 'warning' -> WARNING."""
    monkeypatch.setenv("RUST_LOG", "warning")
    assert run_polytope_worker._python_log_level() == logging.WARNING


def test_python_log_level_error(monkeypatch):
    """Test _python_log_level with 'error' -> ERROR."""
    monkeypatch.setenv("RUST_LOG", "error")
    assert run_polytope_worker._python_log_level() == logging.ERROR


def test_python_log_level_off(monkeypatch):
    """Test _python_log_level with 'off' -> CRITICAL + 1."""
    monkeypatch.setenv("RUST_LOG", "off")
    assert run_polytope_worker._python_log_level() == logging.CRITICAL + 1


def test_python_log_level_first_bare_token(monkeypatch):
    """Test _python_log_level with comma-separated RUST_LOG -> first bare token."""
    monkeypatch.setenv("RUST_LOG", "warn,hyper=off,tower=debug")
    assert run_polytope_worker._python_log_level() == logging.WARNING


def test_python_log_level_only_crate_specific(monkeypatch):
    """Test _python_log_level with only crate-specific tokens -> default INFO."""
    monkeypatch.setenv("RUST_LOG", "hyper=debug,tower=warn")
    assert run_polytope_worker._python_log_level() == logging.INFO


def test_python_log_level_unknown_token(monkeypatch):
    """Test _python_log_level with unknown first token -> default INFO."""
    monkeypatch.setenv("RUST_LOG", "unknown_level")
    assert run_polytope_worker._python_log_level() == logging.INFO


def test_log_handler_idempotent(reset_log_buffer):
    """Test that _install_log_handler can be called multiple times safely."""
    # Get initial handler count
    initial_handler_count = len(logging.root.handlers)

    # Install handler multiple times
    run_polytope_worker._install_log_handler()
    run_polytope_worker._install_log_handler()
    run_polytope_worker._install_log_handler()

    # Should only add one handler
    # (Note: depending on test order, there might be 1 existing handler from previous tests)
    assert len(logging.root.handlers) <= initial_handler_count + 1


def test_logs_captured_only_during_process_call(
    monkeypatch, tmp_path, reset_log_buffer
):
    """Test that logs outside process() calls are not captured."""

    def retrieve_with_log(request):
        logging.info("Inside process call")
        return {"test_ms": 1.0}

    fake_ds = FakeDatasource(retrieve_behavior=retrieve_with_log)
    monkeypatch.setattr(
        run_polytope_worker, "_get_datasource", lambda config_path: fake_ds
    )

    config_file = tmp_path / "config.yaml"
    config_file.write_text("type: polytope")

    # Log outside of process() call
    logging.info("Outside process call")

    payload = {
        "config_path": str(config_file),
        "request": {"class": "od"},
        "user": {"username": "test"},
        "metadata": {},
    }

    body_bytes, status_json = run_polytope_worker.process(json.dumps(payload))
    status = json.loads(status_json)

    # Assert only the log inside process() was captured
    logs = status["logs"]
    log_messages = [log["message"] for log in logs]
    assert "Inside process call" in log_messages
    assert "Outside process call" not in log_messages


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
