"""
Throwaway inspection test for Flask request JSON shape.

Run with:
  pytest --noconftest tests/unit/test_payload_shape_throwaway.py -s -q
"""

import json
import sys
import types
from unittest.mock import MagicMock

import pytest
import yaml
from flask import Flask

from polytope_server.common.user import User
from polytope_server.frontend.common.data_transfer import DataTransfer

# Stub out polytope_server.frontend package __init__ to avoid opentelemetry dependency.
_frontend_pkg = types.ModuleType("polytope_server.frontend")
_frontend_pkg.__path__ = ["polytope_server/frontend"]
_frontend_pkg.__package__ = "polytope_server.frontend"
sys.modules.setdefault("polytope_server.frontend", _frontend_pkg)


@pytest.fixture
def app():
    flask_app = Flask(__name__)
    flask_app.config["TESTING"] = True
    return flask_app


@pytest.fixture
def user():
    return User(username="testuser", realm="testrealm")


@pytest.fixture
def mock_request_store():
    store = MagicMock()
    store.add_request.return_value = None
    return store


@pytest.fixture
def dt(mock_request_store):
    return DataTransfer(mock_request_store, MagicMock())


@pytest.mark.parametrize(
    "label,payload",
    [
        (
            "dict-request",
            {"request": {"date": "20230101", "param": "130", "optional": None}},
        ),
        (
            "yaml-string-request",
            {"request": "date: 20230101\nparam: 130\noptional: null"},
        ),
        (
            "plain-string-request",
            {"request": "some plain string"},
        ),
    ],
)
def test_inspect_payload_runtime_shape(label, payload, app, dt, user, mock_request_store):
    with app.test_request_context(
        "/",
        method="POST",
        data=json.dumps(payload),
        content_type="application/json",
    ):
        from flask import request as flask_request

        runtime_payload = flask_request.json
        runtime_request = runtime_payload["request"]
        stringified = str(runtime_request)
        roundtripped = yaml.safe_load(stringified)

        print(f"\n=== {label} ===")
        print(f"http_request.json type: {type(runtime_payload).__name__}")
        print(f"http_request.json value: {runtime_payload!r}")
        print(f"payload['request'] type: {type(runtime_request).__name__}")
        print(f"payload['request'] value: {runtime_request!r}")
        print(f"str(payload['request']) type: {type(stringified).__name__}")
        print(f"str(payload['request']) value: {stringified!r}")
        print(f"yaml.safe_load(str(payload['request'])) type: {type(roundtripped).__name__}")
        print(f"yaml.safe_load(str(payload['request'])) value: {roundtripped!r}")

        dt.request_download(flask_request, user, "test-collection")

    persisted = mock_request_store.add_request.call_args[0][0]
    print(f"persisted.user_request: {persisted.user_request!r}")
    print(f"persisted.coerced_request: {persisted.coerced_request!r}")

    assert True
