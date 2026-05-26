"""
Unit tests for DataTransfer coercion-at-insertion path.
Run with: pytest --noconftest tests/unit/test_data_transfer.py -q
"""

import json
import sys
import types
from unittest.mock import MagicMock

import pytest
from flask import Flask

from polytope_server.common.exceptions import BadRequest
from polytope_server.common.user import User
from polytope_server.frontend.common.data_transfer import DataTransfer

# Stub out polytope_server.frontend package __init__ to avoid opentelemetry dependency.
# We only need polytope_server.frontend.common.data_transfer, not the full Frontend class.
_frontend_pkg = types.ModuleType("polytope_server.frontend")
_frontend_pkg.__path__ = ["polytope_server/frontend"]  # make it a package
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
def mock_staging():
    return MagicMock()


@pytest.fixture
def dt(mock_request_store, mock_staging):
    return DataTransfer(mock_request_store, mock_staging)


def make_http_request(app, payload):
    """Create a Flask Request object with JSON payload."""
    with app.test_request_context(
        "/",
        method="POST",
        data=json.dumps(payload),
        content_type="application/json",
    ) as _:
        from flask import request as flask_request

        return flask_request._get_current_object()


class TestRequestDownload:
    def test_coerced_request_populated(self, app, dt, user, mock_request_store):
        """request_download populates coerced_request on the persisted request."""
        payload = {"request": {"date": "20230101", "param": "130"}}
        with app.test_request_context(
            "/",
            method="POST",
            data=json.dumps(payload),
            content_type="application/json",
        ):
            from flask import request as flask_request

            dt.request_download(flask_request, user, "test-collection")

        mock_request_store.add_request.assert_called_once()
        persisted = mock_request_store.add_request.call_args[0][0]
        assert persisted.coerced_request is not None
        assert isinstance(persisted.coerced_request, dict)
        assert "date" in persisted.coerced_request

    def test_invalid_date_raises_bad_request(self, app, dt, user, mock_request_store):
        """request_download with invalid date raises BadRequest and does NOT call add_request."""
        payload = {"request": {"date": "not-a-date"}}
        with app.test_request_context(
            "/",
            method="POST",
            data=json.dumps(payload),
            content_type="application/json",
        ):
            from flask import request as flask_request

            with pytest.raises(BadRequest):
                dt.request_download(flask_request, user, "test-collection")

        mock_request_store.add_request.assert_not_called()

    def test_malformed_yaml_raises_bad_request(self, app, dt, user, mock_request_store):
        """Malformed YAML in request string raises BadRequest and does NOT call add_request."""
        # Pass a raw string that is invalid YAML (e.g. unbalanced braces)
        payload = {"request": "{bad: yaml: [unclosed"}
        with app.test_request_context(
            "/",
            method="POST",
            data=json.dumps(payload),
            content_type="application/json",
        ):
            from flask import request as flask_request

            with pytest.raises(BadRequest):
                dt.request_download(flask_request, user, "test-collection")

        mock_request_store.add_request.assert_not_called()

    def test_non_dict_payload_wraps_in_data(self, app, dt, user, mock_request_store):
        """request_download with a non-dict payload (plain string) wraps it in {'data': ...}."""
        payload = {"request": "some plain string"}
        with app.test_request_context(
            "/",
            method="POST",
            data=json.dumps(payload),
            content_type="application/json",
        ):
            from flask import request as flask_request

            dt.request_download(flask_request, user, "test-collection")

        mock_request_store.add_request.assert_called_once()
        persisted = mock_request_store.add_request.call_args[0][0]
        assert persisted.coerced_request == {"data": "some plain string"}


class TestRequestUpload:
    def test_coerced_request_populated(self, app, dt, user, mock_request_store):
        """request_upload populates coerced_request on the persisted request."""
        payload = {"request": {"date": "20230101", "param": "130"}}
        with app.test_request_context(
            "/",
            method="POST",
            data=json.dumps(payload),
            content_type="application/json",
        ):
            from flask import request as flask_request

            dt.request_upload(flask_request, user, "test-collection")

        mock_request_store.add_request.assert_called_once()
        persisted = mock_request_store.add_request.call_args[0][0]
        assert persisted.coerced_request is not None
        assert isinstance(persisted.coerced_request, dict)
        assert "date" in persisted.coerced_request

    def test_non_dict_payload_wraps_in_data(self, app, dt, user, mock_request_store):
        """request_upload with a non-dict payload wraps it in {'data': ...}."""
        payload = {"request": 42}
        with app.test_request_context(
            "/",
            method="POST",
            data=json.dumps(payload),
            content_type="application/json",
        ):
            from flask import request as flask_request

            dt.request_upload(flask_request, user, "test-collection")

        mock_request_store.add_request.assert_called_once()
        persisted = mock_request_store.add_request.call_args[0][0]
        assert persisted.coerced_request == {"data": 42}
