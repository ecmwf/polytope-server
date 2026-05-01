import logging

import mongomock
import pytest

from polytope_server.common.request import PolytopeRequest, Status
from polytope_server.common.request_store.mongodb_request_store import MongoRequestStore
from polytope_server.common.user import User

from .test_request_store import (
    _test_get_active_requests,
    _test_get_request_ids,
    _test_remove_old_requests,
    _test_remove_requests,
    _test_revoke_request,
    _test_update_request,
)


@pytest.fixture(scope="function")
def mongomock_request_store(monkeypatch):
    """Fixture to create a MongoRequestStore backed entirely by mongomock."""

    mock_client = mongomock.MongoClient()

    # Make MongoRequestStore (and anything else using mongo_client_factory)
    # use our in-memory mongomock client instead of a real MongoDB.
    def fake_create_client(uri, username=None, password=None):
        return mock_client

    monkeypatch.setattr(
        "polytope_server.common.request_store.mongodb_request_store.mongo_client_factory.create_client",
        fake_create_client,
    )

    # Now this will use mongomock under the hood
    store = MongoRequestStore({"uri": "mongodb://ignored", "collection": "requests"})
    yield store


def test_revoke_request(mongomock_request_store):
    _test_revoke_request(mongomock_request_store)


def test_update_request(mongomock_request_store):
    _test_update_request(mongomock_request_store)


def test_remove_old_requests(mongomock_request_store):
    _test_remove_old_requests(mongomock_request_store)


def test_get_active_requests(mongomock_request_store):
    _test_get_active_requests(mongomock_request_store)


def test_get_request_ids(mongomock_request_store):
    _test_get_request_ids(mongomock_request_store)


def test_remove_requests(mongomock_request_store):
    _test_remove_requests(mongomock_request_store)


def test_add_request_logs_serialize_logging_payload(mongomock_request_store, caplog):
    req = PolytopeRequest(
        user=User("test-user", "test-realm"),
        collection="test-collection",
        status=Status.WAITING,
        user_request="raw-user-request",
    )
    req.coerced_request = {"param": "167"}

    caplog.set_level(logging.INFO)

    mongomock_request_store.add_request(req)

    record = next(r for r in caplog.records if "added to request store" in r.getMessage())
    assert record.getMessage() == f"Request ID {req.id} added to request store."
    assert record.__dict__["request"] == req.serialize_logging()


def test_update_request_logs_serialize_logging_payload(mongomock_request_store, caplog):
    req = PolytopeRequest(
        user=User("test-user", "test-realm"),
        collection="test-collection",
        status=Status.WAITING,
        user_request="raw-user-request",
    )
    req.coerced_request = {"param": "167"}
    mongomock_request_store.add_request(req)
    caplog.clear()

    caplog.set_level(logging.INFO)
    req.status = Status.PROCESSED
    mongomock_request_store.update_request(req)

    record = next(r for r in caplog.records if "updated on request store" in r.getMessage())
    assert record.__dict__["request"] == req.serialize_logging()
