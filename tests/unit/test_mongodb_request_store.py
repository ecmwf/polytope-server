import mongomock
import pytest

from polytope_server.common.request_store.mongodb_request_store import MongoRequestStore

from .test_request_store import (
    _test_remove_old_requests,
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
