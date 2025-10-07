import mongomock
import pytest

from polytope_server.common.request_store.mongodb_request_store import MongoRequestStore

from .test_request_store import (
    _test_remove_old_requests,
    _test_revoke_request,
    _test_update_request,
)


@pytest.fixture(scope="function")
def mongomock_request_store():
    """Fixture to create a mocked MongoRequestStore."""
    store = MongoRequestStore({})
    store.store = mongomock.MongoClient().db.requests
    yield store


def test_revoke_request(mongomock_request_store):
    _test_revoke_request(mongomock_request_store)


def test_update_request(mongomock_request_store):
    _test_update_request(mongomock_request_store)


def test_remove_old_requests(mongomock_request_store):
    _test_remove_old_requests(mongomock_request_store)
