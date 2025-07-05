import mongomock

from polytope_server.common.request_store.mongodb_request_store import MongoRequestStore

from .test_request_store import _test_revoke_request


def test_revoke_request():
    # Create a mocked MongoRequestStore
    store = MongoRequestStore({})
    store.store = mongomock.MongoClient().db.requests

    _test_revoke_request(store)
