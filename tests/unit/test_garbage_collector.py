import os
from unittest import mock

import mongomock
import pytest
from moto import mock_aws

from polytope_server.common.request import PolytopeRequest
from polytope_server.common.request_store.dynamodb_request_store import (
    DynamoDBRequestStore,
)
from polytope_server.common.request_store.mongodb_request_store import MongoRequestStore
from polytope_server.garbage_collector.garbage_collector import GarbageCollector


class _DummyData:
    def __init__(self, name):
        self.name = name
        self.size = 0
        self.last_modified = 0


class _DummyStaging:
    def __init__(self, names):
        self.names = list(names)
        self.deleted = []

    def list(self):
        return [_DummyData(name) for name in self.names]

    def delete(self, name):
        if name not in self.names:
            raise KeyError
        self.deleted.append(name)
        self.names.remove(name)


class _DummyRequestStore:
    def __init__(self):
        self.calls = []

    def remove_old_requests(self, cutoff):
        self.calls.append(("remove_old_requests", cutoff))

    def remove_request(self, request_id):
        self.calls.append(("remove_request", request_id))

    def get_request_ids(self):  # pragma: no cover - not used here
        return []


class _DummyMetricStore:
    def __init__(self):
        self.calls = []

    def remove_old_metrics(self, cutoff):
        self.calls.append(("remove_old_metrics", cutoff))


@pytest.fixture(scope="function")
def mongo_store(monkeypatch):
    mock_client = mongomock.MongoClient()

    def fake_create_client(uri, username=None, password=None):
        return mock_client

    monkeypatch.setattr(
        "polytope_server.common.request_store.mongodb_request_store.mongo_client_factory.create_client",
        fake_create_client,
    )

    return MongoRequestStore({"uri": "mongodb://ignored", "collection": "requests"})


@pytest.fixture(scope="function")
def mocked_aws():
    values = {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_SECURITY_TOKEN": "testing",
        "AWS_SESSION_TOKEN": "testing",
        "AWS_DEFAULT_REGION": "us-east-1",
        "AWS_ENDPOINT_URL_DYNAMODB": "https://dynamodb.us-east-1.amazonaws.com",
    }
    with mock.patch.dict(os.environ, values):
        with mock_aws():
            yield


def _run_gc(request_store, staging):
    gc = GarbageCollector.__new__(GarbageCollector)
    gc.request_store = request_store
    gc.staging = staging
    gc.remove_dangling_data()
    return staging


def test_remove_dangling_data_mongo(mongo_store):
    keep_a = PolytopeRequest()
    keep_b = PolytopeRequest()
    mongo_store.add_request(keep_a)
    mongo_store.add_request(keep_b)

    staging = _DummyStaging([keep_a.id, f"{keep_b.id}.bin", "orphan-file"])

    _run_gc(mongo_store, staging)

    assert staging.deleted == ["orphan-file"]
    assert set(staging.names) == {keep_a.id, f"{keep_b.id}.bin"}


def test_remove_dangling_data_dynamo(mocked_aws):
    keep_a = PolytopeRequest()
    keep_b = PolytopeRequest()
    store = DynamoDBRequestStore()
    store.add_request(keep_a)
    store.add_request(keep_b)

    staging = _DummyStaging([f"{keep_a.id}.txt", keep_b.id, "dangling"])

    _run_gc(store, staging)

    assert staging.deleted == ["dangling"]
    assert set(staging.names) == {f"{keep_a.id}.txt", keep_b.id}


def test_remove_by_size_deletes_oldest_first():
    class Obj:
        def __init__(self, name, size, last_modified):
            self.name = name
            self.size = size
            self.last_modified = last_modified

    class Staging:
        def __init__(self, objs):
            self.objs = list(objs)
            self.deleted = []

        def list(self):
            return list(self.objs)

        def delete(self, name):
            self.deleted.append(name)
            self.objs = [o for o in self.objs if o.name != name]

    store = _DummyRequestStore()
    objs = [
        Obj("keep-newest", 40, 3),
        Obj("delete-oldest", 30, 1),
        Obj("delete-second", 50, 2),
    ]
    staging = Staging(objs)

    gc = GarbageCollector.__new__(GarbageCollector)
    gc.threshold = 80
    gc.staging = staging
    gc.request_store = store

    gc.remove_by_size()

    # total was 120 > 80 so deletes oldest (30) then second (50) to drop to 40
    assert staging.deleted == ["delete-oldest", "delete-second"]
    assert [call for call, _ in store.calls] == ["remove_request", "remove_request"]
    assert {req_id for _, req_id in store.calls} == {"delete-oldest", "delete-second"}
