import asyncio as aio
from concurrent.futures import ThreadPoolExecutor

import pytest

import polytope_server.worker.worker as worker_module
from polytope_server.common.request import PolytopeRequest, Status


class FakeQueueMessage:
    def __init__(self, request_id):
        self.body = {"id": request_id}


class FakeQueue:
    def __init__(self, request_id):
        self.message = FakeQueueMessage(request_id)
        self.acked = []
        self.nacked = []
        self.dequeued = False

    def dequeue(self):
        if self.dequeued:
            return None
        self.dequeued = True
        return self.message

    def ack(self, message):
        self.acked.append(message)

    def nack(self, message):
        self.nacked.append(message)


class FakeRequestStore:
    def __init__(self, request):
        self.request = request
        self.updated = []

    def get_request(self, request_id):
        assert request_id == self.request.id
        return self.request

    def set_request_status(self, request, status):
        request.status = status

    def update_request(self, request):
        self.updated.append(request)


def test_listen_queue_clears_request_state_before_graceful_termination(monkeypatch):
    monkeypatch.setattr(worker_module.collection, "create_collections", lambda config: {})
    monkeypatch.setattr(worker_module.staging, "create_staging", lambda config: object())
    monkeypatch.setattr(worker_module.request_store, "create_request_store", lambda *args, **kwargs: None)

    worker = worker_module.Worker(
        {
            "worker": {},
            "collections": {},
            "staging": {},
            "request_store": {},
            "metric_store": {},
        }
    )
    request = PolytopeRequest()
    request.status = Status.QUEUED
    worker.queue = FakeQueue(request.id)
    worker.request_store = FakeRequestStore(request)
    monkeypatch.setattr(worker, "process_request", lambda request: None)

    with ThreadPoolExecutor(max_workers=1) as executor:
        with pytest.raises(worker_module.TaskGroupTermination):
            aio.run(worker.listen_queue(executor))

    assert worker.request is None
    assert worker.queue_msg is None


def test_on_process_terminated_reschedules_inflight_processing_request(monkeypatch):
    monkeypatch.setattr(worker_module.collection, "create_collections", lambda config: {})
    monkeypatch.setattr(worker_module.staging, "create_staging", lambda config: object())
    monkeypatch.setattr(worker_module.request_store, "create_request_store", lambda *args, **kwargs: None)

    worker = worker_module.Worker(
        {
            "worker": {},
            "collections": {},
            "staging": {},
            "request_store": {},
            "metric_store": {},
        }
    )
    request = PolytopeRequest()
    request.status = Status.PROCESSING
    queue = FakeQueue(request.id)
    worker.queue = queue
    worker.queue_msg = queue.message
    worker.request = request
    worker.request_store = FakeRequestStore(request)

    worker.on_process_terminated()

    assert request.status == Status.QUEUED
    assert queue.nacked == [queue.message]
