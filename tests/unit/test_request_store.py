import datetime

import pytest

from polytope_server.common import exceptions, request, user


def _test_revoke_request(store):
    # Create a test user
    test_user = user.User("test-user", "test-realm")
    # test queued request gets removed
    req_queued = request.PolytopeRequest(status=request.Status.QUEUED, user=test_user)
    store.add_request(req_queued)
    assert store.get_request(req_queued.id) is not None
    store.revoke_request(test_user, req_queued.id)
    assert store.get_request(req_queued.id) is None

    # test processed request does not get removed and raises KeyError about status
    req_processed = request.PolytopeRequest(status=request.Status.PROCESSED, user=test_user)
    store.add_request(req_processed)

    assert store.get_request(req_processed.id) is not None
    with pytest.raises(exceptions.ForbiddenRequest):
        store.revoke_request(test_user, req_processed.id)
    assert store.get_request(req_processed.id) is not None

    # test request from a different user raises UnauthorizedRequest
    other_user = user.User("other-user", "other-realm")
    req_other = request.PolytopeRequest(status=request.Status.QUEUED, user=other_user)
    store.add_request(req_other)
    assert store.get_request(req_other.id) is not None
    with pytest.raises(exceptions.UnauthorizedRequest):
        store.revoke_request(test_user, req_other.id)

    # test non-existing request raises KeyError
    with pytest.raises(exceptions.NotFound):
        store.revoke_request(test_user, "non-existing-id")

    # test revoking all requests of the user
    req1 = request.PolytopeRequest(status=request.Status.WAITING, user=test_user)
    req2 = request.PolytopeRequest(status=request.Status.QUEUED, user=test_user)
    store.add_request(req1)
    store.add_request(req2)
    assert store.get_request(req1.id) is not None
    assert store.get_request(req2.id) is not None
    deleted_count = store.revoke_request(test_user, "all")
    assert deleted_count == 2
    assert store.get_request(req_processed.id) is not None
    assert store.get_request(req_other.id) is not None
    assert store.get_request(req1.id) is None
    assert store.get_request(req2.id) is None


def _test_update_request(store):
    # Create a test user
    test_user = user.User("test-user", "test-realm")
    # Create a request
    req = request.PolytopeRequest(user=test_user, collection="test-collection", status=request.Status.WAITING)
    store.add_request(req)

    # Update the request
    req.status = request.Status.PROCESSED
    store.update_request(req)

    # Retrieve the updated request and check its status
    updated_req = store.get_request(req.id)
    assert updated_req.status == request.Status.PROCESSED

    # Test updating a non-existing request raises NotFound
    non_existing_req = request.PolytopeRequest(id="non-existing-id", user=test_user)
    with pytest.raises(exceptions.NotFound):
        store.update_request(non_existing_req)


def _test_remove_old_requests(store):
    # Create a test user
    test_user = user.User("test-user", "test-realm")
    # Create requests with different statuses and timestamps
    old_failed_req = request.PolytopeRequest(
        user=test_user, status=request.Status.FAILED, last_modified=datetime.datetime(2000, 1, 1).timestamp()
    )
    store.add_request(old_failed_req)
    old_processing_req = request.PolytopeRequest(
        user=test_user, status=request.Status.PROCESSING, last_modified=datetime.datetime(2000, 1, 1).timestamp()
    )
    store.add_request(old_processing_req)
    recent_processed_req = request.PolytopeRequest(
        user=test_user, status=request.Status.PROCESSED, last_modified=datetime.datetime.now().timestamp()
    )
    store.add_request(recent_processed_req)
    # Remove old requests
    cutoff = datetime.datetime(2001, 1, 1)  # Cutoff date for old requests
    removed_count = store.remove_old_requests(cutoff)
    # Check that only the old requests were removed
    assert removed_count == 1
    assert store.get_request(old_failed_req.id) is None
    assert store.get_request(old_processing_req.id) is not None
    assert store.get_request(recent_processed_req.id) is not None
