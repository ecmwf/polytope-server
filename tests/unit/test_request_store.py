import pytest

from polytope_server.common import exceptions, request, user


def _test_revoke_request(store):
    # Create a test user
    test_user = user.User("test-user", "test-realm")
    # test queued request gets removed
    req_queued = request.Request(status=request.Status.QUEUED, user=test_user)
    store.add_request(req_queued)
    assert store.get_request(req_queued.id) is not None
    store.revoke_request(test_user, req_queued.id)
    assert store.get_request(req_queued.id) is None

    # test processed request does not get removed and raises KeyError about status
    req_processed = request.Request(status=request.Status.PROCESSED, user=test_user)
    store.add_request(req_processed)
    assert store.get_request(req_processed.id) is not None
    with pytest.raises(exceptions.ForbiddenRequest):
        store.revoke_request(test_user, req_processed.id)
    assert store.get_request(req_processed.id) is not None

    # test request from a different user raises UnauthorizedRequest
    other_user = user.User("other-user", "other-realm")
    req_other = request.Request(status=request.Status.QUEUED, user=other_user)
    store.add_request(req_other)
    assert store.get_request(req_other.id) is not None
    with pytest.raises(exceptions.UnauthorizedRequest):
        store.revoke_request(test_user, req_other.id)
    # test non-existing request raises KeyError
    with pytest.raises(exceptions.NotFound):
        store.revoke_request(test_user, "non-existing-id")
