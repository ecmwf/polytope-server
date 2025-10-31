from datetime import datetime, timedelta

from polytope_server.common.metric import RequestStatusChange
from polytope_server.common.metric_store.metric_store import MetricStore
from polytope_server.common.request import Status


def _test_remove_old_metrics(store: MetricStore):
    """Test that old metrics are deleted correctly."""

    # Create a metric older than the cutoff
    old_metric = RequestStatusChange(
        status=Status.PROCESSED,
        request_id="test-request-1",
        user_id="test-user-1",
        timestamp=(datetime.now() - timedelta(days=10)).timestamp(),
    )
    store.add_metric(old_metric)

    assert store.get_metric(old_metric.uuid) is not None

    # Create a metric newer than the cutoff
    new_metric = RequestStatusChange(
        status=Status.PROCESSED,
        request_id="test-request-2",
        user_id="test-user-2",
        timestamp=(datetime.now().timestamp()),
    )
    store.add_metric(new_metric)

    assert store.get_metric(new_metric.uuid) is not None

    # Set the cutoff to 5 days ago
    cutoff = datetime.now() - timedelta(days=5)

    # Remove old metrics
    deleted_count = store.remove_old_metrics(cutoff)

    # Verify that only the old metric was deleted
    assert store.get_metric(old_metric.uuid) is None
    assert store.get_metric(new_metric.uuid) is not None
    assert deleted_count == 1
