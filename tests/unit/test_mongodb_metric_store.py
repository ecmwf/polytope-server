from unittest.mock import MagicMock

import mongomock
from polytope_server.common.metric_store.mongodb_metric_store import MongoMetricStore

from .test_metric_store import _test_remove_old_metrics


def test_remove_old_metrics():
    # Create store with mocked _ensure_indexes to prevent real DB connection
    store = MongoMetricStore.__new__(MongoMetricStore)

    # Manually set up the mock store
    mock_client = mongomock.MongoClient()
    store.store = mock_client.db.metrics

    # Skip the initialization that tries to connect to real MongoDB
    store._ensure_indexes = MagicMock()

    _test_remove_old_metrics(store)
