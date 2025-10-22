from unittest.mock import patch

import mongomock

from polytope_server.common.metric_store.mongodb_metric_store import MongoMetricStore

from .test_metric_store import _test_remove_old_metrics


def test_remove_old_metrics():
    mock_client = mongomock.MongoClient()
    mock_collection = mock_client.db.metrics

    # Patch pymongo.MongoClient at the source, not in the module
    with patch("pymongo.MongoClient") as mock_mongo_class:
        mock_mongo_class.return_value = mock_client

        # Now create the store - it will use the mocked client
        store = MongoMetricStore({})

        # Replace the store with mock collection
        store.store = mock_collection

        _test_remove_old_metrics(store)
