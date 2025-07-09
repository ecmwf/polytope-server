import mongomock

from polytope_server.common.metric_store.mongodb_metric_store import MongoMetricStore

from .test_metric_store import _test_remove_old_metrics


def test_remove_old_metrics():
    store = MongoMetricStore({})
    store.store = mongomock.MongoClient().db.metrics
    _test_remove_old_metrics(store)
