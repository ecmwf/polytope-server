#
# Copyright 2022 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
#

import logging

import pymongo

from .. import mongo_client_factory
from ..metric import (
    CacheInfo,
    Metric,
    MetricType,
    QueueInfo,
    RequestStatusChange,
    StorageInfo,
    WorkerInfo,
    WorkerStatusChange,
)
from ..metric_collector import MongoStorageMetricCollector
from . import MetricStore


class MongoMetricStore(MetricStore):
    def __init__(self, config=None):
        if config is None:
            config = {}

        uri = config.get("uri", "mongodb://localhost:27017")
        metric_collection = config.get("collection", "metrics")

        log_level = config.get("log_level", logging.WARNING)
        logging.getLogger("pymongo").setLevel(log_level)

        username = config.get("username")
        password = config.get("password")

        self.mongo_client = mongo_client_factory.create_client(uri, username, password)
        self.database = self.mongo_client.metric_store
        self.store = self.database[metric_collection]

        self.metric_type_class_map = {
            MetricType.WORKER_STATUS_CHANGE: WorkerStatusChange,
            MetricType.WORKER_INFO: WorkerInfo,
            MetricType.REQUEST_STATUS_CHANGE: RequestStatusChange,
            MetricType.STORAGE_INFO: StorageInfo,
            MetricType.CACHE_INFO: CacheInfo,
            MetricType.QUEUE_INFO: QueueInfo,
        }

        self.storage_metric_collector = MongoStorageMetricCollector(
            uri, self.mongo_client, "metric_store", metric_collection
        )

        logging.info("MongoClient configured to open at {}".format(uri))

    def get_type(self):
        return "mongodb"

    def add_metric(self, metric):
        if self.get_metric(metric.uuid) is not None:
            raise ValueError("Metric already exists in metric store")
        self.store.insert_one(metric.serialize())

    def remove_metric(self, uuid, include_processed=False):
        """
        Removes a metric with the given UUID. By default, it skips entries with status 'processed'.
        """
        # Find the document
        metric = self.store.find_one({"uuid": uuid})
        if metric is None:
            raise KeyError("Metric does not exist in request store")

        # Skip removal if the status is 'processed' and include_processed is False
        if metric["status"] == "processed" and not include_processed:
            # Log skipping for better traceability
            logging.info(f"Skipping removal of metric with UUID {uuid} as it has status 'processed'")
            return

        # Delete the metric
        result = self.store.find_one_and_delete({"uuid": uuid})
        if result is None:
            raise KeyError("Metric does not exist in request store")

    def get_metric(self, uuid):
        result = self.store.find_one({"uuid": uuid}, {"_id": False})
        if result:
            metric = self.metric_type_class_map[Metric.deserialize_slot("type", result["type"])](from_dict=result)
            return metric
        else:
            return None

    def get_metrics(self, ascending=None, descending=None, limit=None, exclude_fields=None, **kwargs):
        """
        Fetch metrics from the store with optional sorting, limiting, and field exclusion.

        Args:
            ascending (str): Field to sort by ascending order.
            descending (str): Field to sort by descending order.
            limit (int): Limit the number of results.
            exclude_fields (dict): Fields to exclude in the result (default is {"_id": False}).
            **kwargs: Filters to apply to the query.

        Returns:
            List of metrics matching the query.
        """
        # Default exclude_fields to {"_id": False} if not provided
        if exclude_fields is None:
            exclude_fields = {"_id": False}

        all_slots = []
        found_type = None
        for k, v in self.metric_type_class_map.items():
            class_slots = list(set().union(Metric.__slots__, v.__slots__))
            if not found_type and all([xi in class_slots for xi in list(kwargs.keys())]):
                found_type = k
            all_slots = list(set().union(all_slots, class_slots))

        if not found_type:
            raise KeyError("The provided keys must be a subset of slots of any of the available metric types.")

        if ascending and ascending not in class_slots:
            raise KeyError(f"The identified metric type does not have the key {ascending}")

        if descending and descending not in class_slots:
            raise KeyError(f"The identified metric type does not have the key {descending}")

        if ascending and descending:
            raise ValueError("Cannot sort by ascending and descending at the same time.")

        # Serialize and clean kwargs
        kwargs = {
            k: self.metric_type_class_map[found_type].serialize_slot(k, v) for k, v in kwargs.items() if v is not None
        }

        # Query the database with filters and exclude_fields
        cursor = self.store.find(kwargs, exclude_fields)

        # Apply sorting
        if ascending:
            cursor = cursor.sort(ascending, pymongo.ASCENDING)
        elif descending:
            cursor = cursor.sort(descending, pymongo.DESCENDING)

        # Apply limit
        if limit:
            cursor = cursor.limit(limit)

        # Process results
        cursor_list = list(cursor)
        if cursor_list:
            return [self.metric_type_class_map[MetricType(i.get("type"))](from_dict=i) for i in cursor_list]

        return []

    def update_metric(self, metric):
        return self.store.find_one_and_update(
            {"uuid": metric.uuid},
            {"$set": metric.serialize()},
            return_document=pymongo.ReturnDocument.AFTER,
        )

    def wipe(self):
        self.database.drop_collection(self.store.name)

    def collect_metric_info(self):
        return self.storage_metric_collector.collect().serialize()

    def remove_old_metrics(self, cutoff):
        cutoff = cutoff.timestamp()
        result = self.store.delete_many({"timestamp": {"$lt": cutoff}})
        return result.deleted_count
