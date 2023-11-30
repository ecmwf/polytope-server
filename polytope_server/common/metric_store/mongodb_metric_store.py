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
        uri = config.get("uri", "mongodb://localhost:27017")
        metric_collection = config.get("collection", "metrics")

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

    def remove_metric(self, uuid):
        result = self.store.find_one_and_delete({"uuid": uuid})
        if result is None:
            raise KeyError("Metric does not exist in request store")

    def get_metric(self, uuid):
        result = self.store.find_one({"uuid": uuid}, {"_id": False})
        if result:
            metric = self.metric_type_class_map[result.type](from_dict=result)
            return metric
        else:
            return None

    def get_metrics(self, ascending=None, descending=None, limit=None, **kwargs):
        all_slots = []

        found_type = None
        for k, v in self.metric_type_class_map.items():
            class_slots = list(set().union(Metric.__slots__, v.__slots__))
            if not found_type and all([xi in class_slots for xi in list(kwargs.keys())]):
                found_type = k
            all_slots = list(set().union(all_slots, class_slots))

        if not found_type:
            raise KeyError(
                "The provided keys must be a subset of slots of any of the ",
                "available metric types.",
            )

        if ascending:
            if ascending not in class_slots:
                raise KeyError("The identified metric type does not have the key {}".format(ascending))

        if descending:
            if descending not in class_slots:
                raise KeyError("The identified metric type does not have the key {}".format(descending))

        kwargs_to_pop = []
        for k, v in kwargs.items():
            if v is None:
                kwargs_to_pop.append(k)
                continue
            kwargs[k] = self.metric_type_class_map[found_type].serialize_slot(k, v)
        for k in kwargs_to_pop:
            kwargs.pop(k)

        cursor = self.store.find(kwargs, {"_id": False})

        if ascending is not None and descending is not None:
            raise ValueError("Cannot sort by ascending and descending at the same time.")
        if ascending is not None:
            cursor.sort(ascending, pymongo.ASCENDING)
        elif descending is not None:
            cursor.sort(descending, pymongo.DESCENDING)
        if limit is not None:
            cursor.limit(limit)

        cursor_list = list(cursor)
        if cursor_list:
            res = []
            for i in cursor_list:
                metric = self.metric_type_class_map[MetricType(i.get("type"))](from_dict=i)
                res.append(metric)
            return res
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
