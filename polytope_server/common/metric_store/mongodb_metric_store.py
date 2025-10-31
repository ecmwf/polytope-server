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
from pymongo import ASCENDING

from .. import mongo_client_factory
from ..metric import Metric, MetricType, RequestStatusChange
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

        self._ensure_indexes()

        self.metric_type_class_map = {
            MetricType.REQUEST_STATUS_CHANGE: RequestStatusChange,
        }

        logging.info("MongoClient configured to open at {}".format(uri))

    def _ensure_indexes(self) -> None:
        """
        Indexes tuned for:
          - $match {type:'request_status_change', status:'processed', timestamp:{$gte:...}}
          - per-user grouping / distinct counting
          - all-time totals (requests & unique users)
        Safe to call repeatedly
        """

        # Primary filter path for the pipelines (time-window scans + totals)
        #    Matches: type + status + timestamp >= X
        self.store.create_index(
            [("type", ASCENDING), ("status", ASCENDING), ("timestamp", ASCENDING)],
            name="type_status_ts",
        )

        # Partial index restricted to the subset:
        #    Great for user-based grouping within the 'processed' subset.
        self.store.create_index(
            [("timestamp", ASCENDING), ("user_id", ASCENDING)],
            name="processed_ts_user",
            partialFilterExpression={
                "type": "request_status_change",
                "status": "processed",
            },
        )

        # 3) Distinct users
        self.store.create_index(
            [("type", ASCENDING), ("status", ASCENDING), ("user_id", ASCENDING)],
            name="type_status_user",
        )

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

    def remove_old_metrics(self, cutoff):
        cutoff = cutoff.timestamp()
        result = self.store.delete_many({"timestamp": {"$lt": cutoff}})
        return result.deleted_count

    def get_usage_metrics_aggregated(self, cutoff_timestamps):
        """
        Aggregates usage metrics for multiple timeframes in a single MongoDB query.

        Strategy: Pre-filter to earliest timeframe, group by user first to reduce
        the working set size, then calculate per-timeframe metrics.

        Args:
            cutoff_timestamps: Dict mapping timeframe names to Unix timestamps
                            e.g., {"last_24h": 1729012520.0, "last_7d": 1728407720.0}

        Returns:
            Dict with total_requests, unique_users, and time_frame_metrics
        """

        # Only query documents from the earliest timeframe onwards to reduce dataset size
        min_cutoff = min(cutoff_timestamps.values()) if cutoff_timestamps else 0

        pipeline = [
            # Filter: Only processed requests since earliest timeframe
            {"$match": {"type": "request_status_change", "status": "processed", "timestamp": {"$gte": min_cutoff}}},
            # Group by user_id first - this reduces subsequent stages' working set
            {
                "$group": {
                    "_id": "$user_id",
                    "timestamps": {"$push": "$timestamp"},  # Keep all request timestamps per user
                    "request_count": {"$sum": 1},
                }
            },
            # Unwind timestamps back to one document per request
            # Now each doc has: {_id: user_id, timestamps: timestamp, request_count: N}
            {"$unwind": "$timestamps"},
            # Calculate all timeframes in parallel using $facet
            {
                "$facet": {
                    # Python dict comprehension creates one sub-pipeline per timeframe
                    **{
                        frame_name: [
                            {"$match": {"timestamps": {"$gte": cutoff}}},
                            {
                                "$group": {
                                    "_id": None,
                                    "requests": {"$sum": 1},
                                    "unique_users": {"$addToSet": "$_id"},  # Collect unique user_ids
                                }
                            },
                            {"$project": {"requests": 1, "unique_users": {"$size": "$unique_users"}}},
                        ]
                        for frame_name, cutoff in cutoff_timestamps.items()
                    }
                }
            },
        ]

        result = list(self.store.aggregate(pipeline))[0]

        # Extract timeframe results
        time_frame_metrics = {}
        for frame_name in cutoff_timestamps.keys():
            frame_data = result[frame_name][0] if result[frame_name] else {"requests": 0, "unique_users": 0}
            time_frame_metrics[frame_name] = frame_data

        # Separate queries for all-time totals
        total_pipeline = [
            {"$match": {"type": "request_status_change", "status": "processed"}},
            {"$group": {"_id": None, "total_requests": {"$sum": 1}}},
        ]

        total_users_pipeline = [
            {"$match": {"type": "request_status_change", "status": "processed"}},
            {"$group": {"_id": "$user_id"}},  # Group by user_id for distinct count
            {"$count": "unique_users"},
        ]

        total_result = list(self.store.aggregate(total_pipeline))
        users_result = list(self.store.aggregate(total_users_pipeline))

        return {
            "total_requests": total_result[0]["total_requests"] if total_result else 0,
            "unique_users": users_result[0]["unique_users"] if users_result else 0,
            "time_frame_metrics": time_frame_metrics,
        }
