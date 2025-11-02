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

import datetime
import logging
from typing import Any, Dict, List, Optional

import pymongo
from pymongo import ASCENDING, DESCENDING

from ...telemetry.telemetry_utils import (
    PROCESSING_DURATION_BUCKETS,
    REQUEST_DURATION_BUCKETS,
    TELEMETRY_PRODUCT_LABELS,
    now_utc_ts,
)
from .. import metric_store, mongo_client_factory
from ..exceptions import ForbiddenRequest, NotFound, UnauthorizedRequest
from ..metric import MetricType, RequestStatusChange
from ..metric_collector import MongoRequestStoreMetricCollector
from ..request import PolytopeRequest, Status
from . import request_store


class MongoRequestStore(request_store.RequestStore):
    def __init__(self, config=None, metric_store_config=None):
        uri = config.get("uri", "mongodb://localhost:27017")
        request_collection = config.get("collection", "requests")
        log_level = config.get("log_level", logging.WARNING)
        logging.getLogger("pymongo").setLevel(log_level)
        username = config.get("username")
        password = config.get("password")

        self.mongo_client = mongo_client_factory.create_client(uri, username, password)
        self.database = self.mongo_client.request_store
        self.store = self.database[request_collection]

        self.metric_store = None
        if metric_store_config:
            self.metric_store = metric_store.create_metric_store(metric_store_config)

        self.request_store_metric_collector = MongoRequestStoreMetricCollector()

        logging.debug("MongoClient configured to open at {}".format(uri))

    def get_type(self):
        return "mongodb"

    def add_request(self, request):
        if self.get_request(request.id) is not None:
            raise ValueError("Request already exists in request store")
        self.store.insert_one(request.serialize())

        if self.metric_store and request.status == Status.PROCESSED:
            self.metric_store.add_metric(
                RequestStatusChange(request_id=request.id, status=request.status, user_id=request.user.id)
            )

        logging.info("Request ID {} status set to {}.".format(request.id, request.status))

    def remove_request(self, id):
        result = self.store.find_one_and_delete({"id": id})
        if result is None:
            raise KeyError("Request does not exist in request store")
        if self.metric_store:
            res = self.metric_store.get_metrics(type=MetricType.REQUEST_STATUS_CHANGE, request_id=id)
            for i in res:
                self.metric_store.remove_metric(i.uuid)
        logging.info("Request ID %s removed.", id)

    def revoke_request(self, user, id):
        if id == "all":
            # Revoke all requests of the user that are waiting or queued
            result = self.store.delete_many(
                {"status": {"$in": [Status.WAITING.value, Status.QUEUED.value]}, "user.id": user.id}
            )
            return result.deleted_count

        result = self.store.find_one_and_delete(
            {"id": id, "status": {"$in": [Status.WAITING.value, Status.QUEUED.value]}, "user.id": user.id}
        )
        if result is None:
            # Check if the request exists to distinguish error cause
            request = self.get_request(id)
            if request is None:
                raise NotFound("Request does not exist in request store")
            elif request.user != user:
                raise UnauthorizedRequest("Request belongs to a different user")
            elif request.status not in [Status.WAITING, Status.QUEUED]:
                raise ForbiddenRequest("Request has started processing and can no longer be revoked.", None)
            else:
                raise
        logging.info("Request ID %s revoked.", id)
        return 1  # Successfully revoked one request

    def get_request(self, id):
        result = self.store.find_one({"id": id}, {"_id": False})
        if result:
            request = PolytopeRequest(from_dict=result)
            return request
        else:
            return None

    def get_requests(self, ascending=None, descending=None, limit=None, **kwargs):
        if ascending:
            if ascending not in PolytopeRequest.__slots__:
                raise KeyError("Request has no key {}".format(ascending))

        if descending:
            if descending not in PolytopeRequest.__slots__:
                raise KeyError("Request has no key {}".format(descending))

        query = {}
        for k, v in kwargs.items():
            if k not in PolytopeRequest.__slots__:
                raise KeyError("Request has no key {}".format(k))

            if v is None:
                continue

            # Querying of mongodb subdocuments behaves unintuitively.
            # Prefer to use an objects custom 'id' attribute if it exists.
            # https://www.oreilly.com/library/view/mongodb-the-definitive/9781449344795/ch04.html

            sub_doc_id = getattr(v, "id", None)
            if sub_doc_id is not None:
                query[k + ".id"] = sub_doc_id
                continue

            query[k] = PolytopeRequest.serialize_slot(k, v)

        cursor = self.store.find(query, {"_id": False})

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
                request = PolytopeRequest(from_dict=i)
                res.append(request)
            return res
        return []

    def update_request(self, request):
        request.last_modified = datetime.datetime.now(datetime.timezone.utc).timestamp()
        res = self.store.find_one_and_update(
            {"id": request.id},
            {"$set": request.serialize()},
            return_document=pymongo.ReturnDocument.AFTER,
        )

        if res is None:
            raise NotFound("Request {} not found in request store".format(request.id))

        if self.metric_store and request.status == Status.PROCESSED:
            self.metric_store.add_metric(
                RequestStatusChange(request_id=request.id, status=request.status, user_id=request.user.id)
            )

        logging.info(
            "Request ID {} updated on request store. Status set to {}.".format(request.id, request.status),
            extra={"request": request.serialize()},
        )

        return res

    def wipe(self):
        if self.metric_store:
            res = self.get_requests()
            for i in res:
                self.metric_store.remove_metric(
                    type=MetricType.REQUEST_STATUS_CHANGE, request_id=i.id, include_processed=True
                )

        self.database.drop_collection(self.store.name)

    def collect_metric_info(self):
        metric = self.request_store_metric_collector.collect().serialize()
        return metric

    def remove_old_requests(self, cutoff):
        cutoff = cutoff.timestamp()
        result = self.store.delete_many(
            {"status": {"$in": [Status.FAILED.value, Status.PROCESSED.value]}, "last_modified": {"$lt": cutoff}}
        )
        return result.deleted_count

    # Methods after this point are for creating indexes and other optimizations to support telemetry.
    def _ensure_indexes(self) -> None:
        """
        Create indexes to support fast sliding-window aggregations.
        Mirrors the pattern used in metric_store._ensure_indexes.
        """
        # State scans (processed/failed) within a time window.
        self.store.create_index(
            [("status", ASCENDING), ("last_modified", DESCENDING)],
            name="ix_terminal_status_last_modified",
            partialFilterExpression={"status": {"$in": [Status.PROCESSED.value, Status.FAILED.value]}},
        )
        # Generic time-window scans.
        self.store.create_index(
            [("last_modified", DESCENDING)],
            name="ix_last_modified_desc",
        )
        # Group-bys on collection, realm, and the product labels
        keys = [
            ("collection", ASCENDING),
            ("user.realm", ASCENDING),
            *[(f"coerced_request.{k}", ASCENDING) for k in TELEMETRY_PRODUCT_LABELS],
            ("last_modified", DESCENDING),
        ]
        self.store.create_index(keys, name="ix_product_labels_last_modified")

        # Optional helpers histogram phases
        self.store.create_index(
            [("timestamp", DESCENDING), ("last_modified", DESCENDING)],
            name="ix_ts_last_modified_desc",
        )
        self.store.create_index(
            [("status", ASCENDING), ("content_length", ASCENDING), ("last_modified", DESCENDING)],
            name="ix_processed_bytes_window",
            partialFilterExpression={"status": Status.PROCESSED.value},
        )

    def agg_requests_total_window(self, window_seconds: float) -> List[Dict[str, Any]]:
        """
        Count request states within the sliding window, grouped by:
          status, collection, realm, and TELEMETRY_PRODUCT_LABELS.
        """
        cutoff = now_utc_ts() - window_seconds
        pipeline = [
            {
                "$match": {
                    "last_modified": {"$gte": cutoff},
                    "status": {"$in": [Status.PROCESSED.value, Status.FAILED.value]},
                }
            },
            {
                "$project": {
                    "status": 1,
                    "collection": 1,
                    "realm": "$user.realm",
                    "cr": "$coerced_request",
                }
            },
            {
                "$group": {
                    "_id": {
                        "status": "$status",
                        "collection": "$collection",
                        "realm": "$realm",
                        **{k: f"$cr.{k}" for k in TELEMETRY_PRODUCT_LABELS},
                    },
                    "value": {"$sum": 1},
                }
            },
            {"$project": {"_id": 0, "labels": "$_id", "value": 1}},
        ]
        return list(self.store.aggregate(pipeline, allowDiskUse=False))

    def agg_bytes_served_total_window(self, window_seconds: float) -> List[Dict[str, Any]]:
        """
        Sum content_length for processed requests within the sliding window, grouped by:
          collection, realm, and TELEMETRY_PRODUCT_LABELS.
        """
        cutoff = now_utc_ts() - window_seconds
        pipeline = [
            {
                "$match": {
                    "last_modified": {"$gte": cutoff},
                    "status": Status.PROCESSED.value,
                    "content_length": {"$type": "number"},
                }
            },
            {
                "$project": {
                    "collection": 1,
                    "realm": "$user.realm",
                    "cr": "$coerced_request",
                    "content_length": 1,
                }
            },
            {
                "$group": {
                    "_id": {
                        "collection": "$collection",
                        "realm": "$realm",
                        **{k: f"$cr.{k}" for k in TELEMETRY_PRODUCT_LABELS},
                    },
                    "value": {"$sum": "$content_length"},
                }
            },
            {"$project": {"_id": 0, "labels": "$_id", "value": 1}},
        ]
        return list(self.store.aggregate(pipeline, allowDiskUse=False))

    def agg_request_duration_histogram(self, window_seconds: float) -> Dict[str, List[Dict[str, Any]]]:
        """
        End-to-end request duration: last_modified - timestamp for requests in window.
        Emits Mongo-scanned rows once, buckets in Python for clarity and to keep one collection scan.
        Labels include status to distinguish success vs failure latency.
        """
        cutoff = now_utc_ts() - window_seconds

        # $match: bound by time and terminal statuses to use ix_terminal_status_last_modified
        rows = list(
            self.store.aggregate(
                [
                    {
                        "$match": {
                            "last_modified": {"$gte": cutoff},
                            "status": {"$in": [Status.PROCESSED.value, Status.FAILED.value]},
                            "timestamp": {"$type": "number"},
                        }
                    },
                    # $project: shrink documents early, compute duration in-database
                    {
                        "$project": {
                            "status": 1,
                            "collection": 1,
                            "realm": "$user.realm",
                            "cr": "$coerced_request",
                            "duration": {"$subtract": ["$last_modified", "$timestamp"]},
                        }
                    },
                ],
                allowDiskUse=False,
            )
        )

        from collections import defaultdict

        # Adding +Inf at exposition time; boundaries define the <= 'le' buckets
        bnds = REQUEST_DURATION_BUCKETS + [float("inf")]

        def le_str(b: float) -> str:
            return "+Inf" if b == float("inf") else str(b)

        def pick_bucket(v: float) -> float:
            for b in bnds:
                if v <= b:
                    return b
            return float("inf")

        bucket_out: Dict[tuple, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        sum_map: Dict[tuple, float] = defaultdict(float)
        cnt_map: Dict[tuple, int] = defaultdict(int)

        for r in rows:
            lid = (
                r["status"],
                r["collection"],
                r.get("realm", ""),
                tuple((r.get("cr") or {}).get(k, "") for k in TELEMETRY_PRODUCT_LABELS),
            )
            dur = float(r.get("duration", 0.0))
            b = pick_bucket(dur)
            bucket_out[lid][le_str(b)] += 1
            sum_map[lid] += dur
            cnt_map[lid] += 1

        buckets_rows, sum_rows, count_rows = [], [], []
        for lid, le_counts in bucket_out.items():
            status, collection, realm, prod = lid
            prod_map = dict(zip(TELEMETRY_PRODUCT_LABELS, prod))
            base = {"status": status, "collection": collection, "realm": realm, **prod_map}
            for b in bnds:
                key = le_str(b)
                buckets_rows.append({"labels": {"le": key, **base}, "value": le_counts.get(key, 0)})
            sum_rows.append({"labels": base, "value": sum_map[lid]})
            count_rows.append({"labels": base, "value": cnt_map[lid]})

        return {"buckets": buckets_rows, "sum": sum_rows, "count": count_rows}

    def agg_processing_duration_histogram(self, window_seconds: float) -> Dict[str, List[Dict[str, Any]]]:
        """
        Processing duration: status_history.processed - status_history.processing for processed requests.
        Only includes documents where both timestamps exist; no status label.
        """
        cutoff = now_utc_ts() - window_seconds

        rows = list(
            self.store.aggregate(
                [
                    {
                        "$match": {
                            "last_modified": {"$gte": cutoff},
                            "status": Status.PROCESSED.value,
                            "status_history.processing": {"$type": "number"},
                            "status_history.processed": {"$type": "number"},
                        }
                    },
                    {
                        "$project": {
                            "collection": 1,
                            "realm": "$user.realm",
                            "cr": "$coerced_request",
                            "proc": {"$subtract": ["$status_history.processed", "$status_history.processing"]},
                        }
                    },
                ],
                allowDiskUse=False,
            )
        )

        from collections import defaultdict

        bnds = PROCESSING_DURATION_BUCKETS + [float("inf")]

        def le_str(b: float) -> str:
            return "+Inf" if b == float("inf") else str(b)

        def pick_bucket(v: float) -> float:
            for b in bnds:
                if v <= b:
                    return b
            return float("inf")

        bucket_out: Dict[tuple, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        sum_map: Dict[tuple, float] = defaultdict(float)
        cnt_map: Dict[tuple, int] = defaultdict(int)

        for r in rows:
            lid = (
                r["collection"],
                r.get("realm", ""),
                tuple((r.get("cr") or {}).get(k, "") for k in TELEMETRY_PRODUCT_LABELS),
            )
            dur = float(r.get("proc", 0.0))
            b = pick_bucket(dur)
            bucket_out[lid][le_str(b)] += 1
            sum_map[lid] += dur
            cnt_map[lid] += 1

        buckets_rows, sum_rows, count_rows = [], [], []
        for lid, le_counts in bucket_out.items():
            collection, realm, prod = lid
            prod_map = dict(zip(TELEMETRY_PRODUCT_LABELS, prod))
            base = {"collection": collection, "realm": realm, **prod_map}
            for b in bnds:
                key = le_str(b)
                buckets_rows.append({"labels": {"le": key, **base}, "value": le_counts.get(key, 0)})
            sum_rows.append({"labels": base, "value": sum_map[lid]})
            count_rows.append({"labels": base, "value": cnt_map[lid]})

        return {"buckets": buckets_rows, "sum": sum_rows, "count": count_rows}

    def agg_unique_users(self, windows_seconds: List[int]) -> Dict[int, int]:
        """
        Distinct user.id counts for multiple windows via $facet.
        Returns { "w{sec}": N }.
        """
        now = now_utc_ts()
        facets = {}
        for w in windows_seconds:
            cutoff = now - w
            facets[str(w)] = [
                {
                    "$match": {
                        "last_modified": {"$gte": cutoff},
                        "status": {"$in": [Status.PROCESSED.value, Status.FAILED.value]},
                        "user.id": {"$exists": True, "$type": "string"},
                    }
                },
                {"$group": {"_id": "$user.id"}},
                {"$group": {"_id": None, "unique": {"$sum": 1}}},
                {"$project": {"_id": 0, "unique": 1}},
            ]
        out = list(self.store.aggregate([{"$facet": facets}], allowDiskUse=False))
        res: Dict[int, int] = {}
        if out:
            row = out[0]
            for k, v in row.items():
                res[int(k)] = v[0]["unique"] if v else 0
        return res

    def list_requests(
        self,
        status: Optional[str] = None,
        req_id: Optional[str] = None,
        limit: int = 100,
        fields: Optional[Dict[str, int]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fast path for /requests:
        - Optional status filter,
        - Optional single id,
        - Sorted by last_modified desc,
        - Light projection driven by 'fields'.
        """
        q: Dict[str, Any] = {}
        if req_id:
            q["id"] = req_id
        if status:
            q["status"] = status

        proj = fields or {
            "_id": 0,
            "id": 1,
            "status": 1,
            "collection": 1,
            "user.id": 1,
            "user.realm": 1,
            "user.username": 1,
            "user.attributes": 1,
            "last_modified": 1,
            "timestamp": 1,
            "content_length": 1,
            "coerced_request": 1,
            "status_history": 1,
            "user_message": 1,
        }
        cur = self.store.find(q, proj).sort("last_modified", -1)
        if limit and limit > 0:
            cur = cur.limit(int(limit))
        return list(cur)

    def list_requests_by_user(
        self,
        user_id: str,
        status: Optional[str] = None,
        limit: int = 100,
        fields: Optional[Dict[str, int]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fast path for /users/{user_id}/requests with optional status.
        """
        q: Dict[str, Any] = {"user.id": user_id}
        if status:
            q["status"] = status

        proj = fields or {
            "_id": 0,
            "id": 1,
            "status": 1,
            "collection": 1,
            "user.id": 1,
            "user.realm": 1,
            "user.username": 1,
            "user.attributes": 1,
            "last_modified": 1,
            "timestamp": 1,
            "content_length": 1,
            "coerced_request": 1,
            "status_history": 1,
            "user_message": 1,
        }

        cur = self.store.find(q, proj).sort("last_modified", -1)
        if limit and limit > 0:
            cur = cur.limit(int(limit))
        return list(cur)
