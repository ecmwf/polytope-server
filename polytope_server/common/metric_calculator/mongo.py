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
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pymongo import ASCENDING, DESCENDING
from pymongo.collection import Collection
from pymongo.errors import OperationFailure

from ...telemetry.telemetry_utils import (
    PROCESSING_DURATION_BUCKETS,
    REQUEST_DURATION_BUCKETS,
    TELEMETRY_PRODUCT_LABELS,
    now_utc_ts,
)
from ..request import Status
from .base import MetricCalculator
from .histogram import HistogramBuilder

logger = logging.getLogger(__name__)


def safe_create_index(
    coll: Collection,
    keys: List[Tuple[str, Any]],
    name: str,
    **kwargs,
) -> str:
    """
    Create a MongoDB index in a way that's resilient to legacy index names/specs.

    Handles:
    - code 86 (IndexKeySpecsConflict): same name, different key spec -> drop old, recreate.
    - code 85 (IndexOptionsConflict): same key spec, different name -> keep existing, don't fail.

    Returns the name of the index that ends up being in use.
    """
    try:
        return coll.create_index(keys, name=name, **kwargs)
    except OperationFailure as exc:
        # 86: same name, different spec  -> we want *our* spec, drop & recreate
        if exc.code == 86:
            coll.drop_index(name)
            return coll.create_index(keys, name=name, **kwargs)

        # 85: same spec, different name -> find index with same keys, keep it
        if exc.code == 85:
            existing = coll.index_information()
            # keys in index_information() are list of (field, direction)
            for idx_name, spec in existing.items():
                if spec.get("key") == keys:
                    # There is already an index with this spec under another name.
                    # Index name doesn't affect query planning, so we just keep it.
                    return idx_name

        # anything else: bubble up
        raise


class MongoMetricCalculator(MetricCalculator):
    """
    MongoDB-specific metric calculator using aggregation pipelines.

    Operates on two collections:
    - requests collection (request_store DB) for request metrics
    - metrics collection (metric_store DB) for usage metrics
    """

    def __init__(self, collection: Collection, metric_collection: Optional[Collection] = None):
        """
        Initialize with a MongoDB collection.

        Args:
            collection: The MongoDB collection containing request documents (requests)
            metric_collection: Optional MongoDB collection containing metric documents (metrics)
        """
        self.collection = collection
        self.metric_collection = metric_collection
        self.histogram_builder = HistogramBuilder()
        logger.info("Initialized MongoMetricCalculator for collection %s", collection.name)
        if metric_collection is not None:
            logger.info("  with metric_collection %s", metric_collection.name)

    def ensure_indexes(self) -> None:
        """Ensure all indexes needed for metric queries exist."""

        # For fast queries on terminal status + time windows
        safe_create_index(
            self.collection,
            [("status", ASCENDING), ("last_modified", DESCENDING)],
            name="ix_terminal_status_last_modified",
            partialFilterExpression={
                "status": {
                    "$in": [
                        Status.PROCESSED.value,
                        Status.WAITING.value,
                        Status.QUEUED.value,
                        Status.PROCESSING.value,
                        Status.FAILED.value,
                    ]
                }
            },
        )

        # Generic descending timestamp + last_modified index
        safe_create_index(
            self.collection,
            [("timestamp", DESCENDING), ("last_modified", DESCENDING)],
            name="ix_ts_last_modified_desc",
        )

        # Fallback index on last_modified alone
        safe_create_index(
            self.collection,
            [("last_modified", DESCENDING)],
            name="ix_last_modified_desc",
        )

        # For user-specific queries
        safe_create_index(
            self.collection,
            [("user.id", ASCENDING), ("last_modified", DESCENDING)],
            name="ix_requests_by_user_last_modified",
        )

        # Dynamic product-label index based on TELEMETRY_PRODUCT_LABELS
        keys = (
            [
                ("collection", ASCENDING),
                ("datasource", ASCENDING),
                ("user.realm", ASCENDING),
            ]
            + [(f"coerced_request.{k}", ASCENDING) for k in TELEMETRY_PRODUCT_LABELS]
            + [
                ("last_modified", DESCENDING),
            ]
        )

        # Main index for product-label aggregations
        # safe_create_index handles both spec changes (code 86) and legacy names (code 85)
        safe_create_index(
            self.collection,
            keys,
            name="ix_product_labels_last_modified",
        )

        # For aggregations over processed bytes within time windows
        safe_create_index(
            self.collection,
            [("status", ASCENDING), ("content_length", ASCENDING), ("last_modified", DESCENDING)],
            name="ix_processed_bytes_window",
            partialFilterExpression={"status": Status.PROCESSED.value},
        )

        # For queries using status history (processing → processed timing)
        safe_create_index(
            self.collection,
            [("status_history.processing", ASCENDING), ("status_history.processed", ASCENDING)],
            name="ix_status_history_processing",
        )

        logger.info("Metric aggregation indexes ensured successfully")

    def ensure_metric_indexes(self) -> None:
        """
        Create indexes optimized for metrics collection queries.

        Operates on the metrics collection, not the requests collection.
        """
        if self.metric_collection is None:
            logger.warning("No metric_collection provided, skipping metric indexes")
            return

        logger.info("Ensuring metric store indexes for collection: %s", self.metric_collection.name)

        # Index for type + status + timestamp queries
        safe_create_index(
            self.metric_collection,
            [("type", 1), ("status", 1), ("timestamp", -1)],
            name="ix_type_status_ts",
        )

        # Index for type + status + user aggregations (processed requests)
        safe_create_index(
            self.metric_collection,
            [("type", 1), ("status", 1), ("user_id", 1)],
            name="ix_type_status_user",
        )

        # Index for processed timestamps and user grouping
        safe_create_index(
            self.metric_collection,
            [("timestamp", ASCENDING), ("user_id", ASCENDING)],
            name="ix_processed_ts_user",
            partialFilterExpression={
                "type": "request_status_change",
                "status": "processed",
            },
        )

        logger.info("Metric store indexes ensured successfully")

    def get_usage_metrics_aggregated(self, cutoff_timestamps: Dict[str, float]) -> Dict[str, Any]:
        """
        Get aggregated usage metrics for multiple time windows.

        Operates on the metrics collection, not the requests collection.

        Args:
            cutoff_timestamps: Dict mapping timeframe names (e.g., "5m", "1h")
                            to Unix timestamps

        Returns:
            Dict containing:
                - total_requests: Total count of all processed requests
                - unique_users: Count of unique users across all time
                - timeframe_metrics: Dict mapping timeframe names to
                {requests: int, unique_users: int}
        """
        if self.metric_collection is None:
            logger.warning("No metric_collection provided, returning empty metrics")
            return {
                "total_requests": 0,
                "unique_users": 0,
                "timeframe_metrics": {name: {"requests": 0, "unique_users": 0} for name in cutoff_timestamps.keys()},
            }

        logger.debug("Aggregating usage metrics for cutoffs %s", cutoff_timestamps)

        base_filter = {
            "type": "request_status_change",
            "status": "processed",
        }

        # Total number of requests
        # Assume no duplicates
        t0 = time.time()
        total_requests = self.metric_collection.count_documents(base_filter)
        logger.debug("Total requests count took %.4fs", time.time() - t0)

        # Total number of unique users
        t0 = time.time()
        user_pipeline = [
            {"$match": base_filter},
            {"$group": {"_id": "$user_id"}},
            {"$count": "total"},
        ]
        user_res = list(self.metric_collection.aggregate(user_pipeline, allowDiskUse=True))
        unique_users = user_res[0]["total"] if user_res else 0
        logger.debug("Total unique users count took %.4fs", time.time() - t0)

        timeframe_metrics = {}

        # Per-window metrics
        for name, cutoff in cutoff_timestamps.items():
            window_filter = base_filter.copy()
            window_filter["timestamp"] = {"$gte": cutoff}

            t0 = time.time()
            req_count = self.metric_collection.count_documents(window_filter)
            logger.debug("Request count for window %s took %.4fs", name, time.time() - t0)

            t0 = time.time()
            window_user_pipeline = [
                {"$match": window_filter},
                {"$group": {"_id": "$user_id"}},
                {"$count": "total"},
            ]
            window_user_res = list(self.metric_collection.aggregate(window_user_pipeline, allowDiskUse=True))
            user_count = window_user_res[0]["total"] if window_user_res else 0
            logger.debug("User count for window %s took %.4fs", name, time.time() - t0)

            timeframe_metrics[name] = {
                "requests": req_count,
                "unique_users": user_count,
            }

        result = {
            "total_requests": total_requests,
            "unique_users": unique_users,
            "timeframe_metrics": timeframe_metrics,
        }

        logger.debug("Usage metrics aggregation result: %s", result)
        return result

    def aggregate_requests_total_window(self, window_seconds: float) -> List[Dict[str, Any]]:
        """MongoDB implementation using aggregation pipeline."""
        logger.debug("Aggregating requests total for window: %.2fs", window_seconds)

        cutoff = now_utc_ts() - window_seconds
        pipeline: Sequence[Dict[str, Any]] = [
            {
                "$match": {
                    "last_modified": {"$gte": cutoff},
                    "status": {
                        "$in": [
                            Status.PROCESSED.value,
                            Status.WAITING.value,
                            Status.QUEUED.value,
                            Status.PROCESSING.value,
                            Status.FAILED.value,
                        ]
                    },
                }
            },
            {
                "$group": {
                    "_id": {
                        "status": "$status",
                        "collection": "$collection",
                        "datasource": "$datasource",
                        "realm": "$user.realm",
                        **{k: f"$coerced_request.{k}" for k in TELEMETRY_PRODUCT_LABELS},
                    },
                    "value": {"$sum": 1},
                }
            },
            {"$project": {"_id": 0, "labels": "$_id", "value": 1}},
        ]

        result = list(self.collection.aggregate(pipeline, allowDiskUse=False))
        logger.debug("Requests total aggregation returned %d label groups", len(result))
        return result

    def aggregate_bytes_served_total_window(self, window_seconds: float) -> List[Dict[str, Any]]:
        """MongoDB implementation using aggregation pipeline."""
        logger.debug("Aggregating bytes served for window: %.2fs", window_seconds)

        cutoff = now_utc_ts() - window_seconds
        pipeline: Sequence[Dict[str, Any]] = [
            {
                "$match": {
                    "last_modified": {"$gte": cutoff},
                    "status": Status.PROCESSED.value,
                    "content_length": {"$type": "number"},
                }
            },
            {
                "$group": {
                    "_id": {
                        "collection": "$collection",
                        "datasource": "$datasource",
                        "realm": "$user.realm",
                        **{k: f"$coerced_request.{k}" for k in TELEMETRY_PRODUCT_LABELS},
                    },
                    "value": {"$sum": "$content_length"},
                }
            },
            {"$project": {"_id": 0, "labels": "$_id", "value": 1}},
        ]

        result = list(self.collection.aggregate(pipeline, allowDiskUse=False))
        total_bytes = sum(item["value"] for item in result)
        logger.debug("Bytes served aggregation returned %d label groups, total: %d bytes", len(result), total_bytes)
        return result

    def aggregate_request_duration_histogram(self, window_seconds: float) -> Dict[str, List[Dict[str, Any]]]:
        """MongoDB implementation with client-side histogram building."""
        logger.debug("Aggregating request duration histogram for window: %.2fs", window_seconds)

        cutoff = now_utc_ts() - window_seconds
        # Optimize: Only fetch fields needed for histogram
        pipeline: Sequence[Dict[str, Any]] = [
            {
                "$match": {
                    "last_modified": {"$gte": cutoff},
                    "status": {"$in": [Status.PROCESSED.value, Status.FAILED.value]},
                    "timestamp": {"$type": "number"},
                }
            },
            {
                "$project": {
                    "status": 1,
                    "collection": 1,
                    "datasource": 1,
                    "realm": "$user.realm",
                    # Only project the specific label fields we need
                    **{f"cr_{k}": f"$coerced_request.{k}" for k in TELEMETRY_PRODUCT_LABELS},
                    "duration": {"$subtract": ["$last_modified", "$timestamp"]},
                }
            },
        ]
        rows = list(self.collection.aggregate(pipeline, allowDiskUse=False))
        logger.debug("Retrieved %d rows for request duration histogram", len(rows))

        # Restructure rows for histogram builder
        formatted_rows = []
        for r in rows:
            cr = {k: r.get(f"cr_{k}", "") for k in TELEMETRY_PRODUCT_LABELS}
            formatted_rows.append(
                {
                    "status": r["status"],
                    "collection": r["collection"],
                    "datasource": r.get("datasource", ""),
                    "realm": r.get("realm", ""),
                    "cr": cr,
                    "duration": r["duration"],
                }
            )

        result = self.histogram_builder.build_histogram(
            rows=formatted_rows,
            buckets=list(REQUEST_DURATION_BUCKETS),
            duration_key="duration",
            include_status=True,
            product_labels=list(TELEMETRY_PRODUCT_LABELS),
        )

        logger.debug(
            "Request duration histogram built: %d buckets, %d sums, %d counts",
            len(result["buckets"]),
            len(result["sum"]),
            len(result["count"]),
        )
        return result

    def aggregate_processing_duration_histogram(self, window_seconds: float) -> Dict[str, List[Dict[str, Any]]]:
        """MongoDB implementation with client-side histogram building."""
        logger.debug("Aggregating processing duration histogram for window: %.2fs", window_seconds)

        cutoff = now_utc_ts() - window_seconds
        # Optimize: Only fetch fields needed for histogram
        pipeline: Sequence[Dict[str, Any]] = [
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
                    "datasource": 1,
                    "realm": "$user.realm",
                    # Only project the specific label fields we need
                    **{f"cr_{k}": f"$coerced_request.{k}" for k in TELEMETRY_PRODUCT_LABELS},
                    "proc": {"$subtract": ["$status_history.processed", "$status_history.processing"]},
                }
            },
        ]
        rows = list(self.collection.aggregate(pipeline, allowDiskUse=False))
        logger.debug("Retrieved %d rows for processing duration histogram", len(rows))

        # Restructure rows for histogram builder
        formatted_rows = []
        for r in rows:
            cr = {k: r.get(f"cr_{k}", "") for k in TELEMETRY_PRODUCT_LABELS}
            formatted_rows.append(
                {
                    "collection": r["collection"],
                    "datasource": r.get("datasource", ""),
                    "realm": r.get("realm", ""),
                    "cr": cr,
                    "proc": r["proc"],
                }
            )

        result = self.histogram_builder.build_histogram(
            rows=formatted_rows,
            buckets=list(PROCESSING_DURATION_BUCKETS),
            duration_key="proc",
            include_status=False,
            product_labels=list(TELEMETRY_PRODUCT_LABELS),
        )

        logger.debug(
            "Processing duration histogram built: %d buckets, %d sums, %d counts",
            len(result["buckets"]),
            len(result["sum"]),
            len(result["count"]),
        )
        return result

    def aggregate_unique_users(self, windows_seconds: List[int]) -> Dict[int, int]:
        """MongoDB implementation using $facet for multiple windows."""
        logger.debug("Aggregating unique users for windows: %s", windows_seconds)

        now = now_utc_ts()
        facets: Dict[str, List[Dict[str, Any]]] = {}

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
                {"$count": "unique"},
            ]

        pipeline: Sequence[Dict[str, Any]] = [{"$facet": facets}]
        out = list(self.collection.aggregate(pipeline, allowDiskUse=False))

        res: Dict[int, int] = {}
        if out:
            row = out[0]
            for k, v in row.items():
                res[int(k)] = v[0]["unique"] if v else 0

        logger.debug("Unique users aggregation result: %s", res)
        return res

    def list_requests(
        self,
        status: Optional[str] = None,
        req_id: Optional[str] = None,
        limit: Optional[int] = 0,
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
            "datasource": 1,
            "url": 1,
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
        cur = self.collection.find(q, proj).sort("last_modified", -1)
        if limit and limit > 0:
            cur = cur.limit(int(limit))
        return list(cur)

    def list_requests_by_user(
        self,
        user_id: str,
        status: Optional[str] = None,
        limit: Optional[int] = 0,
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
            "datasource": 1,
            "url": 1,
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

        cur = self.collection.find(q, proj).sort("last_modified", -1)
        if limit and limit > 0:
            cur = cur.limit(int(limit))
        return list(cur)
