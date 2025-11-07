import logging
from typing import Any, Dict, List, Optional, Sequence

from pymongo import ASCENDING, DESCENDING
from pymongo.collection import Collection

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


class MongoMetricCalculator(MetricCalculator):
    """
    MongoDB-specific metric calculator using aggregation pipelines.
    """

    def __init__(self, collection: Collection):
        """
        Initialize with a MongoDB collection.

        Args:
            collection: The MongoDB collection containing request documents
        """
        self.collection = collection
        self.histogram_builder = HistogramBuilder()
        logger.info("Initialized MongoMetricCalculator for collection: %s", collection.name)

    def ensure_indexes(self) -> None:
        """Create indexes optimized for metric aggregation queries."""
        logger.info("Ensuring metric aggregation indexes for collection: %s", self.collection.name)

        # Drop old indexes with old field names if they exist
        old_index_names = [
            "ix_terminal_status_lastmodified",
            "ix_ts_lastmodified_desc",
            "ix_lastmodified_desc",
            "ix_product_labels_lastmodified",
            "ix_processed_bytes_window",
            "ix_statushistory_processing",
        ]

        for index_name in old_index_names:
            try:
                self.collection.drop_index(index_name)
                logger.info("Dropped old index: %s", index_name)
            except Exception as e:
                logger.debug("Could not drop index %s: %s", index_name, e)

        # Terminal status + last_modified for time-windowed queries
        self.collection.create_index(
            [("status", ASCENDING), ("last_modified", DESCENDING)],
            name="ix_terminal_status_last_modified",
            partialFilterExpression={"status": {"$in": [Status.PROCESSED.value, Status.FAILED.value]}},
        )

        # Timestamp + last_modified descending
        self.collection.create_index(
            [("timestamp", DESCENDING), ("last_modified", DESCENDING)],
            name="ix_ts_last_modified_desc",
        )

        # Last_modified descending (general queries)
        self.collection.create_index(
            [("last_modified", DESCENDING)],
            name="ix_last_modified_desc",
        )

        # Product labels for grouping
        keys = (
            [
                ("collection", ASCENDING),
                ("user.realm", ASCENDING),
            ]
            + [(f"coerced_request.{k}", ASCENDING) for k in TELEMETRY_PRODUCT_LABELS]
            + [
                ("last_modified", DESCENDING),
            ]
        )
        self.collection.create_index(keys, name="ix_product_labels_last_modified")

        # Status + content_length + last_modified for bytes served
        self.collection.create_index(
            [("status", ASCENDING), ("content_length", ASCENDING), ("last_modified", DESCENDING)],
            name="ix_processed_bytes_window",
            partialFilterExpression={"status": Status.PROCESSED.value},
        )

        # Status history indexes for processing duration
        self.collection.create_index(
            [("status_history.processing", ASCENDING), ("status_history.processed", ASCENDING)],
            name="ix_status_history_processing",
        )

        logger.info("Metric aggregation indexes ensured successfully")

    def aggregate_requests_total_window(self, window_seconds: float) -> List[Dict[str, Any]]:
        """MongoDB implementation using aggregation pipeline."""
        logger.debug("Aggregating requests total for window: %.2fs", window_seconds)

        cutoff = now_utc_ts() - window_seconds
        pipeline: Sequence[Dict[str, Any]] = [
            {
                "$match": {
                    "last_modified": {"$gte": cutoff},
                    "status": {"$in": [Status.PROCESSED.value, Status.FAILED.value]},
                }
            },
            {
                "$group": {
                    "_id": {
                        "status": "$status",
                        "collection": "$collection",
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
        cur = self.collection.find(q, proj).sort("last_modified", -1)
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

        cur = self.collection.find(q, proj).sort("last_modified", -1)
        if limit and limit > 0:
            cur = cur.limit(int(limit))
        return list(cur)
