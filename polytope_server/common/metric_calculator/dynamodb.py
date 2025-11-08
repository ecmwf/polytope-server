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
from typing import Any, Dict, List, Optional

from .base import MetricCalculator

logger = logging.getLogger(__name__)


class DynamoDBMetricCalculator(MetricCalculator):
    """
    DynamoDB metric calculator placeholder.
    Returns empty data for all aggregation methods.
    """

    def __init__(self, *args, **kwargs):
        """Initialize DynamoDB calculator (ignores all arguments)."""
        logger.info("DynamoDB metric calculator initialized - metrics will return empty data")

    def ensure_indexes(self) -> None:
        """No-op for DynamoDB (uses GSIs defined at table creation)."""
        pass

    def ensure_metric_indexes(self) -> None:
        """No-op for DynamoDB (uses GSIs defined at table creation)."""
        pass

    def aggregate_requests_total_window(self, window_seconds: float) -> List[Dict[str, Any]]:
        """Returns empty list."""
        return []

    def aggregate_bytes_served_total_window(self, window_seconds: float) -> List[Dict[str, Any]]:
        """Returns empty list."""
        return []

    def aggregate_request_duration_histogram(self, window_seconds: float) -> Dict[str, List[Dict[str, Any]]]:
        """Returns empty histogram structure."""
        return {"buckets": [], "sum": [], "count": []}

    def aggregate_processing_duration_histogram(self, window_seconds: float) -> Dict[str, List[Dict[str, Any]]]:
        """Returns empty histogram structure."""
        return {"buckets": [], "sum": [], "count": []}

    def aggregate_unique_users(self, windows_seconds: List[int]) -> Dict[int, int]:
        """Returns zeros for all windows."""
        return {w: 0 for w in windows_seconds}

    def get_usage_metrics_aggregated(self, cutoff_timestamps: Dict[str, float]) -> Dict[str, Any]:
        """Returns empty metrics structure."""
        logger.info("DynamoDB get_usage_metrics_aggregated called - returning zeros")
        timeframe_metrics = {name: {"requests": 0, "unique_users": 0} for name in cutoff_timestamps.keys()}
        return {
            "total_requests": 0,
            "unique_users": 0,
            "timeframe_metrics": timeframe_metrics,
        }

    def list_requests(
        self,
        status: Optional[str] = None,
        req_id: Optional[str] = None,
        limit: Optional[int] = None,
        fields: Optional[Dict[str, int]] = None,
    ) -> List[Dict[str, Any]]:
        """
        List requests with optional filtering.

        Returns empty list for DynamoDB implementation.

        Args:
            status: Optional status filter
            req_id: Optional request ID filter
            limit: Optional limit on number of results (None or 0 for no limit)
            fields: Optional projection dict (ignored for DynamoDB)

        Returns:
            Empty list
        """
        logger.info("DynamoDB list_requests called - returning empty list")
        return []

    def list_requests_by_user(
        self,
        user_id: str,
        status: Optional[str] = None,
        limit: Optional[int] = None,
        fields: Optional[Dict[str, int]] = None,
    ) -> List[Dict[str, Any]]:
        """
        List requests for a specific user with optional filtering.

        Returns empty list for DynamoDB implementation.

        Args:
            user_id: User ID to filter by
            status: Optional status filter
            limit: Optional limit on number of results (None or 0 for no limit)
            fields: Optional projection dict (ignored for DynamoDB)

        Returns:
            Empty list
        """
        logger.info(f"DynamoDB list_requests_by_user called for user {user_id} - returning empty list")
        return []
