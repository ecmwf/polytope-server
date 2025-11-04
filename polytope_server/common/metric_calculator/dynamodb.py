import logging
from typing import Any, Dict, List

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
