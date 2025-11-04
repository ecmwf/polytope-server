from abc import ABC, abstractmethod
from typing import Any, Dict, List


class MetricCalculator(ABC):
    """
    Abstract base class for calculating request store metrics.
    Each database backend (MongoDB, DynamoDB, etc.) implements this interface.
    """

    @abstractmethod
    def ensure_indexes(self) -> None:
        """
        Ensure required database indexes exist for efficient metric queries.
        This is database-specific and may be a no-op for some backends.
        """
        pass

    @abstractmethod
    def aggregate_requests_total_window(self, window_seconds: float) -> List[Dict[str, Any]]:
        """
        Aggregate total requests in a sliding time window.

        Groups requests by status, collection, realm, and product labels,
        counting the number of requests in each group.

        Args:
            window_seconds: Time window in seconds to look back from now

        Returns:
            List of dicts with 'labels' (dict) and 'value' (int) keys
        """
        pass

    @abstractmethod
    def aggregate_bytes_served_total_window(self, window_seconds: float) -> List[Dict[str, Any]]:
        """
        Aggregate total bytes served in a sliding time window.

        Sums content_length for processed requests, grouped by collection,
        realm, and product labels.

        Args:
            window_seconds: Time window in seconds to look back from now

        Returns:
            List of dicts with 'labels' (dict) and 'value' (int) keys
        """
        pass

    @abstractmethod
    def aggregate_request_duration_histogram(self, window_seconds: float) -> Dict[str, List[Dict[str, Any]]]:
        """
        Calculate histogram of end-to-end request durations.

        Computes request duration as (lastmodified - timestamp) and buckets
        the results for histogram visualization.

        Args:
            window_seconds: Time window in seconds to look back from now

        Returns:
            Dict with 'buckets', 'sum', and 'count' keys, each containing
            lists of label/value pairs for Prometheus histogram format
        """
        pass

    @abstractmethod
    def aggregate_processing_duration_histogram(self, window_seconds: float) -> Dict[str, List[Dict[str, Any]]]:
        """
        Calculate histogram of processing durations (processing → processed).

        Computes processing time from status_history and buckets the results.

        Args:
            window_seconds: Time window in seconds to look back from now

        Returns:
            Dict with 'buckets', 'sum', and 'count' keys for histogram format
        """
        pass

    @abstractmethod
    def aggregate_unique_users(self, windows_seconds: List[int]) -> Dict[int, int]:
        """
        Count unique users across multiple time windows.

        Args:
            windows_seconds: List of time windows in seconds

        Returns:
            Dict mapping window size (seconds) to unique user count
        """
        pass
