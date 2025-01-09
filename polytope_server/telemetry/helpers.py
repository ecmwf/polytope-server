import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import Response
from fastapi.responses import JSONResponse
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Gauge,
    generate_latest,
)

from .config import config
from .enums import StatusEnum
from .exceptions import (
    MetricCalculationError,
    OutputFormatError,
    RequestFetchError,
    TelemetryConfigError,
    TelemetryDataError,
)

logger = logging.getLogger(__name__)

# Global cache dictionary for usage metrics
usage_metrics_cache = {"data": None, "timestamp": None}


def obfuscate_apikey(key: str) -> str:
    """Obfuscate the API key, keeping only the first and last 4 characters visible."""
    if len(key) > 8:
        return f"{key[:4]}{'*' * (len(key) - 8)}{key[-4:]}"
    return "*" * len(key)


def is_usage_enabled():
    """
    Check if telemetry usage endpoint is enabled in the config.
    Handles errors internally and raises an appropriate exception if there's an issue.
    """
    try:
        enabled = config.get("telemetry", {}).get("usage", {}).get("enabled", False)
        if not isinstance(enabled, bool):
            raise TelemetryConfigError("The 'enabled' field in the telemetry config must be a boolean")
        return enabled
    except Exception as e:
        logger.error(f"Error checking telemetry usage status: {e}")
        raise TelemetryConfigError("An error occurred while reading the telemetry configuration")


def get_usage_timeframes_from_config() -> List[Dict[str, Any]]:
    """
    Load timeframes from the telemetry configuration and generate metric details.
    """
    try:
        raw_timeframes = config.get("telemetry", {}).get("usage", {}).get("timeframes", [])
        if not raw_timeframes:
            raise TelemetryConfigError("No timeframes defined in telemetry configuration")

        timeframes = []
        for days in raw_timeframes:
            if not isinstance(days, int) or days <= 0:
                raise TelemetryConfigError(f"Invalid timeframe value: {days}")
            timeframes.append(
                {
                    "name": f"last_{days}d",
                    "delta": timedelta(days=days),
                    "request_metric_name": f"polytope_requests_last_{days}d",
                    "user_metric_name": f"polytope_unique_users_last_{days}d",
                    "request_metric_description": f"Number of requests in the last {days} days",
                    "user_metric_description": f"Number of unique users in the last {days} days",
                }
            )
        return timeframes
    except Exception as e:
        logger.error(f"Error loading timeframes from config: {e}")
        raise TelemetryConfigError("An error occurred while reading telemetry timeframes from the config")


async def get_cached_usage_user_requests(
    status: Optional[StatusEnum],
    id: Optional[str],
    request_store,
    metric_store,
    fetch_function,
    cache_expiry_seconds: int,
) -> List[Dict[str, Any]]:
    """
    Fetches user requests from the cache or calls the fetch_function if cache is expired.
    """
    try:
        now = datetime.now(timezone.utc)
        cache_expiry = timedelta(seconds=cache_expiry_seconds)

        if usage_metrics_cache["data"] and usage_metrics_cache["timestamp"]:
            if now - usage_metrics_cache["timestamp"] < cache_expiry:
                return usage_metrics_cache["data"]

        # Fetch fresh data if cache is expired
        user_requests = await fetch_function(
            status=status,
            id=id,
            request_store=request_store,
            metric_store=metric_store,
        )

        if not isinstance(user_requests, list):
            raise TelemetryDataError("Fetched data is not in the expected list format")

        # Update the cache
        usage_metrics_cache["data"] = user_requests
        usage_metrics_cache["timestamp"] = now
        return user_requests
    except Exception as e:
        logger.error(f"Unexpected error while fetching cached user requests: {e}")
        raise RequestFetchError("Failed to retrieve or cache user requests")


def calculate_usage_metrics(
    user_requests: List[Dict[str, Any]], time_frames: List[Dict[str, Any]], now: datetime
) -> Dict[str, Any]:
    """
    Calculates usage metrics over specified time frames.
    """
    try:
        metrics = {"total_requests": len(user_requests), "unique_users": set(), "time_frame_metrics": {}}

        # Collect unique users and calculate time frame metrics
        for request_data in user_requests:
            user_id = request_data.get("user", {}).get("id")
            if user_id:
                metrics["unique_users"].add(user_id)

        for frame in time_frames:
            frame_name = frame["name"]
            frame_threshold = now - frame["delta"]
            metrics["time_frame_metrics"][frame_name] = {"requests": 0, "unique_users": set()}

        for request_data in user_requests:
            request_timestamp = datetime.fromtimestamp(request_data["timestamp"], tz=timezone.utc)
            user_id = request_data.get("user", {}).get("id")

            for frame in time_frames:
                frame_name = frame["name"]
                frame_threshold = now - frame["delta"]
                if request_timestamp >= frame_threshold:
                    metrics["time_frame_metrics"][frame_name]["requests"] += 1
                    if user_id:
                        metrics["time_frame_metrics"][frame_name]["unique_users"].add(user_id)

        return metrics
    except Exception as e:
        logger.error(f"Error calculating usage metrics: {e}")
        raise MetricCalculationError("An error occurred while calculating usage metrics")


def prepare_usage_json_metrics(metrics: Dict[str, Any], time_frames: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Prepares metrics in JSON format.
    """
    json_metrics = {
        "total_requests": metrics["total_requests"],
        "unique_users": len(metrics["unique_users"]),
        "time_frames": {},
    }

    for frame in time_frames:
        frame_name = frame["name"]
        frame_metrics = metrics["time_frame_metrics"][frame_name]
        json_metrics["time_frames"][frame_name] = {
            "requests": frame_metrics["requests"],
            "unique_users": len(frame_metrics["unique_users"]),
        }

    return json_metrics


def set_usage_prometheus_metrics(
    registry: CollectorRegistry,
    metrics: Dict[str, Any],
    time_frames: List[Dict[str, Any]],
):
    """
    Define and register Prometheus metrics for the given usage data.
    """
    try:
        # Total requests metric
        total_requests_metric = Gauge("polytope_total_requests", "Total number of requests", registry=registry)
        total_requests_metric.set(metrics["total_requests"])

        # Unique users metric
        unique_users_metric = Gauge("polytope_unique_users", "Total number of unique users", registry=registry)
        unique_users_metric.set(len(metrics["unique_users"]))

        # Timeframe-specific metrics
        for frame in time_frames:
            frame_metrics = metrics["time_frame_metrics"][frame["name"]]

            # Requests metric for this timeframe
            requests_metric = Gauge(
                frame["request_metric_name"],
                frame["request_metric_description"],
                registry=registry,
            )
            requests_metric.set(frame_metrics["requests"])

            # Unique users metric for this timeframe
            users_metric = Gauge(
                frame["user_metric_name"],
                frame["user_metric_description"],
                registry=registry,
            )
            users_metric.set(len(frame_metrics["unique_users"]))

    except Exception as e:
        logger.error(f"Error setting Prometheus metrics: {e}")
        raise


def format_output(metrics, time_frames, format: str):
    """
    Format metrics output as JSON or Prometheus.
    """
    try:
        if format == "json":
            return JSONResponse(content=prepare_usage_json_metrics(metrics, time_frames))

        elif format == "prometheus":
            # Use a new CollectorRegistry for each request
            registry = CollectorRegistry()

            # Set Prometheus metrics
            set_usage_prometheus_metrics(registry, metrics, time_frames)

            # Generate Prometheus metrics output
            metrics_data = generate_latest(registry)
            return Response(content=metrics_data, media_type=CONTENT_TYPE_LATEST)

        else:
            raise OutputFormatError(f"Unsupported output format: {format}")

    except OutputFormatError as e:
        logger.error(e)
        raise e  # Reraise for the main exception handler

    except Exception as e:
        logger.error(f"Error formatting output: {e}")
        raise OutputFormatError("An error occurred while formatting the output")
        raise e  # Reraise for the main exception handler

    except Exception as e:
        logger.error(f"Error formatting output: {e}")
        raise OutputFormatError("An error occurred while formatting the output")
