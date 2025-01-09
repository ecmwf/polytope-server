import json
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional

from fastapi import Response
from prometheus_client import CollectorRegistry, Gauge

from .config import config
from .enums import StatusEnum

# Global cache dictionary for usage metrics
usage_metrics_cache = {"data": None, "timestamp": None}


def is_usage_enabled():
    """Check if telemetry usage endpoint is enabled in the config."""
    return config.get("telemetry", {}).get("usage", {}).get("enabled", False)


def get_usage_timeframes_from_config():
    """
    Load timeframes from YAML config and auto-calculate metric details.
    """
    raw_timeframes = config.get("telemetry", {}).get("usage", {}).get("timeframes", [1, 2, 7, 30])
    timeframes = []

    for days in raw_timeframes:
        frame = {
            "name": f"last_{days}d",
            "delta": timedelta(days=days),
            "request_metric_name": f"polytope_requests_last_{days}d",
            "user_metric_name": f"polytope_unique_users_last_{days}d",
            "request_metric_description": f"Number of requests in the last {days} days",
            "user_metric_description": f"Number of unique users in the last {days} days",
        }
        timeframes.append(frame)

    return timeframes


async def get_cached_usage_user_requests(
    status: Optional[StatusEnum],
    id: Optional[str],
    request_store,
    metric_store,
    fetch_function: Callable,  # Callable function
    cache_expiry_seconds: int = 30,
) -> List[Dict[str, Any]]:
    """
    Fetches user requests from the all_requests function and caches the result.
    """
    now = datetime.now(timezone.utc)
    cache_expiry = timedelta(seconds=cache_expiry_seconds)

    if usage_metrics_cache["data"] and usage_metrics_cache["timestamp"]:
        if now - usage_metrics_cache["timestamp"] < cache_expiry:
            return usage_metrics_cache["data"]

    user_requests = await fetch_function(
        status=status,
        id=id,
        request_store=request_store,
        metric_store=metric_store,
    )

    if isinstance(user_requests, Response):
        user_requests = json.loads(user_requests.body.decode("utf-8"))
    elif not isinstance(user_requests, list):
        raise Exception("Unexpected data format from all_requests")

    usage_metrics_cache["data"] = user_requests
    usage_metrics_cache["timestamp"] = now
    return user_requests


def calculate_usage_metrics(
    user_requests: List[Dict[str, Any]], time_frames: List[Dict[str, Any]], now: datetime
) -> Dict[str, Any]:
    """
    Calculates metrics over specified time frames.
    """
    metrics = {"total_requests": len(user_requests), "unique_users": set(), "time_frame_metrics": {}}

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
    registry: CollectorRegistry, metrics: Dict[str, Any], time_frames: List[Dict[str, Any]]
):
    """
    Defines and sets Prometheus metrics based on calculated metrics and time frames.
    """
    total_requests_metric = Gauge("polytope_total_requests", "Total number of requests", registry=registry)
    total_requests_metric.set(metrics["total_requests"])

    unique_users_metric = Gauge("polytope_unique_users", "Total number of unique users", registry=registry)
    unique_users_metric.set(len(metrics["unique_users"]))

    for frame in time_frames:
        frame_metrics = metrics["time_frame_metrics"][frame["name"]]

        requests_metric = Gauge(frame["request_metric_name"], frame["request_metric_description"], registry=registry)
        requests_metric.set(frame_metrics["requests"])

        users_metric = Gauge(frame["user_metric_name"], frame["user_metric_description"], registry=registry)
        users_metric.set(len(frame_metrics["unique_users"]))
