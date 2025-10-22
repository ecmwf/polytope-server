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
import re
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List

from fastapi import Response
from fastapi.responses import JSONResponse
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Gauge,
    generate_latest,
)

from .config import config
from .exceptions import OutputFormatError, RequestFetchError, TelemetryConfigError

logger = logging.getLogger(__name__)


# Regular expression for parsing time strings
regex = re.compile(r"((?P<days>\d+?)d)?((?P<hours>\d+?)h)?((?P<minutes>\d+?)m)?((?P<seconds>\d+?)s)?")


def parse_time(time_str):
    """
    Parse a time string (e.g., '3d', '12h', '10m') into a timedelta object.
    """
    parts = regex.match(time_str)
    if not parts:
        raise ValueError(f"Invalid time format: {time_str}")
    parts = parts.groupdict()
    time_params = {name: int(value) for name, value in parts.items() if value}
    return timedelta(**time_params)


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
        for time_str in raw_timeframes:
            delta = parse_time(time_str)
            metric_name = time_str.replace(" ", "").lower()  # Normalize the metric name
            timeframes.append(
                {
                    "name": f"last_{metric_name}",
                    "delta": delta,
                    "request_metric_name": f"polytope_requests_last_{metric_name}",
                    "user_metric_name": f"polytope_unique_users_last_{metric_name}",
                    "request_metric_description": f"Number of requests in the last {time_str}",
                    "user_metric_description": f"Number of unique users in the last {time_str}",
                }
            )
        return timeframes
    except Exception as e:
        logger.error(f"Error loading timeframes from config: {e}")
        raise TelemetryConfigError("An error occurred while reading telemetry timeframes from the config")


async def get_usage_metrics_aggregated(
    metric_store,
    time_frames: List[Dict[str, Any]],
    now: datetime,
) -> Dict[str, Any]:
    """Fetches aggregated usage metrics directly from MongoDB."""
    try:
        # Calculate cutoff timestamps for each timeframe
        cutoff_timestamps = {frame["name"]: (now - frame["delta"]).timestamp() for frame in time_frames}

        return metric_store.get_usage_metrics_aggregated(cutoff_timestamps)

    except Exception as e:
        logger.error(f"Error fetching metrics: {e}")
        raise RequestFetchError("Failed to retrieve usage metrics")


def format_output_aggregated(metrics, time_frames, format: str):
    """
    Format aggregated metrics output as JSON or Prometheus.
    """
    try:
        if format == "json":
            return JSONResponse(content=prepare_aggregated_json_metrics(metrics, time_frames))

        elif format == "prometheus":
            registry = CollectorRegistry()
            set_aggregated_prometheus_metrics(registry, metrics, time_frames)
            metrics_data = generate_latest(registry)
            return Response(content=metrics_data, media_type=CONTENT_TYPE_LATEST)

        else:
            raise OutputFormatError(f"Unsupported output format: {format}")

    except OutputFormatError as e:
        logger.error(e)
        raise e

    except Exception as e:
        logger.error(f"Error formatting output: {e}")
        raise OutputFormatError("An error occurred while formatting the output")


def prepare_aggregated_json_metrics(metrics: Dict[str, Any], time_frames: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Prepares aggregated metrics in JSON format.
    """
    json_metrics = {
        "total_requests": metrics["total_requests"],
        "unique_users": metrics["unique_users"],
        "time_frames": {},
    }

    for frame in time_frames:
        frame_name = frame["name"]
        if frame_name in metrics["time_frame_metrics"]:
            json_metrics["time_frames"][frame_name] = metrics["time_frame_metrics"][frame_name]

    return json_metrics


def set_aggregated_prometheus_metrics(
    registry: CollectorRegistry,
    metrics: Dict[str, Any],
    time_frames: List[Dict[str, Any]],
):
    """
    Define and register Prometheus metrics from aggregated data.
    """
    try:
        total_requests_metric = Gauge("polytope_total_requests", "Total number of requests", registry=registry)
        total_requests_metric.set(metrics["total_requests"])

        unique_users_metric = Gauge("polytope_unique_users", "Total number of unique users", registry=registry)
        unique_users_metric.set(metrics["unique_users"])

        for frame in time_frames:
            frame_name = frame["name"]
            frame_metrics = metrics["time_frame_metrics"].get(frame_name, {"requests": 0, "unique_users": 0})

            requests_metric = Gauge(
                frame["request_metric_name"],
                frame["request_metric_description"],
                registry=registry,
            )
            requests_metric.set(frame_metrics["requests"])

            users_metric = Gauge(
                frame["user_metric_name"],
                frame["user_metric_description"],
                registry=registry,
            )
            users_metric.set(frame_metrics["unique_users"])

    except Exception as e:
        logger.error(f"Error setting Prometheus metrics: {e}")
        raise


class TelemetryLogSuppressor:
    """
    Suppresses repeated logs of successful auth for the same user
    within a given TTL (in seconds).
    """

    def __init__(self, ttl_seconds: int):
        self.ttl_seconds = ttl_seconds
        # Key: user_id (string), Value: last log timestamp (float)
        self._last_log_time = {}

    def log_if_needed(self, user_id: str):
        now = time.time()
        last_time = self._last_log_time.get(user_id)

        # If within TTL window, skip logging
        if last_time and (now - last_time) < self.ttl_seconds:
            return

        # Otherwise, log and update timestamp
        logger.info(f"User '{user_id}' authenticated for telemetry.")
        self._last_log_time[user_id] = now
