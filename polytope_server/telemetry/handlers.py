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
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from ..common.metric import MetricType
from .config import config
from .dependencies import (
    get_auth,
    get_metric_store,
    get_request_store,
    get_staging,
    metrics_auth,
)
from .enums import StatusEnum
from .exceptions import (
    MetricCalculationError,
    OutputFormatError,
    RequestFetchError,
    TelemetryConfigError,
    TelemetryUsageDisabled,
)
from .helpers import (
    calculate_usage_metrics,
    format_output,
    get_cached_usage_metrics,
    get_usage_timeframes_from_config,
    is_usage_enabled,
    obfuscate_apikey,
)

logger = logging.getLogger(__name__)

# Prefix for all telemetry endpoints
router = APIRouter(prefix="/telemetry/v1")


@router.get("/", summary="List API routes (optional)")
async def list_endpoints():
    """
    Optional 'index' endpoint for enumerating possible sub-routes.
    """
    return ["health", "status", "requests", "users/{user_id}/requests", "workers", "report", "metrics"]


@router.get("/health", summary="Health check endpoint")
async def health():
    """
    Simple endpoint to verify server is up and running.
    """
    return {"message": "Polytope telemetry server is alive"}


@router.get("/status", summary="Get overall service status")
async def service_status(
    request_store=Depends(get_request_store),
    staging=Depends(get_staging),
    auth=Depends(get_auth),
    metric_store=Depends(get_metric_store),
):
    """
    Returns status or metrics for core services/stores.
    """
    return {
        "request_store": request_store.collect_metric_info(),
        "staging": staging.collect_metric_info(),
        "auth": auth.collect_metric_info(),
        "metric_store": metric_store.collect_metric_info() if metric_store else None,
    }


@router.get("/requests", summary="Retrieve requests")
async def all_requests(
    status: Optional[StatusEnum] = Query(None, description="Filter requests by status"),
    id: Optional[str] = Query(None, description="Filter requests by ID"),
    request_store=Depends(get_request_store),
    metric_store=Depends(get_metric_store),
):
    """
    Fetch a list of requests. Can filter by status and/or ID.
    """
    active_statuses = {
        StatusEnum.ACTIVE: [
            StatusEnum.WAITING,
            StatusEnum.UPLOADING,
            StatusEnum.QUEUED,
            StatusEnum.PROCESSING,
        ]
    }

    if status == StatusEnum.ACTIVE:
        statuses = active_statuses[status]
    elif status:
        statuses = [status]
    else:
        statuses = []

    # Fetch requests from the store
    user_requests = []
    if statuses:
        for status_filter in statuses:
            query = {"status": status_filter, "id": id}
            user_requests += request_store.get_requests(**query)
    else:
        user_requests = request_store.get_requests(id=id)

    # Serialize and possibly attach metrics
    response_message = []
    for request in user_requests:
        serialized_request = request.serialize()

        if id:
            metrics = metric_store.get_metrics(type=MetricType.REQUEST_STATUS_CHANGE, request_id=id)
            serialized_request["trace"] = [metric.serialize() for metric in metrics]

        # Obfuscate user details
        serialized_request["user"]["details"] = "**hidden**"
        if config.get("telemetry", {}).get("obfuscate_apikeys", False):
            attributes = serialized_request["user"].get("attributes", {})
            if "ecmwf-apikey" in attributes:
                attributes["ecmwf-apikey"] = obfuscate_apikey(attributes["ecmwf-apikey"])

        response_message.append(serialized_request)

    return response_message


@router.get("/users/{user_id}/requests", summary="Get requests by user")
async def user_requests(
    user_id: str,
    status: Optional[StatusEnum] = Query(None, description="Filter by status"),
    id: Optional[str] = Query(None, description="Filter by ID"),
    request_store=Depends(get_request_store),
    metric_store=Depends(get_metric_store),
):
    """
    Get all requests for a given user, optionally filtered by status or ID.
    """
    active_statuses = {
        StatusEnum.ACTIVE: [
            StatusEnum.WAITING,
            StatusEnum.UPLOADING,
            StatusEnum.QUEUED,
            StatusEnum.PROCESSING,
        ]
    }

    user_requests = request_store.get_requests(id=id)

    # Filter by status if provided
    if status == StatusEnum.ACTIVE:
        statuses = active_statuses[status]
    elif status:
        statuses = [status]
    else:
        statuses = []

    if statuses:
        user_requests = [r for r in user_requests if r.status in statuses]

    # Filter by user ID
    filtered_requests = []
    for request in user_requests:
        if request.serialize()["user"]["id"] == user_id:
            filtered_requests.append(request)

    # Serialize and attach metrics
    response_message = []
    for request in filtered_requests:
        serialized_request = request.serialize()

        metrics = metric_store.get_metrics(type=MetricType.REQUEST_STATUS_CHANGE, request_id=user_id)
        serialized_request["metrics"] = [m.serialize() for m in metrics]

        # Hide sensitive info
        serialized_request["user"]["details"] = "**hidden**"
        if config.get("telemetry", {}).get("obfuscate_apikeys", False):
            attributes: Dict[str, Any] = serialized_request["user"].get("attributes", {})
            if "ecmwf-apikey" in attributes:
                attributes["ecmwf-apikey"] = obfuscate_apikey(attributes["ecmwf-apikey"])

        response_message.append(serialized_request)

    return response_message


@router.get("/workers", summary="Get workers information")
async def active_workers(
    uuid: Optional[str] = Query(None, description="Filter by worker UUID"),
    host: Optional[str] = Query(None, description="Filter by host name"),
    metric_store=Depends(get_metric_store),
):
    """
    Retrieve info about active workers from the metric store.
    """
    if not metric_store:
        raise HTTPException(status_code=500, detail="Metric store is unavailable.")

    query = {"uuid": uuid, "host": host, "type": MetricType.WORKER_INFO}
    worker_statuses = metric_store.get_metrics(**query)

    response = []
    for worker in worker_statuses:
        serialized_worker = worker.serialize(ndigits=2)
        # Show original timestamp
        serialized_worker["timestamp_served"] = worker.timestamp
        response.append(serialized_worker)

    return response


@router.get("/report", summary="Get a full overview")
async def full_telemetry_report(
    request_store=Depends(get_request_store),
    staging=Depends(get_staging),
    auth=Depends(get_auth),
    metric_store=Depends(get_metric_store),
):
    """
    Retrieves an aggregated 'big picture': service status,
    active requests, active workers, etc.
    """
    # Service status
    service_status_data = {
        "request_store": request_store.collect_metric_info(),
        "staging": staging.collect_metric_info(),
        "auth": auth.collect_metric_info(),
        "metric_store": metric_store.collect_metric_info() if metric_store else None,
    }

    # Active requests
    active_requests = []
    active_statuses = [
        StatusEnum.WAITING,
        StatusEnum.UPLOADING,
        StatusEnum.QUEUED,
        StatusEnum.PROCESSING,
    ]
    for st in active_statuses:
        query = {"status": st}
        active_requests += request_store.get_requests(**query)

    # Active workers
    worker_statuses = []
    if metric_store:
        worker_statuses = metric_store.get_metrics(type=MetricType.WORKER_INFO)

    # Combine
    return {
        "service_status": service_status_data,
        "active_requests": [r.serialize() for r in active_requests],
        "active_workers": [w.serialize(ndigits=2) for w in worker_statuses],
    }


@router.get("/metrics", summary="Retrieve usage metrics")
async def usage_metrics(
    _=Depends(metrics_auth),
    format: str = Query("prometheus", description="Output format: prometheus or json"),
    metric_store=Depends(get_metric_store),
):
    """
    Endpoint exposing usage metrics in various formats.
    """
    try:
        if not is_usage_enabled():
            raise TelemetryUsageDisabled("Telemetry usage is disabled")

        now = datetime.now(timezone.utc)
        cache_expiry_seconds = config.get("telemetry", {}).get("usage", {}).get("cache_expiry_seconds", 30)

        # Fetch user requests
        user_requests = await get_cached_usage_metrics(
            metric_store=metric_store,
            cache_expiry_seconds=cache_expiry_seconds,
        )

        # Load timeframes from config
        time_frames = get_usage_timeframes_from_config()

        # Calculate metrics
        metrics = calculate_usage_metrics(user_requests, time_frames, now)

        # Format output
        return format_output(metrics, time_frames, format)

    except TelemetryUsageDisabled as e:
        logger.warning(e)
        raise HTTPException(status_code=403, detail=str(e))

    except (TelemetryConfigError, RequestFetchError, MetricCalculationError, OutputFormatError) as e:
        logger.error(e)
        raise HTTPException(status_code=500, detail=str(e))

    except Exception as e:
        logger.error(f"Unexpected error in telemetry usage endpoint: {e}")
        raise HTTPException(status_code=500, detail="An unexpected error occurred")
