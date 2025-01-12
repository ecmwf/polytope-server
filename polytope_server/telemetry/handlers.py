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
from .dependencies import get_auth, get_metric_store, get_request_store, get_staging
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

router = APIRouter()


@router.get("/telemetry/v1", summary="List available telemetry endpoints")
async def list_endpoints():
    return ["test", "summary", "all", "requests", "workers"]


@router.get("/telemetry/v1/test", summary="Check server status")
async def test():
    return {"message": "Polytope telemetry server is alive"}


@router.get("/telemetry/v1/summary", summary="Get service status")
async def service_status(
    request_store=Depends(get_request_store),
    staging=Depends(get_staging),
    auth=Depends(get_auth),
    metric_store=Depends(get_metric_store),
):
    return {
        "request_store": request_store.collect_metric_info(),
        "staging": staging.collect_metric_info(),
        "auth": auth.collect_metric_info(),
        "metric_store": metric_store.collect_metric_info() if metric_store else None,
    }


@router.get("/telemetry/v1/requests", summary="Get all requests")
async def all_requests(
    status: Optional[StatusEnum] = Query(None, description="Filter requests by status"),
    id: Optional[str] = Query(None, description="Filter requests by ID"),
    request_store=Depends(get_request_store),
    metric_store=Depends(get_metric_store),
):
    active_statuses = {
        StatusEnum.ACTIVE: [
            StatusEnum.WAITING,
            StatusEnum.UPLOADING,
            StatusEnum.QUEUED,
            StatusEnum.PROCESSING,
        ]
    }

    # Fetch requests based on status
    if status == StatusEnum.ACTIVE:
        statuses = active_statuses[status]
    elif status:
        statuses = [status]
    else:
        # If no status is provided, fetch all requests without filtering by status
        statuses = []

    user_requests = []
    if statuses:
        # Fetch requests for each status in the list
        for status_filter in statuses:
            query = {"status": status_filter, "id": id}
            user_requests += request_store.get_requests(**query)
    else:
        # Fetch all requests without status filter
        user_requests = request_store.get_requests(id=id)

    response_message = []
    for request in user_requests:
        serialized_request = request.serialize()

        if id:
            metrics = metric_store.get_metrics(type=MetricType.REQUEST_STATUS_CHANGE, request_id=id)
            serialized_request["trace"] = [metric.serialize() for metric in metrics]

        serialized_request["user"]["details"] = "**hidden**"

        # Check for attributes and API key
        if config.get("telemetry", {}).get("obfuscate_apikeys", False) and "attributes" in serialized_request["user"]:
            attributes = serialized_request["user"]["attributes"]
            if "ecmwf-apikey" in attributes:
                attributes["ecmwf-apikey"] = obfuscate_apikey(attributes["ecmwf-apikey"])

        response_message.append(serialized_request)

    return response_message


@router.get("/telemetry/v1/requests/user/{user_id}", summary="Get all requests for a user")
async def user_requests(
    user_id: str,
    status: Optional[StatusEnum] = Query(None, description="Filter requests by status"),
    id: Optional[str] = Query(None, description="Filter requests by ID"),
    request_store=Depends(get_request_store),
    metric_store=Depends(get_metric_store),
):
    active_statuses = {
        StatusEnum.ACTIVE: [
            StatusEnum.WAITING,
            StatusEnum.UPLOADING,
            StatusEnum.QUEUED,
            StatusEnum.PROCESSING,
        ]
    }

    # TODO: implement more robust user fetching
    # Now we just fetch all requests and filter by user_id
    # Fetch all requests for the user
    user_requests = request_store.get_requests(id=id)

    # Apply status filtering
    if status == StatusEnum.ACTIVE:
        statuses = active_statuses[status]
    elif status:
        statuses = [status]
    else:
        statuses = []

    if statuses:
        user_requests = [request for request in user_requests if request.status in statuses]

    filtered_requests = []
    for request in user_requests:
        if request.serialize()["user"]["id"] == user_id:
            filtered_requests.append(request)

    # Serialize and enrich with metrics
    response_message = []
    for request in filtered_requests:
        serialized_request = request.serialize()
        metrics = metric_store.get_metrics(type=MetricType.REQUEST_STATUS_CHANGE, request_id=user_id)
        serialized_request["metrics"] = [metric.serialize() for metric in metrics]
        serialized_request["user"]["details"] = "**hidden**"

        # Check for attributes and API key
        if config.get("telemetry", {}).get("obfuscate_apikeys", False) and "attributes" in serialized_request["user"]:
            attributes: Dict[str, Any] = serialized_request["user"]["attributes"]
            if "ecmwf-apikey" in attributes:
                attributes["ecmwf-apikey"] = obfuscate_apikey(attributes["ecmwf-apikey"])

        response_message.append(serialized_request)

    return response_message


@router.get("/telemetry/v1/workers", summary="Get active workers")
async def active_workers(
    uuid: Optional[str] = Query(None, description="Filter workers by UUID"),
    host: Optional[str] = Query(None, description="Filter workers by host"),
    metric_store=Depends(get_metric_store),
):
    if not metric_store:
        raise HTTPException(status_code=500, detail="Metric store is unavailable.")

    query = {"uuid": uuid, "host": host, "type": MetricType.WORKER_INFO}
    worker_statuses = metric_store.get_metrics(**query)

    response_message = []
    for worker in worker_statuses:
        serialized_worker = worker.serialize(ndigits=2)
        serialized_worker["timestamp_served"] = worker.timestamp
        response_message.append(serialized_worker)

    return response_message


@router.get("/telemetry/v1/all", summary="Get all metrics and information")
async def all_metrics(
    request_store=Depends(get_request_store),
    staging=Depends(get_staging),
    auth=Depends(get_auth),
    metric_store=Depends(get_metric_store),
):
    # Service status
    service_status = {
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
    for status in active_statuses:
        query = {"status": status}
        active_requests += request_store.get_requests(**query)

    # Active workers
    worker_statuses = []
    if metric_store:
        worker_statuses = metric_store.get_metrics(type=MetricType.WORKER_INFO)

    # Combine all information
    response_message = {
        "service_status": service_status,
        "active_requests": [request.serialize() for request in active_requests],
        "active_workers": [worker.serialize(ndigits=2) for worker in worker_statuses],
    }

    return response_message


@router.get("/telemetry/v1/usage", summary="Get usage metrics")
async def usage_metrics(
    format: str = Query("prometheus", description="Output format: prometheus or json"),
    metric_store=Depends(get_metric_store),
):
    """
    Endpoint to expose usage metrics in Prometheus or JSON format.
    """
    try:
        # Ensure telemetry usage is enabled
        if not is_usage_enabled():
            raise TelemetryUsageDisabled("Telemetry usage is disabled")

        now = datetime.now(timezone.utc)
        # Intentionally using seconds here as this cache should be short-lived
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
        # Format and return output
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
