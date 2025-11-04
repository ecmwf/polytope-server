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
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from prometheus_client import CONTENT_TYPE_LATEST

from ..common.metric import MetricType
from ..common.metric_calculator.base import MetricCalculator
from .config import config
from .dependencies import (
    get_metric_calculator,
    get_metric_store,
    get_request_store,
    metrics_auth,
)
from .enums import StatusEnum
from .exceptions import (
    OutputFormatError,
    RequestFetchError,
    TelemetryConfigError,
    TelemetryUsageDisabled,
)
from .helpers import (
    format_output_aggregated,
    get_usage_metrics_aggregated,
    get_usage_timeframes_from_config,
    is_usage_enabled,
    obfuscate_apikey,
)
from .renderer import (
    render_counters,
    render_proc_hist,
    render_req_duration_hist,
    render_unique_users,
)
from .telemetry_utils import parse_window

logger = logging.getLogger(__name__)

# Prefix for all telemetry endpoints
router = APIRouter(prefix="/telemetry/v1")

DEFAULT_WINDOW = "5m"


@router.get("/application-metrics", summary="Windowed application metrics")
async def application_metrics(
    window: str = Query("5m", description="Time window, e.g., 5m, 1h"),
    sections: Optional[str] = Query("counters,histograms", description="Comma-separated sections: counters,histograms"),
    metric_calculator: MetricCalculator = Depends(get_metric_calculator),
):
    try:
        win_secs = parse_window(window, default_seconds=300.0)
        selected = set((sections or "").split(","))
        want_counters = "counters" in selected or sections is None
        want_histograms = "histograms" in selected or sections is None

        lines: List[str] = []
        if want_counters:
            lines += render_counters(metric_calculator, win_secs)
        if want_histograms:
            lines += render_req_duration_hist(metric_calculator, win_secs)
            lines += render_proc_hist(metric_calculator, win_secs)

        # Unique users always useful and cheap;
        # Calculating over standard windows: 5m, 1h, 1d, 3d as garbage collector removes old entries
        lines += render_unique_users(metric_calculator, [300, 3600, 86400, 259200])

        data = "\n".join(lines) + ("\n" if lines else "")
        return Response(content=data, media_type=CONTENT_TYPE_LATEST)
    except Exception as e:
        logger.exception("Unexpected error in /application-metrics: %s", e)
        raise HTTPException(status_code=500, detail="Unexpected error")


@router.get("/", summary="List API routes (optional)")
async def list_endpoints():
    """
    Optional 'index' endpoint for enumerating possible sub-routes.
    """
    return ["health", "requests", "users/{user_id}/requests", "metrics"]


@router.get("/health", summary="Health check endpoint")
async def health():
    """
    Simple endpoint to verify server is up and running.
    """
    return {"message": "Polytope telemetry server is alive"}


@router.get("/requests", summary="Retrieve requests")
async def all_requests(
    status: Optional[StatusEnum] = Query(None),
    id: Optional[str] = Query(None),
    limit: Optional[int] = Query(None, ge=0, description="Max items; 0 or None means no limit"),
    include_trace: bool = Query(False),
    request_store=Depends(get_request_store),
    metric_store=Depends(get_metric_store),
):
    try:
        rows = request_store.list_requests(
            status=status.value if status else None,
            req_id=id,
            limit=limit,
        )
        # include trace only when a single id is specified
        if include_trace and id and metric_store:
            metrics = metric_store.get_metrics(type=MetricType.REQUEST_STATUS_CHANGE, request_id=id)
            trace = [m.serialize() for m in metrics]
        else:
            trace = None

        out = []
        for r in rows:
            # Obfuscate API key (in-place)
            if config.get("telemetry", {}).get("obfuscate_apikeys", False):
                attrs = ((r.get("user") or {}).get("attributes")) or {}
                if "ecmwf-apikey" in attrs:
                    attrs["ecmwf-apikey"] = obfuscate_apikey(attrs["ecmwf-apikey"])
            if trace is not None and r.get("id") == id:
                r["trace"] = trace
            out.append(r)
        return out
    except Exception as e:
        logger.exception("Error in /requests: %s", e)
        raise HTTPException(status_code=500, detail="Failed to retrieve requests")


@router.get("/users/{user_id}/requests", summary="Get requests by user")
async def user_requests(
    user_id: str,
    status: Optional[StatusEnum] = Query(None, description="Filter by status"),
    limit: Optional[int] = Query(None, ge=0, description="Max items; 0 or None means no limit"),
    request_store=Depends(get_request_store),
):
    try:
        rows = request_store.list_requests_by_user(
            user_id=user_id,
            status=status.value if status else None,
            limit=limit,
        )
        for r in rows:
            if config.get("telemetry", {}).get("obfuscate_apikeys", False):
                attrs = ((r.get("user") or {}).get("attributes")) or {}
                if "ecmwf-apikey" in attrs:
                    attrs["ecmwf-apikey"] = obfuscate_apikey(attrs["ecmwf-apikey"])
        return rows
    except Exception as e:
        logger.exception("Error in /users/{user_id}/requests: %s", e)
        raise HTTPException(status_code=500, detail="Failed to retrieve user requests")


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

        # Load timeframes from config
        time_frames = get_usage_timeframes_from_config()

        # Fetch aggregated metrics
        metrics = await get_usage_metrics_aggregated(
            metric_store=metric_store,
            time_frames=time_frames,
            now=now,
        )

        # Format output
        return format_output_aggregated(metrics, time_frames, format)

    except TelemetryUsageDisabled as e:
        logger.warning(e)
        raise HTTPException(status_code=403, detail=str(e))

    except (TelemetryConfigError, RequestFetchError, OutputFormatError) as e:
        logger.error(e)
        raise HTTPException(status_code=500, detail=str(e))

    except Exception as e:
        logger.error(f"Unexpected error in telemetry usage endpoint: {e}")
        raise HTTPException(status_code=500, detail="An unexpected error occurred")
