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

from fastapi import HTTPException, Request, status

from ..common.auth import AuthHelper
from ..common.authentication.plain_authentication import PlainAuthentication
from ..common.exceptions import ForbiddenRequest
from ..common.metric_calculator.base import MetricCalculator
from ..common.metric_store import create_metric_store
from ..common.request_store import create_request_store
from ..common.staging import create_staging
from .config import config
from .helpers import TelemetryLogSuppressor

logger = logging.getLogger(__name__)

# This is to avoid spamming the logs with the same auth message
log_suppression_ttl = config.get("telemetry", {}).get("basic_auth", {}).get("log_suppression_ttl", 300)
_telemetry_log_suppressor = TelemetryLogSuppressor(log_suppression_ttl)

plain_auth = PlainAuthentication(
    name="telemetry_basic_auth",
    realm="telemetry_realm",
    config={"users": config.get("telemetry", {}).get("basic_auth", {}).get("users", [])},
)


def initialize_resources(config):
    """Initialize and return all resources."""
    return {
        "request_store": create_request_store(config.get("request_store"), config.get("metric_store")),
        "staging": create_staging(config.get("staging")),
        "metric_store": create_metric_store(config.get("metric_store")) if config.get("metric_store") else None,
        "auth": AuthHelper(config.config),
    }


def get_request_store(request: Request):
    return request.app.state.resources["request_store"]


def get_staging(request: Request):
    return request.app.state.resources["staging"]


def get_metric_store(request: Request):
    return request.app.state.resources["metric_store"]


def get_auth(request: Request):
    return request.app.state.resources["auth"]


def get_metric_calculator(request: Request) -> MetricCalculator:
    requeststore = get_request_store(request)
    return requeststore.metric_calculator


def metrics_auth(request: Request):
    """
    FastAPI dependency that:
      - Reads the 'Authorization' header.
      - If Basic Auth is disabled, returns immediately.
      - If it's enabled, calls 'plain_auth.authenticate'.
      - Translates 'ForbiddenRequest' -> FastAPI's HTTPException.
    """
    basic_auth_cfg = config.get("telemetry", {}).get("basic_auth", {})
    if not basic_auth_cfg.get("enabled", False):
        # Basic Auth is disabled; skip credential checks
        return

    auth_header = request.headers.get("Authorization")
    if not auth_header:
        logger.warning("Missing Authorization header for telemetry.")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Basic Auth credentials",
            headers={"WWW-Authenticate": "Basic"},
        )

    if not auth_header.startswith("Basic "):
        logger.warning("Invalid Auth scheme (expected Basic) for telemetry.")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid Auth scheme",
            headers={"WWW-Authenticate": "Basic"},
        )

    encoded_creds = auth_header[len("Basic ") :]

    try:
        user = plain_auth.authenticate(encoded_creds)
        # If this succeeded, we have a valid user
        # Instead of logging directly every time, let the log suppressor decide.
        _telemetry_log_suppressor.log_if_needed(user.id)
    except ForbiddenRequest as e:
        # Ensure we never send an empty detail message
        detail_msg = str(e).strip() or "Invalid credentials"
        logger.warning(f"ForbiddenRequest: {detail_msg}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=detail_msg,
            headers={"WWW-Authenticate": "Basic"},
        )
