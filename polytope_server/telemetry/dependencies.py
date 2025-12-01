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
from typing import Optional

from fastapi import Depends, HTTPException, Request, status

from ..common.auth import AuthHelper
from ..common.exceptions import ForbiddenRequest, UnauthorizedRequest
from ..common.metric_calculator.base import MetricCalculator
from ..common.metric_store import create_metric_store
from ..common.request_store import create_request_store
from ..common.staging import create_staging
from ..common.user import User
from .config import config
from .helpers import TelemetryLogSuppressor

logger = logging.getLogger(__name__)

# This is to avoid spamming the logs with the same auth message
log_suppression_ttl = config.get("telemetry", {}).get("basic_auth", {}).get("log_suppression_ttl", 300)

_telemetry_log_suppressor = TelemetryLogSuppressor(log_suppression_ttl)


def _load_telemetry_allowed_roles() -> list[str]:
    """
    Read allowed telemetry roles from config.

    Example:

      telemetry:
        allowed_roles:
          - polytope-telemetry
          - polytope-admin
    """
    tele_cfg = config.get("telemetry", {}) or {}
    raw = tele_cfg.get("allowed_roles")

    # Default if nothing configured
    if raw is None:
        return ["polytope-telemetry", "polytope-admin"]

    if isinstance(raw, str):
        # also allow "role1,role2" as a single string
        return [p.strip() for p in raw.split(",") if p.strip()]

    if isinstance(raw, (list, tuple, set)):
        return [str(r) for r in raw]

    return ["polytope-telemetry", "polytope-admin"]


TELEMETRY_ALLOWED_ROLES = _load_telemetry_allowed_roles()


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
    Dependency that enforces Basic Auth for telemetry endpoints,
    delegating verification to the main authentication stack (Auth-o-tron
    via AuthHelper, or any other configured backend).

    It still respects `telemetry.basic_auth.enabled` and
    `telemetry.basic_auth.log_suppression_ttl` from the config.
    """
    basic_auth_cfg = config.get("telemetry", {}).get("basic_auth", {})
    if not basic_auth_cfg.get("enabled", False):
        # Auth disabled for telemetry metrics
        return

    auth_header = request.headers.get("Authorization") or ""
    if not auth_header:
        logger.warning("Missing Authorization header for telemetry.")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Basic Auth credentials",
            headers={"WWW-Authenticate": "Basic"},
        )

    # forcing basic auth
    if not auth_header.startswith("Basic "):
        logger.warning("Invalid Auth scheme (expected Basic) for telemetry.")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid Auth scheme (Basic required)",
            headers={"WWW-Authenticate": "Basic"},
        )

    auth = get_auth(request)

    try:
        # Pass the full Authorization header; AuthHelper/Authotron will
        # validate credentials and return a User object or raise.
        user = auth.authenticate(auth_header)
        # Avoid spamming logs – per-user TTL
        _telemetry_log_suppressor.log_if_needed(user.id)
        return user
    except (ForbiddenRequest, UnauthorizedRequest) as e:
        detail_msg = str(e).strip() or "Invalid credentials"
        logger.warning(f"Telemetry Auth failed: {detail_msg}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=detail_msg,
            headers={"WWW-Authenticate": "Basic"},
        )


def require_telemetry_user(user: Optional[User] = Depends(metrics_auth)) -> User:
    """
    For endpoints that require telemetry roles.

    - Reuses metrics_auth for Basic Auth + Auth-o-tron.
    - Then enforces TELEMETRY_ALLOWED_ROLES via User.has_access().
    - If auth is disabled or missing (metrics_auth returned None), we still
      require credentials → 401.
    """
    if user is None:
        # metrics_auth did not return a user (no/misconfigured auth) – reject
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required for telemetry requests",
            headers={"WWW-Authenticate": "Basic"},
        )

    if not user.has_access(TELEMETRY_ALLOWED_ROLES):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Telemetry access denied",
        )
    return user
