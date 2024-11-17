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

from .config import config


def get_settings():
    telemetry_config = config.get("telemetry", {})
    return {
        "server_type": telemetry_config.get("server", "uvicorn"),
        "handler_type": telemetry_config.get("handler", "fastapi"),
        "host": telemetry_config.get("bind_to", "localhost"),
        "port": int(telemetry_config.get("port", "6000")),
    }


def get_request_store():
    from ..common.request_store import create_request_store

    return create_request_store(config.get("request_store"), config.get("metric_store"))


def get_staging():
    from ..common.staging import create_staging

    return create_staging(config.get("staging"))


def get_metric_store():
    from ..common.metric_store import create_metric_store

    metric_store_config = config.get("metric_store")
    return create_metric_store(metric_store_config) if metric_store_config else None


def get_auth():
    from ..common.auth import AuthHelper

    return AuthHelper(config.config)
