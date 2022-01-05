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

import importlib
import logging
from abc import ABC, abstractmethod

from ..common import queue as polytope_queue
from ..common.auth import AuthHelper
from ..common.caching import cache
from ..common.identity import create_identity
from ..common.keygenerator import create_keygenerator
from ..common.metric_store import create_metric_store
from ..common.request_store import create_request_store
from ..common.staging import create_staging


class TelemetryHandler(ABC):
    @abstractmethod
    def create_handler(self):
        pass

    @abstractmethod
    def run_server(self):
        pass


class Telemetry:
    def __init__(self, config):
        self.config = config

        telemetry_config = config.get("telemetry", {})

        self.server_type = telemetry_config.get("server", "gunicorn")

        self.handler_type = telemetry_config.get("handler", "flask")
        self.handler_dict = {
            "flask": "FlaskHandler",
            "restplus": "RestplusHandler",
            "falcon": "FalconHandler",
        }

        self.host = telemetry_config.get("bind_to", "localhost")
        self.port = telemetry_config.get("port", "6000")

    def run(self):

        request_store = create_request_store(self.config.get("request_store"), self.config.get("metric_store"))

        keygenerator = create_keygenerator(self.config.get("api-keys", {}).get("generator", None))

        staging = create_staging(self.config.get("staging"))
        identity = create_identity(self.config.get("identity"))

        metric_store = None
        if self.config.get("metric_store"):
            metric_store = create_metric_store(self.config.get("metric_store"))

        queue = polytope_queue.create_queue(self.config.get("queue"))
        auth = AuthHelper(self.config)

        handler_module = importlib.import_module("polytope_server.telemetry." + self.handler_type + "_handler")
        handler_class = getattr(handler_module, self.handler_dict[self.handler_type])()
        handler = handler_class.create_handler(
            request_store,
            keygenerator,
            staging,
            identity,
            metric_store,
            queue,
            auth,
            cache,
        )

        logging.info("Starting telemetry service...")
        handler_class.run_server(handler, self.server_type, self.host, self.port)
