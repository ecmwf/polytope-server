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

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider

from ..common.auth import AuthHelper
from ..common.collection import create_collections
from ..common.identity import create_identity
from ..common.keygenerator import create_keygenerator
from ..common.request_store import create_request_store
from ..common.staging import create_staging

trace.set_tracer_provider(TracerProvider(resource=Resource.create({"service.name": "frontend"})))

tracer = trace.get_tracer(__name__)


class FrontendHandler(ABC):
    @abstractmethod
    def create_handler(self):
        pass

    @abstractmethod
    def run_server(self):
        pass


class Frontend:
    def __init__(self, config):
        self.config = config

        frontend_config = config.get("frontend", {})

        self.server_type = frontend_config.get("server", "gunicorn")

        self.handler_type = frontend_config.get("handler", "flask")
        self.handler_dict = {
            "flask": "FlaskHandler",
        }

        self.host = frontend_config.get("bind_to", "localhost")
        self.port = frontend_config.get("port", "5000")

    def run(self):
        # create instances of authentication, request_store & staging
        request_store = create_request_store(self.config.get("request_store"), self.config.get("metric_store"))

        auth = AuthHelper(self.config)
        apikeygenerator = create_keygenerator(self.config.get("api-keys", {}).get("generator", None))

        staging = create_staging(self.config.get("staging"))
        collections = create_collections(self.config.get("collections"))
        identity = create_identity(self.config.get("identity"))

        handler_module = importlib.import_module("polytope_server.frontend." + self.handler_type + "_handler")
        handler_class = getattr(handler_module, self.handler_dict[self.handler_type])()
        handler = handler_class.create_handler(
            request_store,
            auth,
            staging,
            collections,
            identity,
            apikeygenerator,
            self.config.get("frontend", {}).get("proxy_support", False),
        )

        logging.info("Starting frontend...")
        handler_class.run_server(handler, self.server_type, self.host, self.port)
