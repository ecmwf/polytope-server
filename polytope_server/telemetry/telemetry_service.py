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
from contextlib import asynccontextmanager

from fastapi import FastAPI


class TelemetryService:
    def __init__(self, config):
        self.config = config
        self.handler_dict = {
            "fastapi": "FastAPIHandler",
        }

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        from .dependencies import initialize_resources

        resources = initialize_resources(self.config)

        # Attach resources to the app state
        app.state.resources = resources
        yield

    def load_handler(self):
        handler_type = self.config.get("telemetry", {}).get("handler", "fastapi")
        if handler_type not in self.handler_dict:
            raise ValueError(f"Handler type '{handler_type}' is not supported.")

        handler_module_name = f"polytope_server.telemetry.{handler_type}_handler"
        handler_class_name = self.handler_dict[handler_type]

        handler_module = importlib.import_module(handler_module_name)
        return getattr(handler_module, handler_class_name)

    def create_handler(self):
        handler_class = self.load_handler()
        return handler_class().create_handler(lifespan=self.lifespan)

    def create_app(self) -> FastAPI:
        # Log the configuration
        sanitized_config = self.sanitize_config(self.config)
        logging.info("Loaded configuration: %s", sanitized_config)

        # Create and return the app
        return self.create_handler()

    def run(self, app: FastAPI):
        import uvicorn

        telemetry_config = self.config.get("telemetry", {})
        host = telemetry_config.get("bind_to", "0.0.0.0")
        port = telemetry_config.get("port", 8000)

        logging.info("Starting telemetry service on %s:%d...", host, port)
        uvicorn.run(app, host=host, port=port)

    @staticmethod
    def sanitize_config(raw_config):
        """Sanitize the configuration to remove sensitive fields."""
        sanitized = {}
        sensitive_keys = ["secret_key", "access_key", "password"]

        # Convert the raw_config into a dictionary if it has a .get() method
        if hasattr(raw_config, "get"):
            config_dict = {key: raw_config.get(key) for key in raw_config.config.keys()}
        elif isinstance(raw_config, dict):
            config_dict = raw_config
        else:
            raise TypeError("Unsupported configuration format")

        for key, section in config_dict.items():
            if isinstance(section, dict):  # Only sanitize if it's a dictionary
                sanitized[key] = {k: ("********" if k in sensitive_keys else v) for k, v in section.items()}
            else:
                sanitized[key] = section  # Leave non-dict sections as-is

        return sanitized
