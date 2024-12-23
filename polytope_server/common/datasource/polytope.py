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

import copy
import json
import logging
import os

import yaml
from polytope_feature.utility.exceptions import PolytopeError
from polytope_mars.api import PolytopeMars

from ..schedule import SCHEDULE_READER
from . import coercion, datasource


class PolytopeDataSource(datasource.DataSource):
    def __init__(self, config):
        self.config = config
        self.type = config["type"]
        assert self.type == "polytope"
        self.match_rules = config.get("match", {})
        self.req_single_keys = config.get("options", {}).pop("req_single_keys", [])
        self.patch_rules = config.get("patch", {})
        self.defaults = config.get("defaults", {})
        self.extra_required_role = config.get("extra_required_role", {})
        self.obey_schedule = config.get("obey_schedule", False)
        self.output = None

        # Create a temp file to store gribjump config
        self.config_file = "/tmp/gribjump.yaml"
        with open(self.config_file, "w") as f:
            f.write(yaml.dump(self.config.pop("gribjump_config")))
        self.config["datacube"]["config"] = self.config_file
        os.environ["GRIBJUMP_CONFIG_FILE"] = self.config_file

        logging.info("Set up gribjump")

    def get_type(self):
        return self.type

    def archive(self, request):
        raise NotImplementedError()

    def check_extra_roles(self, request) -> bool:

        # if the user has any of the extra roles, they are allowed
        realm = request.user.realm
        req_extra_roles = self.extra_required_role.get(realm, [])

        if len(req_extra_roles) == 0:
            return True

        logging.info(f"Checking for user roles in required extra roles: {req_extra_roles}")
        logging.info(f"User roles: {request.user.roles}")

        if any(role in req_extra_roles for role in request.user.roles):
            return True
        else:
            return False

    def retrieve(self, request):
        r = yaml.safe_load(request.user_request)

        r = coercion.Coercion.coerce(r)

        r = self.apply_defaults(r)

        # Downstream expects MARS-like format of request
        for key in r:
            if isinstance(r[key], list):
                r[key] = "/".join(r[key])

        logging.info(r)

        # Set the "pre-path" for this request
        pre_path = {}
        for k, v in r.items():
            v = v.split("/") if isinstance(v, str) else v
            if k in self.req_single_keys:
                if isinstance(v, list):
                    if len(v) == 1:
                        v = v[0]
                        pre_path[k] = v

        polytope_mars_config = copy.deepcopy(self.config)
        polytope_mars_config["options"]["pre_path"] = pre_path

        polytope_mars = PolytopeMars(
            polytope_mars_config,
            log_context={
                "user": request.user.realm + ":" + request.user.username,
                "id": request.id,
            },
        )

        try:
            self.output = polytope_mars.extract(r)
            self.output = json.dumps(self.output).encode("utf-8")
        except PolytopeError as e:
            raise Exception("Polytope Feature Extraction Error: {}".format(e.message))

        return True

    def result(self, request):
        logging.info("Getting result")
        yield self.output

    def match(self, request):

        if not self.check_extra_roles(request):
            raise Exception("not authorized to access this data.")

        r = yaml.safe_load(request.user_request) or {}

        r = coercion.Coercion.coerce(r)

        r = self.apply_defaults(r)

        logging.info("Coerced and patched request: {}".format(r))

        # Check that there is a feature specified in the request
        if "feature" not in r:
            raise Exception("request does not contain key 'feature'")

        # Check that there is only one value if required
        for k, v in r.items():
            if k in self.req_single_keys:
                v = [v] if isinstance(v, str) else v
                if len(v) > 1:
                    raise Exception("key '{}' cannot accept a list yet. This feature is planned.".format(k))
                elif len(v) == 0:
                    raise Exception("Expected a value for key {}".format(k))

        for k, v in self.match_rules.items():
            # Check that all required keys exist
            if k not in r:
                raise Exception("request does not contain key '{}'".format(k))

            # ... and check the value of other keys
            v = [v] if isinstance(v, str) else v

            # Check if all values in the request match the required values
            req_value_list = r[k] if isinstance(r[k], list) else [r[k]]
            for req_value in req_value_list:
                if req_value not in v:
                    raise Exception("got {}: {}, not one of {}".format(k, req_value, v))

        # Downstream expects MARS-like format of request
        for key in r:
            if isinstance(r[key], list):
                r[key] = "/".join(r[key])

        # Check data released
        if SCHEDULE_READER is not None and self.obey_schedule:
            SCHEDULE_READER.check_released_polytope_request(r)

    def destroy(self, request) -> None:
        pass

    def repr(self):
        return self.config.get("repr", "polytope")

    def mime_type(self) -> str:
        return "application/prs.coverage+json"

    def apply_defaults(self, request):
        request = copy.deepcopy(request)
        for k, v in self.defaults.items():
            if k not in request:
                request[k] = v
        return request
