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

import json
import logging
import os

from polytope_mars.api import PolytopeMars

import yaml

from polytope.utility.exceptions import PolytopeError

from . import datasource


class PolytopeDataSource(datasource.DataSource):
    def __init__(self, config):
        self.config = config
        self.type = config["type"]
        assert self.type == "polytope"
        self.match_rules = config.get("match", {})
        self.patch_rules = config.get("patch", {})
        self.output = None

        # Create a temp file to store gribjump config
        self.config_file = "/tmp/gribjump.yaml"
        with open(self.config_file, "w") as f:
            f.write(yaml.dump(self.config["gribjump_config"]))
        self.config["datacube"]["config"] = self.config_file
        os.environ["GRIBJUMP_CONFIG_FILE"] = self.config_file

        self.polytope_options = self.config.get("polytope-options", {})
        self.polytope_mars = PolytopeMars(self.config)

        logging.info("Set up gribjump")

    def get_type(self):
        return self.type

    def archive(self, request):
        raise NotImplementedError()

    def retrieve(self, request):
        r = yaml.safe_load(request.user_request)
        logging.info(r)

        try:
            self.output = self.polytope_mars.extract(r)
            self.output = json.dumps(self.output).encode("utf-8")
        except PolytopeError as e:
            self.output = json.dumps({"error": str(e)}).encode("utf-8")
        # logging.info(self.output)
        return True

    def result(self, request):
        logging.info("Getting result")
        yield self.output

    def match(self, request):

        r = yaml.safe_load(request.user_request) or {}

        for k, v in self.match_rules.items():
            # Check that all required keys exist
            if k not in r:
                raise Exception("Request does not contain expected key {}".format(k))

            # ... and check the value of other keys
            v = [v] if isinstance(v, str) else v

            if r[k] not in v:
                raise Exception("got {} : {}, but expected one of {}".format(k, r[k], v))

            # Finally check that there is a feature specified in the request
            if "feature" not in r:
                raise Exception("Request does not contain expected key 'feature'")

    def destroy(self, request) -> None:
        pass

    def mime_type(self) -> str:
        return "application/prs.coverage+json"
