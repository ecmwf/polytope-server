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
import subprocess

from conflator import Conflator
from polytope_mars.api import PolytopeMars
from polytope_mars.config import PolytopeMarsConfig
# os.environ["GRIBJUMP_HOME"] = "/opt/fdb-gribjump"

import tempfile
from pathlib import Path

import polytope
import yaml

from ..caching import cache
from . import datasource


class PolytopeDataSource(datasource.DataSource):
    def __init__(self, config):
        self.config = config
        self.type = config["type"]
        assert self.type == "polytope"
        self.match_rules = config.get("match", {})
        self.patch_rules = config.get("patch", {})
        self.output = None

        # still need to set up fdb
        # self.fdb_config = self.config["fdb-config"]

        # self.non_sliceable = self.config.get("non-sliceable", None)
        # assert self.non_sliceable is not None

        self.polytope_options = self.config.get("polytope-options", {})

        # self.check_schema()

        # # Set up gribjump
        # self.gribjump_config = self.config["gribjump-config"]
        # os.makedirs("/home/polytope/gribjump/", exist_ok=True)
        # with open("/home/polytope/gribjump/config.yaml", "w") as f:
        #     json.dump(self.gribjump_config, f)
        # os.environ["GRIBJUMP_CONFIG_FILE"] = "/home/polytope/gribjump/config.yaml"
        # self.gj = pygribjump.GribJump()

        # Set up polytope feature extraction library
        # self.polytope_options = {
        #     "values": {"mapper": {"type": "octahedral", "resolution": 1280, "axes": ["latitude", "longitude"]}},
        #     "date": {"merge": {"with": "time", "linkers": ["T", "00"]}},
        #     "step": {"type_change": "int"},
        #     "number": {"type_change": "int"},
        #     "longitude": {"cyclic": [0, 360]},
        # }

        logging.info("Set up gribjump")

    def get_type(self):
        return self.type

    def archive(self, request):
        raise NotImplementedError()

    def retrieve(self, request):
        r = yaml.safe_load(request.user_request)
        logging.info(r)

        # # We take the static config from the match rules of the datasource
        # self.polytope_config = {}
        # for k in self.non_sliceable:
        #     self.polytope_config[k] = r[k]

        # assert len(self.polytope_config) > 0

        # logging.info(self.polytope_config)
        # logging.info(self.polytope_options)

        conf = Conflator(app_name="polytope_mars", model=PolytopeMarsConfig).load()
        cf = conf.model_dump()
        cf["options"] = self.polytope_options

        p = PolytopeMars(cf, None)

        self.output = p.extract(r)
        self.output = json.dumps(self.output).encode("utf-8")
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
