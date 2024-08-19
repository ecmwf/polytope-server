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

os.environ["GRIBJUMP_HOME"] = "/opt/fdb-gribjump"

import tempfile
from pathlib import Path

import yaml

import polytope

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
        self.fdb_config = self.config["fdb-config"]

        self.non_sliceable = self.config.get("non-sliceable", None)
        assert self.non_sliceable is not None

        self.check_schema()

        os.environ["FDB5_CONFIG"] = json.dumps(self.fdb_config)
        os.environ["FDB5_HOME"] = self.config.get("fdb_home", "/opt/fdb-gribjump")
        # forced change

        if "spaces" in self.fdb_config:
            for space in self.fdb_config["spaces"]:
                for root in space["roots"]:
                    os.makedirs(root["path"], exist_ok=True)

        # Set up gribjump
        self.gribjump_config = self.config["gribjump-config"]
        os.makedirs("/home/polytope/gribjump/", exist_ok=True)
        with open("/home/polytope/gribjump/config.yaml", "w") as f:
            json.dump(self.gribjump_config, f)
        os.environ["GRIBJUMP_CONFIG_FILE"] = "/home/polytope/gribjump/config.yaml"
        # self.gj = pygribjump.GribJump()

        # Set up polytope feature extraction library
        self.polytope_options = {
            "values": {"mapper": {"type": "octahedral", "resolution": 1280, "axes": ["latitude", "longitude"]}},
            "date": {"merge": {"with": "time", "linkers": ["T", "00"]}},
            "step": {"type_change": "int"},
            "number": {"type_change": "int"},
            "longitude": {"cyclic": [0, 360]},
        }

        logging.info("Set up gribjump")

    # todo: remove when we no longer need to set up a valid fdb to use gribjump
    def check_schema(self):

        schema = self.fdb_config.get("schema", None)

        # If schema is empty, leave it empty
        if schema is None:
            return

        # If schema is just a string, then it must be a path already
        if isinstance(self.fdb_config["schema"], str):
            return

        # pull schema from git
        if "git" in schema:

            git_config = schema["git"]
            git_path = Path(git_config["path"])

            local_path = (
                Path(tempfile.gettempdir())
                .joinpath(git_config["remote"].replace(":", ""))
                .joinpath(git_config["branch"])
                .joinpath(git_path)
            )

            Path(local_path.parent).mkdir(parents=True, exist_ok=True)

            with open(local_path, "w+") as f:
                f.write(
                    self.git_download_schema(
                        git_config["remote"],
                        git_config["branch"],
                        git_path.parent,
                        git_path.name,
                    )
                )

        self.fdb_config["schema"] = str(local_path)

    @cache(lifetime=5000000)
    def git_download_schema(self, remote, branch, git_dir, git_file):
        call = "git archive --remote {} {}:{} {} | tar -xO {}".format(
            remote, branch, str(git_dir), str(git_file), str(git_file)
        )
        logging.debug("Fetching FDB schema from git with call: {}".format(call))
        output = subprocess.check_output(call, shell=True)
        return output.decode("utf-8")

    def get_type(self):
        return self.type

    def archive(self, request):
        raise NotImplementedError()

    def retrieve(self, request):
        r = yaml.safe_load(request.user_request)
        logging.info(r)

        # We take the static config from the match rules of the datasource
        self.polytope_config = {}
        for k in self.non_sliceable:
            self.polytope_config[k] = r[k]

        assert len(self.polytope_config) > 0

        logging.info(self.polytope_config)
        logging.info(self.polytope_options)
        from polytope_mars.api import PolytopeMars

        p = PolytopeMars(self.polytope_config, self.polytope_options)

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
