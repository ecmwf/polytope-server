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
import subprocess
import tempfile
from pathlib import Path

from ..caching import cache
from . import datasource

# from .datasource import convert_to_mars_request


class FDBDataSource(datasource.DataSource):
    def __init__(self, config):
        self.config = config
        self.fdb_config = self.config["config"]
        self.type = config["type"]
        assert self.type == "fdb"
        self.output = None

        self.check_schema()

        os.environ["FDB5_CONFIG"] = json.dumps(self.fdb_config)
        os.environ["FDB_CONFIG"] = json.dumps(self.fdb_config)
        os.environ["FDB5_HOME"] = self.config.get("fdb_home", "/opt/fdb")
        os.environ["FDB_HOME"] = self.config.get("fdb_home", "/opt/fdb")
        import pyfdb

        self.fdb = pyfdb.FDB()

        if "spaces" in self.fdb_config:
            for space in self.fdb_config["spaces"]:
                for root in space["roots"]:
                    os.makedirs(root["path"], exist_ok=True)

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

    @cache(lifetime=500)
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

        # could add a check that the request is a singular object (does not contain)

        # r = yaml.safe_load(request.user_request)
        self.fdb.archive(self.input_data)
        self.fdb.flush()
        self.output = None
        return True

    def retrieve(self, request):

        r = copy.deepcopy(request.coerced_request)
        logging.info(r)
        self.output = self.fdb.retrieve(r)
        return True

    def result(self, request):

        if not self.output:
            return

        while True:
            d = self.output.read(2 * 1024 * 1024)
            if d:
                yield d
            else:
                break

        self.output.close()
        return

    def destroy(self, request) -> None:
        pass

    def mime_type(self) -> str:
        return "application/x-grib"

    # def fdb_list ( self, loaded_request ):
    #     try:
    #         output = subprocess.check_output(
    #        "FDB5_CONFIG={} fdb-list --json {}".format( self.fdb_config,convert_to_mars_request(loaded_request)),
    #             shell=True,
    #             stderr=subprocess.STDOUT,
    #             executable="/bin/bash"
    #         )
    #     except subprocess.CalledProcessError as err:
    #         logging.exception("fdb archive failed with: {}".format( err.output.decode() ))
    #         raise Exception(err.output.decode())

    #     result = json.loads(output.decode('utf-8'))
    #     return len(result) > 0

    #######################################################
