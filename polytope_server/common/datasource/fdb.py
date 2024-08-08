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
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import yaml
from dateutil.relativedelta import relativedelta

from ..caching import cache
from . import datasource


class FDBDataSource(datasource.DataSource):
    def __init__(self, config):
        self.config = config
        self.fdb_config = self.config["config"]
        self.type = config["type"]
        assert self.type == "fdb"
        self.match_rules = config.get("match", {})
        self.patch_rules = config.get("patch", {})
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

        r = yaml.safe_load(request.user_request)
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

    def match(self, request):

        r = yaml.safe_load(request.user_request) or {}

        for k, v in self.match_rules.items():

            # An empty match rule means that the key must not be present
            if v is None or len(v) == 0:
                if k in r:
                    raise Exception("Request containing key '{}' is not allowed".format(k))
                else:
                    continue  # no more checks to do

            # Check that all required keys exist
            if k not in r and not (v is None or len(v) == 0):
                raise Exception("Request does not contain expected key '{}'".format(k))

            # Process date rules
            if k == "date":
                self.date_check(r["date"], v)
                continue

            # ... and check the value of other keys

            v = [v] if isinstance(v, str) else v
            if r[k] not in v:
                raise Exception("got {} : {}, but expected one of {}".format(k, r[k], v))

    def destroy(self, request) -> None:
        pass

    def mime_type(self) -> str:
        return "application/x-grib"

    # def fdb_list ( self, loaded_request ):
    #     try:
    #         output = subprocess.check_output(
    #        "FDB5_CONFIG={} fdb-list --json {}".format( self.fdb_config,self.convert_to_mars_request(loaded_request)),
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

    def convert_to_mars_request(self, loaded_request):
        request_str = ""
        for k, v in loaded_request.items():
            if isinstance(v, (list, tuple)):
                v = "/".join(str(x) for x in v)
            else:
                v = str(v)
            request_str = request_str + k + "=" + v + ","
        return request_str[:-1]

    def check_single_date(self, date, offset, offset_fmted):

        # Date is relative (0 = now, -1 = one day ago)
        if str(date)[0] == "0" or str(date)[0] == "-":
            date_offset = int(date)
            dt = datetime.today() + timedelta(days=date_offset)

            if dt >= offset:
                raise Exception("Date is too recent, expected < {}".format(offset_fmted))
            else:
                return

        # Absolute date YYYMMDD
        try:
            dt = datetime.strptime(date, "%Y%m%d")
        except ValueError:
            raise Exception("Invalid date, expected real date in YYYYMMDD format")
        if dt >= offset:
            raise Exception("Date is too recent, expected < {}".format(offset_fmted))
        else:
            return

    def date_check(self, date, offsets):
        """Process special match rules for DATE constraints"""

        date = str(date)

        # Default date is -1
        if len(str(date)) == 0:
            date = "-1"

        now = datetime.today()
        offset = now + relativedelta(**dict(offsets))
        offset_fmted = offset.strftime("%Y%m%d")

        split = str(date).split("/")

        # YYYYMMDD
        if len(split) == 1:
            self.check_single_date(split[0], offset, offset_fmted)
            return True

        # YYYYMMDD/to/YYYYMMDD -- check end and start date
        # YYYYMMDD/to/YYYYMMDD/by/N -- check end and start date
        if len(split) == 3 or len(split) == 5:

            if split[1].casefold() == "to".casefold():

                if len(split) == 5 and split[3].casefold() != "by".casefold():
                    raise Exception("Invalid date range")

                self.check_single_date(split[0], offset, offset_fmted)
                self.check_single_date(split[2], offset, offset_fmted)
                return True

        # YYYYMMDD/YYYYMMDD/YYYYMMDD/... -- check each date
        for s in split:
            self.check_single_date(s, offset, offset_fmted)

        return True
