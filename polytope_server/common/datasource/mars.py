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

import logging
import os
import tempfile
from datetime import datetime, timedelta

import yaml
from dateutil.relativedelta import relativedelta

from ..io.fifo import FIFO
from ..subprocess import Subprocess
from . import datasource


class MARSDataSource(datasource.DataSource):
    def __init__(self, config):
        assert config["type"] == "mars"
        self.type = config.get("type")
        self.command = config.get("command", "/usr/local/bin/mars")
        self.tmp_dir = config.get("tmp_dir", "/tmp")
        self.match_rules = config.get("match", {})

        self.override_mars_email = config.get("override_email")
        self.override_mars_apikey = config.get("override_apikey")

        self.subprocess = None
        self.fifo = None

        if self.match_rules is None:
            self.match_rules = {}

        self.mars_binary = config.get("binary", "mars")

        # Write the mars config
        self.mars_config = config.get("config", {})
        self.mars_home = self.tmp_dir + "/mars-home"
        os.makedirs(self.mars_home + "/etc/mars-client/")
        with open(self.mars_home + "/etc/mars-client/databases.yaml", "w") as f:
            yaml.dump(self.mars_config, f)

    def get_type(self):
        return self.type

    def match(self, request):

        r = yaml.safe_load(request.user_request) or {}

        for k, v in self.match_rules.items():
            # Check that all required keys exist
            if k not in r:
                raise Exception("Request does not contain expected key {}".format(k))
            # Process date rules
            if k == "date":
                self.date_check(r["date"], v)
                continue
            # ... and check the value of other keys
            v = [v] if isinstance(v, str) else v
            if r[k] not in v:
                raise Exception("got {} : {}, but expected one of {}".format(k, r[k], v))

    def archive(self, request):
        raise NotImplementedError("Archiving not implemented for MARS data source")

    def retrieve(self, request):

        # Open a FIFO for MARS output
        self.fifo = FIFO("MARS-FIFO-" + request.id)

        # Parse the user request as YAML, and add the FIFO as target
        r = yaml.safe_load(request.user_request) or {}
        r["target"] = '"' + self.fifo.path + '"'

        # Make a temporary file for the request
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            self.request_file = tmp.name
            logging.info("Writing request to tempfile {}".format(self.request_file))
            tmp.write(self.convert_to_mars_request("retrieve", r).encode())

        # Call MARS
        self.subprocess = Subprocess()
        self.subprocess.run(
            cmd=[self.command, self.request_file],
            cwd=os.path.dirname(__file__),
            env=self.make_env(request),
        )

        # Poll until the FIFO has been opened by MARS, watch in case the spawned process dies before opening the FIFO
        try:
            while self.subprocess.running():
                if self.fifo.ready():
                    logging.debug("FIFO is ready for reading.")
                    break
            else:
                self.destroy(request)
        except Exception:
            self.destroy(request)
            raise

        return True

    def result(self, request):

        # The FIFO will get EOF if MARS exits unexpectedly, so we will break out of this loop automatically
        for x in self.fifo.data():
            yield x

        self.destroy(request)
        return

    def destroy(self, request):
        try:
            os.unlink(self.request_file)
        except Exception:
            pass
        try:
            self.fifo.delete()
        except Exception:
            pass
        try:
            self.subprocess.finalize(request, "mars -")  # Will raise if non-zero return
        except Exception:
            pass

    def mime_type(self) -> str:
        return "application/x-grib"

    #######################################################

    def make_env(self, request):
        """Make the environment for the MARS subprocess, primarily for setting credentials"""
        try:
            if self.override_mars_email:
                logging.info("Overriding MARS_USER_EMAIL with {}".format(self.override_mars_email))
                mars_user = self.override_mars_email
            else:
                mars_user = request.user.attributes["ecmwf-email"]

            if self.override_mars_apikey:
                logging.info("Overriding MARS_USER_TOKEN with {}".format(self.override_mars_apikey))
                mars_token = self.override_mars_apikey
            else:
                mars_token = request.user.attributes["ecmwf-apikey"]

            env = {
                **os.environ,
                "MARS_USER_EMAIL": mars_user,
                "MARS_USER_TOKEN": mars_token,
                "MARS_HOME": self.mars_home,
                "ECMWF_MARS_COMMAND": self.mars_binary
            }

            logging.info("Accessing MARS on behalf of user {} with token {}".format(mars_user, mars_token))

        except Exception:
            logging.error("MARS request aborted because user does not have associated ECMWF credentials")
            raise Exception()

        return env

    def convert_to_mars_request(self, verb, user_request):
        """Converts Python dictionary to a MARS request string"""
        request_str = verb
        for k, v in user_request.items():
            if isinstance(v, (list, tuple)):
                v = "/".join(str(x) for x in v)
            else:
                v = str(v)
            request_str = request_str + "," + k + "=" + v
        return request_str

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
