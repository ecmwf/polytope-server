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

import datetime
import json
import logging
import socket

from .. import version

indexable_fields = {"request_id": str}


class LogFormatter(logging.Formatter):
    def __init__(self, mode):
        super(LogFormatter, self).__init__()
        self.mode = mode

    def format(self, record):
        # timezone-aware datetime object
        utc_time = datetime.datetime.fromtimestamp(record.created, datetime.timezone.utc)
        formatted_time = utc_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]

        result = {
            "asctime": formatted_time,
            "hostname": getattr(record, "hostname", socket.gethostname()),
            "process": record.process,
            "thread": record.thread,
            "name": record.name,
            "filename": record.filename,
            "lineno": record.lineno,
            "levelname": record.levelname,
            "message": record.getMessage(),
        }

        if self.mode == "console":
            return result["asctime"] + " | " + result["message"]
        else:
            # following adds extra fields to the log message
            for name, typ in indexable_fields.items():
                if hasattr(record, name):
                    val = getattr(record, name)
                    if isinstance(val, typ):
                        result[name] = val
                    else:
                        raise TypeError("Extra information with key {} is expected to be of type {}".format(name, typ))
            if self.mode == "logserver":
                # Get the local IP address
                try:
                    local_ip = socket.gethostbyname(socket.gethostname())
                except Exception as e:
                    local_ip = "Unable to get IP"
                # software name
                software = "polytope-server"
                # software version
                swVersion = version.__version__
                # construct the origin for logserver
                result["origin"] = {"software": software, "swVersion": swVersion, "ip": local_ip}
                # Ensuring single line output
                return json.dumps(result, indent=None)
            elif self.mode == "prettyprint":
                return json.dumps(result, indent=2)
            else:
                return json.dumps(result, indent=0)


def setup(config, source_name):
    logger = logging.getLogger()
    logger.name = source_name
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)

    mode = config.get("logging", {}).get("mode", "json")
    level = config.get("logging", {}).get("level", "INFO")

    handler.setFormatter(LogFormatter(mode))
    logger.addHandler(handler)
    logger.setLevel(level)

    logger.info("Logging Initialized")
