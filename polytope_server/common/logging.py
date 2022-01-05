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

indexable_fields = {"request_id": str}


def setup(config, source_name):

    # Override the default logger

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


class LogFormatter(logging.Formatter):
    def __init__(self, mode):
        super(LogFormatter, self).__init__()
        self.mode = mode

    def format(self, record):

        msg = super(LogFormatter, self).format(record)

        result = {}
        result["ts"] = datetime.datetime.utcnow().isoformat()[:-3] + "Z"
        result["src"] = str(record.name)
        result["lvl"] = str(record.levelname)
        result["pth"] = str("{}:{}".format(record.pathname, record.lineno))
        result["msg"] = str(msg)

        # log accepts extra={} args to eg. logging.debug
        # if the extra arguments match known indexable_fields these are added to the log
        # these strongly-typed fields can be used for indexing of logs

        if self.mode == "console":
            return result["ts"] + " | " + result["msg"]

        else:
            for name, typ in indexable_fields.items():
                if hasattr(record, name):
                    val = getattr(record, name)
                    if isinstance(val, typ):
                        result[name] = val
                    else:
                        raise TypeError("Extra information with key {} is expected to be of type {}".format(name, typ))

            if self.mode == "prettyprint":
                indent = 2
            else:
                indent = 0
            return json.dumps(result, indent=indent, sort_keys=True)
