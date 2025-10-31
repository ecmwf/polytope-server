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

import contextlib
import datetime
import json
import logging
import socket

from opentelemetry import baggage
from opentelemetry.context import attach, detach, get_current
from pythonjsonlogger import jsonlogger

from .. import version

# Constants for syslog facility and severity
LOCAL7 = 23

# Mapping Python logging levels to syslog severity levels
LOGGING_TO_SYSLOG_SEVERITY = {
    logging.CRITICAL: 2,  # LOG_CRIT
    logging.ERROR: 3,  # LOG_ERR
    logging.WARNING: 4,  # LOG_WARNING
    logging.INFO: 6,  # LOG_INFO
    logging.DEBUG: 7,  # LOG_DEBUG
    logging.NOTSET: 7,  # LOG_DEBUG (default)
}

# Indexable fields
INDEXABLE_FIELDS = {"request_id": str}
DEFAULT_LOGGING_MODE = "json"
DEFAULT_LOGGING_LEVEL = "INFO"


class OTelBaggageFilter(logging.Filter):
    def filter(self, record):
        setattr(record, "asc_time", format_time(record))
        setattr(record, "app", "polytope-server")
        ctx = get_current()
        for key, value in baggage.get_all(context=ctx).items():
            setattr(record, key, value)
        return True


class LogFormatter(logging.Formatter):
    def __init__(self, mode):
        super().__init__()
        self.mode = mode

    def get_hostname(self, record):
        return getattr(record, "hostname", socket.gethostname())

    def get_local_ip(self):
        try:
            return socket.gethostbyname(socket.gethostname())
        except Exception:
            return "Unable to get IP"

    def add_indexable_fields(self, record, result):
        for name, expected_type in INDEXABLE_FIELDS.items():
            if hasattr(record, name):
                value = getattr(record, name)
                if isinstance(value, expected_type):
                    result[name] = value
                else:
                    raise TypeError(f"Extra information with key '{name}' is expected to be of type '{expected_type}'")

    def calculate_syslog_priority(self, logging_level):
        severity = LOGGING_TO_SYSLOG_SEVERITY.get(logging_level, 7)  # Default to LOG_DEBUG if level is not found
        priority = (LOCAL7 << 3) | severity
        return priority

    def format_for_logserver(self, record, result):
        software_info = {
            "software": "polytope-server",
            "swVersion": version.__version__,
            "ip": self.get_local_ip(),
        }
        result["origin"] = software_info

        # Ensure indexable fields are in the message
        message_content = {"message": result["message"]}
        for field in INDEXABLE_FIELDS:
            if field in result:
                message_content[field] = result[field]
        result["message"] = json.dumps(message_content, indent=None)
        # Add syslog facility
        result["syslog_facility"] = LOCAL7
        # Add syslog severity
        result["syslog_severity"] = LOGGING_TO_SYSLOG_SEVERITY.get(record.levelno, 7)
        # Add syslog priority
        result["syslog_priority"] = self.calculate_syslog_priority(record.levelno)

        return json.dumps(result, indent=None)

    def format(self, record):
        formatted_time = format_time(record)
        result = {
            "asctime": formatted_time,
            "process": record.process,
            "thread": record.thread,
            "name": record.name,
            "filename": record.filename,
            "lineno": record.lineno,
            "levelname": record.levelname,
        }

        self.add_indexable_fields(record, result)

        if self.mode == "console":
            return f"{result['asctime']} | {result['message']}"
        if self.mode == "logserver":
            return self.format_for_logserver(record, result)
        if self.mode == "prettyprint":
            return json.dumps(
                result, indent=2, ensure_ascii=False
            )  # Added ensure_ascii=False for correct Unicode display
        if self.mode == "json":
            return json.dumps(result, indent=None)
        # default to json
        return json.dumps(result, indent=None)


def format_time(record):
    utc_time = datetime.datetime.fromtimestamp(record.created, datetime.timezone.utc)
    return utc_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]


def setup(config, source_name):
    logger = logging.getLogger()
    logger.name = source_name

    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    handler.addFilter(OTelBaggageFilter())

    mode = config.get("logging", {}).get("mode", DEFAULT_LOGGING_MODE)
    level = config.get("logging", {}).get("level", DEFAULT_LOGGING_LEVEL)

    if mode == "json":
        reserved_attrs = ["msg", "msecs", "relativeCreated", "process"]
        if level != "DEBUG":
            reserved_attrs += [
                "filename",
                "funcName",
                "lineno",
                "module",
                "processName",
                "thread",
                "threadName",
            ]
        handler.setFormatter(jsonlogger.JsonFormatter(reserved_attrs=reserved_attrs))
    else:
        handler.setFormatter(LogFormatter(mode))

    logger.addHandler(handler)
    logger.setLevel(level)

    logger.info("Logging Initialized")


@contextlib.contextmanager
def with_baggage_items(items: dict[str, str]):
    ctx = get_current()
    for key, value in items.items():
        ctx = baggage.set_baggage(key, value, context=ctx)
    token = attach(ctx)
    try:
        yield
    finally:
        detach(token)
