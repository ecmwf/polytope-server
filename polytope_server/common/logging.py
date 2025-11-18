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
import json
import logging
import socket

from opentelemetry import baggage
from opentelemetry.context import attach, detach, get_current
from pythonjsonlogger.json import JsonFormatter

from .. import version

DEFAULT_LOGGING_MODE = "json"
DEFAULT_LOGGING_LEVEL = "INFO"
DEFAULT_PRIMARY_FIELDS = ["asctime", "request_id", "message"]
DEFAULT_DEFAULTS = {"app": "polytope-server"}
DEFAULT_IGNORED_FIELDS = [
    "args",
    "msg",
    "msecs",
    "relativeCreated",
    "process",
]


def setup(config, source_name):
    logger = logging.getLogger()
    logger.name = source_name

    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    handler.addFilter(OTelBaggageFilter())

    logging_config = config.get("logging", {})
    mode = logging_config.get("mode", DEFAULT_LOGGING_MODE)
    level = logging_config.get("level", DEFAULT_LOGGING_LEVEL)

    # Get configurable formatting options with defaults
    fmt = logging_config.get("primary_fields", DEFAULT_PRIMARY_FIELDS)
    defaults = logging_config.get("defaults", DEFAULT_DEFAULTS)
    reserved_attrs = logging_config.get("reserved_attrs", DEFAULT_IGNORED_FIELDS)

    handler.setFormatter(
        JsonFormatter(
            fmt=fmt,
            defaults=defaults,
            reserved_attrs=reserved_attrs,
            json_serializer=optional_json_dumps(mode=mode),
            json_indent=2 if mode == "prettyprint" else None,
        )
    )

    logger.addHandler(handler)
    logger.setLevel(level)

    logger.info("Logging Initialized")


def optional_json_dumps(mode="json"):
    """
    Return a json.dumps function that can output a simple console format,
    logserver format, or standard JSON.
    """

    def inner(obj, **json_kwargs):
        if not isinstance(obj, dict):
            return json.dumps(obj, **json_kwargs)

        if mode == "console":
            f"{obj['asctime']} | {obj['message']}"

        if mode == "logserver":
            obj = format_for_logserver(obj)

        return json.dumps(obj, **json_kwargs)

    return inner


@contextlib.contextmanager
def with_baggage_items(items: dict[str, str]):
    """
    Context manager that adds the given baggage items to the current OpenTelemetry context.
    Usage:
        with with_baggage_items({"key1": "value1", "key2": "value2"}) as ctx:
            # baggage will be available here, ctx can be used to pass context explicitly
            pass
    """
    ctx = get_current()
    for key, value in items.items():
        ctx = baggage.set_baggage(key, value, context=ctx)
    token = attach(ctx)
    try:
        yield ctx
    finally:
        detach(token)


def propagate_context(func):
    """
    Decorator that captures OpenTelemetry context (including baggage) and propagates it
    when the function is executed in a different thread (e.g., via ThreadPoolExecutor).

    Usage:
        @propagate_context
        def my_function(arg1, arg2):
            # baggage will be available here even in a different thread
            pass
    """

    def wrapper(*args, **kwargs):
        ctx = get_current()
        token = attach(ctx)
        try:
            return func(*args, **kwargs)
        finally:
            detach(token)

    return wrapper


class OTelBaggageFilter(logging.Filter):
    def filter(self, record):
        ctx = get_current()
        for key, value in baggage.get_all(context=ctx).items():
            setattr(record, key, value)
        return True


# SYSLOG FORMATTING HELPERS

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


def get_local_ip():
    try:
        return socket.gethostbyname(socket.gethostname())
    except Exception:
        return "Unable to get IP"


def calculate_syslog_priority(logging_level):
    severity = LOGGING_TO_SYSLOG_SEVERITY.get(logging_level, 7)  # Default to LOG_DEBUG if level is not found
    priority = (LOCAL7 << 3) | severity
    return priority


def format_for_logserver(record: dict):
    result = {
        "asctime": record.get("asctime", "")[:-4],
        "hostname": record.get("hostname", socket.gethostname()),
        "process": record.get("process", ""),
        "thread": record.get("thread", ""),
        "name": record.get("name", ""),
        "filename": record.get("filename", ""),
        "lineno": record.get("lineno", ""),
        "levelname": record.get("levelname", ""),
        "message": record.get("message", ""),
    }
    software_info = {
        "software": "polytope-server",
        "swVersion": version.__version__,
        "ip": get_local_ip(),
    }
    result["origin"] = software_info

    # Ensure extra fields are in the message
    message_content = {"message": result["message"]}
    for field in record.keys():
        if field not in result:
            message_content[field] = record[field]
    result["message"] = json.dumps(message_content, indent=None)

    # Add syslog facility
    result["syslog_facility"] = LOCAL7
    # Add syslog severity
    result["syslog_severity"] = LOGGING_TO_SYSLOG_SEVERITY.get(record.get("levelno"), 7)
    # Add syslog priority
    result["syslog_priority"] = calculate_syslog_priority(record.get("levelno"))

    return result
