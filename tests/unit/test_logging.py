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

import io
import json
import logging
import socket

from pythonjsonlogger.json import JsonFormatter


class Test:
    def setup_method(self, method):
        # Clear any existing handlers
        logger = logging.getLogger()
        logger.handlers = []
        logger.setLevel(logging.NOTSET)

    def teardown_method(self, method):
        # Clean up handlers
        logger = logging.getLogger()
        logger.handlers = []
        logger.setLevel(logging.NOTSET)

    def test_logging_name(self):
        logger = logging.getLogger()
        assert logger.name == "polytope_server.tests.unit"

    def test_logging_setup(self):
        """Test that logging.setup() configures the logger correctly."""
        import polytope_server.common.logging as mylogging

        config = {"logging": {"mode": "json", "level": "DEBUG"}}
        mylogging.setup(config, "test_source")

        logger = logging.getLogger()
        assert logger.name == "test_source"
        assert logger.level == logging.DEBUG
        assert len(logger.handlers) > 0

    def test_logging_json_output(self):
        """Test that logs are formatted as JSON."""
        import polytope_server.common.logging as mylogging

        # Create a string buffer to capture log output
        log_capture = io.StringIO()
        handler = logging.StreamHandler(log_capture)
        handler.setLevel(logging.DEBUG)

        # Configure the formatter
        formatter = mylogging.optional_json_dumps(mode="json")

        handler.setFormatter(
            JsonFormatter(
                fmt=["asctime", "request_id", "message"],
                defaults={"app": "polytope-server"},
                reserved_attrs=["args", "msg", "msecs", "relativeCreated", "process"],
                json_serializer=formatter,
            )
        )

        logger = logging.getLogger("test_logger")
        logger.handlers = []
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        # Log a message
        logger.info("Test message")  # , extra={"request_id": "12345"})

        # Get the output and parse as JSON
        log_output = log_capture.getvalue().strip()
        log_json = json.loads(log_output)

        assert log_json["message"] == "Test message"
        assert log_json["app"] == "polytope-server"
        assert "asctime" in log_json
        # assert log_json["request_id"] == "12345"

    def test_otel_baggage_filter(self):
        """Test that OpenTelemetry baggage is added to log records."""
        import polytope_server.common.logging as mylogging

        log_capture = io.StringIO()
        handler = logging.StreamHandler(log_capture)
        handler.setLevel(logging.DEBUG)
        handler.addFilter(mylogging.OTelBaggageFilter())

        formatter = mylogging.optional_json_dumps(mode="json")

        handler.setFormatter(
            JsonFormatter(
                fmt=["asctime", "request_id", "message"],
                defaults={"app": "polytope-server"},
                reserved_attrs=["args", "msg", "msecs", "relativeCreated", "process"],
                json_serializer=formatter,
            )
        )

        logger = logging.getLogger("test_baggage_logger")
        logger.handlers = []
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        # Log with baggage
        with mylogging.with_baggage_items({"request_id": "test-123"}):
            logger.info("Test with baggage")

        log_output = log_capture.getvalue().strip()
        log_json = json.loads(log_output)

        assert log_json["request_id"] == "test-123"
        assert log_json["message"] == "Test with baggage"

    def test_logserver_format(self):
        """Test the format_for_logserver function."""
        import polytope_server.common.logging as mylogging

        # The function expects a dict with 'levelno' as a key, not an attribute
        record_dict = {
            "asctime": "2025-11-14 12:00:00,123",
            "name": "test_logger",
            "filename": "test.py",
            "lineno": 42,
            "levelname": "INFO",
            "levelno": logging.INFO,
            "message": "Test message",
            "process": 1234,
            "thread": 5678,
            "request_id": "test-123",
        }

        result = mylogging.format_for_logserver(record_dict)

        # Check required fields
        assert result["asctime"] == "2025-11-14 12:00:00"  # Note: last 3 chars (milliseconds) truncated
        assert result["hostname"] == socket.gethostname()
        assert result["name"] == "test_logger"
        assert result["filename"] == "test.py"
        assert result["lineno"] == 42
        assert result["levelname"] == "INFO"
        assert result["process"] == 1234
        assert result["thread"] == 5678

        # Check syslog fields
        assert result["syslog_facility"] == 23  # LOCAL7
        assert result["syslog_severity"] == 6  # INFO
        assert result["syslog_priority"] == (23 << 3) | 6

        # Check origin
        assert result["origin"]["software"] == "polytope-server"
        assert "swVersion" in result["origin"]
        assert "ip" in result["origin"]

        # Check that message is JSON-encoded with extra fields
        message_content = json.loads(result["message"])
        assert message_content["message"] == "Test message"
        assert message_content["request_id"] == "test-123"
