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

import pytest


class Test:
    def setup_method(self, method):
        pass

    def teardown_method(self, method):
        pass

    def test_logging_name(self):

        logger = logging.getLogger()
        assert logger.name == "polytope_server.tests.unit"

    def test_logging_format(self):

        import polytope_server.common.logging as mylogging

        formatter = mylogging.LogFormatter(mode="json")

        # Normal record
        log_message = {
            "name": "polytope_server.tests.unit",
            "filename": "world",
            "lineno": 500,
            "levelname": "DEBUG",
            "message": "Test Message",
        }
        record = logging.LogRecord(
            log_message["name"],
            logging.getLevelNamesMapping()[log_message["levelname"]],
            log_message["filename"],
            log_message["lineno"],
            log_message["message"],
            None,
            None,
        )
        result = json.loads(formatter.format(record))
        for k in log_message.keys():
            assert result[k] == log_message[k]

        # Record with valid extra info
        record.request_id = "hello"
        log_message["request_id"] = "hello"
        result = json.loads(formatter.format(record))
        for k in log_message.keys():
            assert result[k] == log_message[k]

        # Record with invalid extra info (wrong type)
        record.request_id = 1234
        with pytest.raises(TypeError):
            result = formatter.format(record)
        del record.request_id
        del log_message["request_id"]

        # Record with unknown extra info (silently ignores extra)
        record.unknown_extra_arg = "hello"
        result = json.loads(formatter.format(record))
        for k in log_message.keys():
            assert result[k] == log_message[k]
        assert "unknown_extra_arg" not in result
