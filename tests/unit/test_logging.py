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
import re

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
        record = logging.LogRecord("polytope_server.tests.unit", 10, "/hello/world", 500, "Test Message", None, None)

        # Normal record
        result = formatter.format(record)
        assert re.match(
            r'[\s\S]*{[\s\S]*"lvl"[\s\S]*:[\s\S]*"DEBUG"[\s\S]*,[\s\S]*"msg"[\s\S]*:[\s\S]*"Test Message"[\s\S]*,[\s\S]*"pth"[\s\S]*:[\s\S]*"/hello/world:500"[\s\S]*,[\s\S]*"src"[\s\S]*:[\s\S]*"polytope_server.tests.unit"[\s\S]*}[\s\S]*',  # noqa
            result,
        )

        # Record with valid extra info
        record.request_id = "hello"
        result = formatter.format(record)
        assert re.match(
            r'[\s\S]*{[\s\S]*"lvl"[\s\S]*:[\s\S]*"DEBUG"[\s\S]*,[\s\S]*"msg"[\s\S]*:[\s\S]*"Test Message"[\s\S]*,[\s\S]*"pth"[\s\S]*:[\s\S]*"/hello/world:500"[\s\S]*,[\s\S]*"request_id"[\s\S]*:[\s\S]*"hello"[\s\S]*,[\s\S]*"src"[\s\S]*:[\s\S]*"polytope_server.tests.unit"[\s\S]*}[\s\S]*',  # noqa
            result,
        )

        # Record with invalid extra info (wrong type)
        record.request_id = 1234
        with pytest.raises(TypeError):
            result = formatter.format(record)
        del record.request_id

        # Record with unknown extra info (silently ignores extra)
        record.unknown_extra_arg = "hello"
        result = formatter.format(record)
        assert re.match(
            r'[\s\S]*{[\s\S]*"lvl"[\s\S]*:[\s\S]*"DEBUG"[\s\S]*,[\s\S]*"msg"[\s\S]*:[\s\S]*"Test Message"[\s\S]*,[\s\S]*"pth"[\s\S]*:[\s\S]*"/hello/world:500"[\s\S]*,[\s\S]*"src"[\s\S]*:[\s\S]*"polytope_server.tests.unit"[\s\S]*}[\s\S]*',  # noqa
            result,
        )
