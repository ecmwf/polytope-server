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

from copy import deepcopy
from datetime import datetime, timedelta

import pytest  # noqa: F401

from polytope_server.common.datasource import DataSource


@pytest.fixture
def user_request():
    return {
        "stream": "oper",
        "levtype": "sfc",
        "param": "165.128/166.128/167.128",
        "step": "0",
        "time": "00",
        "date": "-32",
        "type": "an",
        "class": "od",
        "expver": "0001",
        "domain": "g",
    }


class TestDataSourceMatching:
    def setup_method(self):

        self.mars_config = {
            "name": "mars",
            "match": {"class": ["od"], "stream": ["oper", "enfo", "something"], "date": "> 30d"},
        }

    def _mock_auth(self, monkeypatch):
        monkeypatch.setattr("polytope_server.common.user.User.has_access", lambda *args, **kwargs: True)

    def test_mars_match_date(self, monkeypatch, user_request):
        # Smoke test: date routing through DataSource.match works for pass and fail
        self._mock_auth(monkeypatch)
        assert "success" == DataSource.match(self.mars_config, user_request, None)
        req = set_request_date(user_request, -5)
        assert "success" != DataSource.match(self.mars_config, req, None)

    def test_mars_match_two_lists(self, monkeypatch, user_request):
        self._mock_auth(monkeypatch)
        req = user_request
        req["stream"] = ["oper"]
        assert "success" == DataSource.match(self.mars_config, req, None)
        req = user_request
        req["stream"] = ["oper", "enfo"]
        assert "success" == DataSource.match(self.mars_config, req, None)
        req = user_request
        req["stream"] = ["oper", "enfo", "something_else"]
        assert "success" != DataSource.match(self.mars_config, req, None)

    def test_mars_match_rule_formatting(self, monkeypatch, user_request):
        self._mock_auth(monkeypatch)
        config = deepcopy(self.mars_config)

        # list of date rules works
        config["match"]["date"] = [">30d", "< 40d"]
        assert "success" == DataSource.match(config, user_request, None)

        # single number rules works
        config["match"]["step"] = 0
        assert "success" == DataSource.match(config, user_request, None)
        config["match"]["step"] = [0, 6]
        assert "success" == DataSource.match(config, user_request, None)
        config["match"]["step"] = [6]
        assert "success" != DataSource.match(config, user_request, None)


def set_request_date(user_request, days_offset):
    date = datetime.today() + timedelta(days=days_offset)
    user_request["date"] = date.strftime("%Y%m%d")
    return user_request
