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

from datetime import datetime, timedelta

import pytest
import yaml

import polytope_server.common.config as polytope_config
from polytope_server.common.datasource import create_datasource
from polytope_server.common.request import Request


class TestMarsDataSource:
    def setup_method(self, method):
        polytope_config.global_config["datasources"]["mars"] = {
            "type": "mars",
            "command": "mars",
            "tmp_dir": "/home/polytope/data",
            "match": None,
        }

        self.mars_config = {
            "name": "mars",
            "match": {"class": ["od"], "stream": ["oper", "enfo"], "date": "> 30d"},
        }
        self.request = Request()
        self.request.user_request = yaml.dump(
            {
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
        )

        self.ds = create_datasource(self.mars_config)

    def set_request_date(self, days_offset):
        date = datetime.today() + timedelta(days=days_offset)
        datefmted = date.strftime("%Y%m%d")
        self.request.user_request = yaml.dump(
            {
                "stream": "oper",
                "levtype": "sfc",
                "param": "165.128/166.128/167.128",
                "step": "0",
                "time": "00",
                "date": datefmted,
                "type": "an",
                "class": "od",
                "expver": "0001",
                "domain": "g",
            }
        )

    def set_request_date_range(self, days_offset, days_end_offset, step=1):
        date = datetime.today() + timedelta(days=days_offset)
        datefmted = date.strftime("%Y%m%d")
        date_end = datetime.today() + timedelta(days=days_end_offset)
        date_endfmted = date_end.strftime("%Y%m%d")
        step_string = ""
        if step != 1:
            step_string = "/by/" + str(step)
        self.request.user_request = yaml.dump(
            {
                "stream": "oper",
                "levtype": "sfc",
                "param": "165.128/166.128/167.128",
                "step": "0",
                "time": "00",
                "date": datefmted + "/to/" + date_endfmted + step_string,
                "type": "an",
                "class": "od",
                "expver": "0001",
                "domain": "g",
            }
        )

    def set_request_date_list(self, *days_offset):
        date_string = ""
        for i in days_offset:
            date = datetime.today() + timedelta(days=i)
            date_string += date.strftime("%Y%m%d") + "/"
        self.request.user_request = yaml.dump(
            {
                "stream": "oper",
                "levtype": "sfc",
                "param": "165.128/166.128/167.128",
                "step": "0",
                "time": "00",
                "date": date_string[:-1],
                "type": "an",
                "class": "od",
                "expver": "0001",
                "domain": "g",
            }
        )

    def test_mars_created_correctly(self):
        assert self.ds.match_rules is not None
        self.ds.match(self.request)

    def test_mars_match_date(self):
        self.set_request_date(-5)
        with pytest.raises(Exception):
            self.ds.match(self.request)

    def test_mars_match_date_range(self):
        self.set_request_date_range(-60, -40)
        self.ds.match(self.request)

        self.set_request_date_range(-60, -25)
        with pytest.raises(Exception):
            self.ds.match(self.request)

    def test_mars_match_date_range_step(self):
        self.set_request_date_range(-60, -40, 4)
        self.ds.match(self.request)

        self.set_request_date_range(-60, -25, 4)
        with pytest.raises(Exception):
            self.ds.match(self.request)

    # POLY-171
    def test_mars_match_date_list2(self):
        self.set_request_date_list(-60, -40)
        self.ds.match(self.request)

        self.set_request_date_list(-60, -25)
        with pytest.raises(Exception):
            self.ds.match(self.request)

    # POLY-171
    def test_mars_match_date_list3(self):
        self.set_request_date_list(-60, -40, -35)
        self.ds.match(self.request)

        self.set_request_date_list(-60, -25, -35)
        with pytest.raises(Exception):
            self.ds.match(self.request)

    # POLY-171
    def test_mars_match_date_list4(self):
        self.set_request_date_list(-60, -40, -35, -36)
        self.ds.match(self.request)

        self.set_request_date_list(-60, -25, -35, -36)
        with pytest.raises(Exception):
            self.ds.match(self.request)

    # POLY-171
    def test_mars_match_date_list5(self):
        self.set_request_date_list(-60, -40, -35, -36, -37)
        self.ds.match(self.request)

        self.set_request_date_list(-60, -25, -35, -36, -37)
        with pytest.raises(Exception):
            self.ds.match(self.request)

    def test_mars_match_date_future(self):
        self.set_request_date(365 * 1000)
        with pytest.raises(Exception):
            self.ds.match(self.request)

    def test_mars_match_inverse_date_range_step(self):
        self.set_request_date_range(-40, -60)
        self.ds.match(self.request)

        self.set_request_date_range(-10, -45)
        with pytest.raises(Exception):
            self.ds.match(self.request)
