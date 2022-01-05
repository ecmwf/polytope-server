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

import subprocess

import pytest
import yaml

from polytope_server.common.datasource import create_datasource
from polytope_server.common.request import Request, Verb


@pytest.mark.skipif(subprocess.call(["which", "fdb"]) != 0, reason="fdb not in path")
class TestDataSourceFDB:
    def setup_method(self, method):

        self.datasource_config = {"name": "fdb"}

        self.request = Request()

        self.request.user_request = yaml.dump(
            {
                "stream": "oper",
                "levtype": "sfc",
                "param": ["165.128", "166.128", "167.128"],
                "step": "0",
                "time": ["0000", "0600", "1200", "1800"],
                "date": "20150323",
                "type": "an",
                "class": "ei",
                "domain": "g",
                "expver": "0001",
            }
        )

        self.ds = create_datasource(self.datasource_config)

        with open("fdb_test.grib", mode="rb") as file:
            self.dummy_data = file.read()

    def test_datasource_fdb(self):

        # archive
        self.request.verb = Verb.ARCHIVE
        success = self.ds.dispatch(self.request, self.dummy_data)
        assert success

        for i in self.ds.result(self.request):
            # result should be empty
            raise Exception()

        # retrieve
        self.request.verb = Verb.RETRIEVE
        success = self.ds.dispatch(self.request, None)
        assert success

        data = b""
        for i in self.ds.result(self.request):
            data += i

        assert data == self.dummy_data
