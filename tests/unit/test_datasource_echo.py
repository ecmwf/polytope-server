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

from polytope_server.common.datasource import create_datasource
from polytope_server.common.request import Request


class TestDataSourceecho:
    def setup_method(self, method):
        self.echo_config = {"name": "echo", "type": "echo"}

        self.request = Request()
        self.request.user_request = "Hello World!" * 1000
        self.ds = create_datasource(self.echo_config)

    def test_datasource_echo(self):
        assert self.ds.dispatch(self.request, None)
        data = b""
        for x in self.ds.result(self.request):
            data += x
        assert len(data.decode()) == 12 * 1000

    def test_datasource_echo_binary(self):
        self.request.user_request = b"abc"
        assert self.ds.dispatch(self.request, None)
        data = b""
        for x in self.ds.result(self.request):
            data += x
        assert len(data) == 3

    def test_datasource_echo_zero_size(self):
        self.request.user_request = ""

        assert self.ds.dispatch(self.request, None)
        data = b""
        for x in self.ds.result(self.request):
            data += x
        assert len(data) == 0

    def test_datasource_echo_contains_pattern(self):
        self.request.user_request = "hello world!"
        assert self.ds.dispatch(self.request, None)
        data = b""
        for x in self.ds.result(self.request):
            data += x
        assert len(data) == 12
        assert data == b"hello world!"
