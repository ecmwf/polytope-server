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

import pytest

from polytope_server.common.datasource import create_datasource
from polytope_server.common.request import PolytopeRequest


class TestDataSourcedummy:
    def setup_method(self, method):
        self.dummy_config = {"name": "dummy", "type": "dummy"}

        self.request = PolytopeRequest()
        self.request.user_request = str(20 * 1024 * 1024)
        self.ds = create_datasource(self.dummy_config)

    def test_datasource_dummy(self):
        assert self.ds.dispatch(self.request, None)
        data = b""
        for x in self.ds.result(self.request):
            data += x
        assert len(data) == 20 * 1024 * 1024

    def test_datasource_dummy_raises_on_not_int(self):
        self.request.user_request = "abc"

        with pytest.raises(Exception):
            self.ds.dispatch(self.request, None)

    def test_datasource_dummy_raises_on_negative(self):
        self.request.user_request = "-100"

        with pytest.raises(Exception):
            self.ds.dispatch(self.request, None)

    def test_datasource_dummy_success_zero_size(self):
        self.request.user_request = "0"

        assert self.ds.dispatch(self.request, None)
        data = b""
        for x in self.ds.result(self.request):
            data += x
        assert len(data) == 0

    def test_datasource_dummy_contains_pattern(self):
        self.request.user_request = "13"
        assert self.ds.dispatch(self.request, None)
        data = b""
        for x in self.ds.result(self.request):
            data += x
        assert len(data) == 13
        assert data == b"xxxxxxxxxxxxx"
