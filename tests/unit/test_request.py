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

import pytest

import polytope_server.common.request as request
from polytope_server.common.user import User


class Test:
    def setup_method(self, method):
        self.user = User("joebloggs", "realm1")
        self.user.attributes["extra_info"] = "realm1_specific_id"

    def test_request(self):
        r = request.Request(user=self.user, verb=request.Verb.RETRIEVE)
        assert r.verb == request.Verb.RETRIEVE
        assert r.verb != "retrieve"  # enum should not evaluate directly
        assert r.user == self.user
        assert r.user.username == "joebloggs"
        assert r.user.realm == "realm1"
        assert r.user.attributes["extra_info"] == "realm1_specific_id"

    def test_request_equality(self):
        r1 = request.Request(user=self.user, verb=request.Verb.RETRIEVE)
        r2 = request.Request(user=self.user, verb=request.Verb.RETRIEVE)
        assert r1 != r2
        r2.id = r1.id
        r2.timestamp = r1.timestamp
        assert r1 == r2

    def test_request_cant_add_attribute(self):
        r1 = request.Request()
        with pytest.raises(AttributeError):
            r1.new_attr = "test"

    def test_request_serialization(self):
        r1 = request.Request(user=self.user, verb=request.Verb.RETRIEVE)
        d = r1.serialize()
        assert d["verb"] == "retrieve"
        assert d["user"] == self.user.serialize()
        r2 = request.Request(from_dict=d)
        r3 = request.Request()
        r3.deserialize(d)
        assert r2 == r1
        assert r3 == r1
        assert r2.verb == request.Verb.RETRIEVE
        assert r3.status == r1.status
        assert r2.user == self.user

    def test_request_copy(self):
        r1 = request.Request(user=self.user, verb=request.Verb.RETRIEVE)
        r2 = deepcopy(r1)
        assert r1 == r2
