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

import copy

import pytest

import polytope_server.common.request as request
import polytope_server.common.request_store as request_store
from polytope_server.common.user import User


@pytest.mark.basic
class Test:
    def setup_method(self, method):

        self.config = copy.deepcopy(pytest.polytope_config)

        self.config["request_store"]["mongodb"]["collection"] = "test_requests"
        self.request_store_config = self.config.get("request_store")

        self.mongodb_config = self.request_store_config.get("mongodb")
        assert "test" in self.mongodb_config.get("collection")

        self.request_store = request_store.create_request_store(self.request_store_config)
        self.request_store.wipe()

        self.user1 = User("one", "realm1")
        self.user1.attributes["extra"] = "abc"
        self.user2 = User("two", "realm2")
        self.user3 = User("three", "realm3")

    def teardown_method(self, method):
        self.request_store.wipe()

    def test_request_store_is_type_mongo(self):
        assert self.request_store.get_type() == "mongodb"

    def test_request_store_add_request(self):
        r = request.PolytopeRequest(user=self.user1)
        r.verb = request.Verb.RETRIEVE
        r.status = request.Status.QUEUED
        assert r.user == self.user1
        self.request_store.add_request(r)
        r2 = self.request_store.get_request(r.id)
        assert r2.user == r.user
        assert r2.user == self.user1
        assert r2.verb == request.Verb.RETRIEVE
        assert r2.status == request.Status.QUEUED
        assert r.verb == r2.verb

    def test_request_store_add_request_duplicate_fails(self):
        r = request.PolytopeRequest(user=self.user1)
        self.request_store.add_request(r)
        with pytest.raises(ValueError):
            self.request_store.add_request(r)

    def test_request_store_remove_request(self):
        r = request.PolytopeRequest(user=self.user1)
        self.request_store.add_request(r)
        assert self.request_store.get_request(r.id) is not None
        self.request_store.remove_request(r.id)
        assert self.request_store.get_request(r.id) is None

    def test_request_store_get_requests(self):
        r1 = request.PolytopeRequest(user=self.user1, collection="hello", status=request.Status.PROCESSED)
        self.request_store.add_request(r1)
        r2 = request.PolytopeRequest(user=self.user2, collection="hello", content_length=10)
        self.request_store.add_request(r2)
        r3 = request.PolytopeRequest(user=self.user3, collection="hello2")
        self.request_store.add_request(r3)

        results = self.request_store.get_requests(user=self.user1)
        assert len(results) == 1
        assert results[0].id == r1.id

        results = self.request_store.get_requests(id=r3.id)
        assert len(results) == 1
        assert results[0].id == r3.id

        results = self.request_store.get_requests(ascending="timestamp")
        assert len(results) == 3
        assert results[0] == r1
        assert results[1] == r2
        assert results[2] == r3

        results = self.request_store.get_requests(descending="timestamp", collection="hello")
        assert len(results) == 2
        assert results[0] == r2
        assert results[1] == r1

        # get with enum
        results = self.request_store.get_requests(status=request.Status.PROCESSED)
        assert len(results) == 1

        # key is an int
        results = self.request_store.get_requests(content_length=10)
        assert len(results) == 1

        # cannot get ascending and descending
        with pytest.raises(ValueError):
            results = self.request_store.get_requests(descending="timestamp", ascending="user")

        # cannot get unknown key
        with pytest.raises(KeyError):
            results = self.request_store.get_requests(descending="not_a_key")
        with pytest.raises(KeyError):
            results = self.request_store.get_requests(descending="not_a_key")
        with pytest.raises(KeyError):
            results = self.request_store.get_requests(not_a_key="a")

        # with limits
        results = self.request_store.get_requests(limit=2)
        assert len(results) == 2

        results = self.request_store.get_requests(descending="timestamp", limit=2)
        assert len(results) == 2
        assert results[0] == r3
        assert results[1] == r2

    def test_request_store_update_request(self):
        r1 = request.PolytopeRequest(user=self.user1)
        id = r1.id
        self.request_store.add_request(r1)
        r2 = self.request_store.get_request(id)
        assert r2 == r1

        r2.user.attributes["test"] = "updated"
        self.request_store.update_request(r2)

        r3 = self.request_store.get_request(id)
        assert r3.id == id
        assert r3.user.attributes["test"] == "updated"

    def test_request_store_wipe(self):
        assert len(self.request_store.get_requests()) == 0
        self.request_store.add_request(request.PolytopeRequest())
        self.request_store.add_request(request.PolytopeRequest())
        self.request_store.add_request(request.PolytopeRequest())
        assert len(self.request_store.get_requests()) == 3
        self.request_store.wipe()
        assert len(self.request_store.get_requests()) == 0
