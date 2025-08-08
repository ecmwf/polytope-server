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

import pickle

import pytest

from polytope_server.common.user import User


class Test:
    def setup_method(self, method):
        pass

    def test_user_equality(self):
        user1 = User("joebloggs", "realm1")
        user1.attributes["extra_info"] = "realm1_specific_id"

        user2 = User("joebloggs", "realm1")
        user2.attributes["extra_info"] = "realm1_specific_id"

        user3 = User("joebloggs", "realm2")
        user3.attributes["extra_info"] = "realm1_specific_id"

        assert user1 == user2
        assert user1.id == user2.id
        assert user1 != user3
        assert user1.id != user3.id

        # only realm + username is required for equality
        user2.attributes["extra_info"] = "something_else"
        assert user1 == user2

    def test_user_must_have_username_realm(self):
        with pytest.raises(AttributeError):
            User("bill")
        with pytest.raises(AttributeError):
            User(realm="earth")

    def test_user_immutable(self):
        # Username and realm are immutable
        user = User("jane", "realm_a")
        with pytest.raises(AttributeError):
            user.realm = "realm_b"
        with pytest.raises(AttributeError):
            user.username = "janette"

        with pytest.raises(AttributeError):
            user.id = "abc"

        # Roles and attributes are mutable
        user.roles = ["changed"]
        user.attributes = {"changed"}

    def test_user_pickles(self):
        user = User("jane", "realm_a")
        pickled = pickle.dumps(user, protocol=-1)
        unpickled = pickle.loads(pickled)
        assert user == unpickled
        assert user.id == unpickled.id

    def test_user_serialize(self):
        user1 = User("joebloggs", "realm1")
        user1.attributes["extra_info"] = "realm1_specific_id"
        d = user1.serialize()
        user2 = User(from_dict=d)
        assert user1 == user2
        user3 = User(from_dict=d)
        assert user1 == user3
