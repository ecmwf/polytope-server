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

"""
    Tests the KeyGenerator abstraction for creating api keys, and the apikey_authentication handler
    for validating those keys.

"""

import copy
import fnmatch

import pytest

from polytope_server.common.auth import AuthHelper
from polytope_server.common.exceptions import (
    EndpointNotImplemented,
    ForbiddenRequest,
    UnauthorizedRequest,
)
from polytope_server.common.keygenerator import (
    keygenerator,
    mongodb_keygenerator,
    none_keygenerator,
)
from polytope_server.common.user import User


@pytest.mark.api_keys_generator_type_mongodb
class TestMongoKeyGenerator:
    def setup_method(self, method):

        config = copy.deepcopy(pytest.polytope_config_auth)

        self.keygen = keygenerator.create_keygenerator(config.get("api-keys").get("generator"))
        self.realm = self.keygen.realms[0]
        assert isinstance(self.keygen, mongodb_keygenerator.MongoKeyGenerator)

        self.username = "some_user"
        self.password = "some_password"
        self.roles = ["some-role"]
        self.realm = "some-realm"

        # Create an authenticator which will validate the user with username:password
        config["authentication"] = []  # no authenticators required, will use the api-keys authenticator
        config["authorization"] = []  # not testing authorization

        self.auth = AuthHelper(config)

    def teardown_method(self, method):
        pass

    def test_keygenerator_create_key(self):

        # Usually a user can only create a key if they are already authenticated,
        # here we can just invent a user

        user = User("some_username", self.realm)
        key = self.keygen.create_key(user)

        assert key
        assert key.timestamp
        assert fnmatch.fnmatch(key.key, "????????-????-????-????-????????????")

        user = User("some_username", "invalid_realm")
        with pytest.raises(ForbiddenRequest):
            self.keygen.create_key(user)

    def test_keygenerator_create_key_forbidden_realm(self):
        user = User("some_username", "forbidden_realm")
        with pytest.raises(ForbiddenRequest):
            self.keygen.create_key(user)

    def test_keygenerator_authenticate_with_key(self):

        user = User("some_username", self.realm)
        key = self.keygen.create_key(user)
        auth_header = "Bearer {}".format(key.key)
        auth_user = self.auth.authenticate(auth_header)

        assert auth_user.username == user.username
        assert auth_user.realm == user.realm

    def test_keygenerator_authenticate_with_invalid_key(self):
        key = "not_really_a_key"
        auth_header = "Bearer {}".format(key)
        with pytest.raises(UnauthorizedRequest):
            self.auth.authenticate(auth_header)

    def test_keygenerator_authenticate_with_invalid_auth_type(self):
        key = "not_really_a_key"
        auth_header = "Invalid {}".format(key)
        with pytest.raises(UnauthorizedRequest):
            self.auth.authenticate(auth_header)


@pytest.mark.basic
class TestNoneGenerator(TestMongoKeyGenerator):
    def setup_method(self, method):

        config = copy.deepcopy(pytest.polytope_config_auth)

        self.keygen = keygenerator.create_keygenerator()  # defaults to none
        assert isinstance(self.keygen, none_keygenerator.NoneKeyGenerator)

        self.username = "some_user"
        self.password = "some_password"
        self.roles = ["some-role"]
        self.realm = "some-realm"

        # Create an authenticator which will validate the user with username:password
        config["authentication"] = []  # no authenticators required, will use the api-keys authenticator
        config["authorization"] = []  # not testing authorization

        self.auth = AuthHelper(config)

    def teardown_method(self, method):
        pass

    def test_keygenerator_create_key(self):
        user = User("some_username", self.realm)
        with pytest.raises(EndpointNotImplemented):
            self.keygen.create_key(user)

    def test_keygenerator_create_key_forbidden_realm(self):
        pass

    def test_keygenerator_authenticate_with_key(self):
        pass

    def test_keygenerator_authenticate_with_invalid_key(self):
        key = "not_really_a_key"
        auth_header = "Bearer {}".format(key)
        with pytest.raises(UnauthorizedRequest):
            self.auth.authenticate(auth_header)

    def test_keygenerator_authenticate_with_invalid_auth_type(self):
        key = "not_really_a_key"
        auth_header = "Invalid {}".format(key)
        with pytest.raises(UnauthorizedRequest):
            self.auth.authenticate(auth_header)
