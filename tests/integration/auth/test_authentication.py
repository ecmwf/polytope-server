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

import base64
import copy
import os

import pytest

from polytope_server.common.authentication import (
    authentication,
    ecmwfapi_authentication,
    federation_authentication,
    mongodb_authentication,
    plain_authentication,
)
from polytope_server.common.caching import cache
from polytope_server.common.exceptions import ForbiddenRequest
from polytope_server.common.identity import identity


@pytest.mark.authentication_any_type_mongodb
class TestMongoAuthentication:
    def setup_method(self, method):
        config = copy.deepcopy(pytest.polytope_config_auth)

        self.authentication_config = [c for c in config.get("authentication") if c["type"] == "mongodb"][0]
        self.authentication = authentication.create_authentication(self.authentication_config)

        assert isinstance(self.authentication, mongodb_authentication.MongoAuthentication)
        assert self.authentication.users.name == "test-users"

        self.realm = self.authentication_config.get("realm")

        # Create a test user using the identity service
        self.identity = identity.create_identity(config.get("identity"))
        assert self.identity.realm == self.realm
        self.username = "Bill"
        self.password = "Flowerpot"
        self.roles = ["some_role"]
        self.header = self.make_basic_header(self.username, self.password)
        self.identity.wipe()
        self.identity.add_user(self.username, self.password, self.roles)

    def make_basic_header(self, username, password):
        return base64.b64encode("{}:{}".format(username, password).encode()).decode()

    def teardown_method(self, method):
        pass

    def test_authentication_validate(self):
        user = self.authentication.validate(self.header)
        assert user.username == self.username
        assert user.realm == self.realm

    def test_authentication_invalid_credentials(self):
        with pytest.raises(ForbiddenRequest):
            self.authentication.validate("not_valid_user_pass")

    def test_authentication_invalid_auth_type(self):
        with pytest.raises(ForbiddenRequest):
            self.authentication.validate("not_valid_token")


@pytest.mark.api_keys_authenticator_type_mongoapikey
class TestMongoApiKeyAuthentication(TestMongoAuthentication):
    """This class is tested in test_apikeys.py"""

    def setup_method(self, method):
        pass

    def teardown_method(self, method):
        pass

    def test_authentication_validate(self):
        pass

    def test_authentication_invalid_credentials(self):
        pass

    def test_authentication_invalid_auth_type(self):
        pass


@pytest.mark.authentication_any_type_ecmwfapi
class TestECMWFAuthentication(TestMongoAuthentication):
    def setup_method(self, method):
        config = copy.deepcopy(pytest.polytope_config_auth)

        cache.init(config.get("caching", {}))

        self.authentication_config = [c for c in config.get("authentication") if c["type"] == "ecmwfapi"][0]
        self.authentication = authentication.create_authentication(self.authentication_config)
        assert isinstance(self.authentication, ecmwfapi_authentication.ECMWFAuthentication)
        self.realm = self.authentication_config.get("realm")

        self.email = os.environ["POLYTOPE_USER_EMAIL"]
        self.key = os.environ["POLYTOPE_USER_KEY"]
        self.header = self.make_ecmwf_header(self.email, self.key)

    def make_ecmwf_header(self, email, key):
        return "{}:{}".format(email, key)

    def test_authentication_validate(self):
        cache.wipe()  # wipe the cache, because this result can be cached
        user = self.authentication.validate(self.header)
        assert user.realm == self.realm

        # Just to check the caching
        user = self.authentication.validate(self.header)
        assert user.realm == self.realm

    def test_authentication_invalid_credentials(self):
        with pytest.raises(ForbiddenRequest):
            self.authentication.validate("not_valid:user_pass")

    def test_authentication_invalid_auth_type(self):
        with pytest.raises(ForbiddenRequest):
            self.authentication.validate("not_valid_token")


@pytest.mark.authentication_any_type_plain
class TestPlainAuthentication(TestMongoAuthentication):
    def setup_method(self, method):
        config = copy.deepcopy(pytest.polytope_config_auth)

        cache.init(config.get("caching", {}))

        self.authentication_config = [c for c in config.get("authentication") if c["type"] == "plain"][0]
        self.authentication = authentication.create_authentication(self.authentication_config)
        assert isinstance(self.authentication, plain_authentication.PlainAuthentication)
        self.realm = self.authentication_config.get("realm")

        self.premade_user = self.authentication_config.get("users")[0]
        self.username = self.premade_user["uid"]
        self.password = self.premade_user["password"]

        self.header = self.make_basic_header(self.username, self.password)


class TestFederationAuthentication:
    def setup_method(self, method):
        self.authentication_config = {"type": "federation", "secret": "foobar"}
        self.realm = "polytope"

        self.authentication = authentication.create_authentication(self.realm, self.authentication_config)
        assert isinstance(self.authentication, federation_authentication.FederationAuthentication)

    def test_authentication_validate(self):
        credentials = ":".join([self.authentication_config["secret"], "joe", "test-realm"])
        user = self.authentication.authenticate(credentials)
        assert user.realm == "test-realm"
        assert user.username == "joe"

    def test_authentication_invalid_credentials(self):
        credentials = self.authentication_config["secret"] + "baz"
        with pytest.raises(ForbiddenRequest):
            self.authentication.authenticate(credentials)
