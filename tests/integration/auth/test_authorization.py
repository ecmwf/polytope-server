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

from polytope_server.common.authorization import (
    authorization,
    ldap_authorization,
    mongodb_authorization,
    plain_authorization,
)
from polytope_server.common.identity import identity
from polytope_server.common.user import User


@pytest.mark.authorization_any_type_mongodb
class TestMongoAuthorization:
    def setup_method(self, method):

        config = copy.deepcopy(pytest.polytope_config_auth)

        self.authorization_config = [c for c in config.get("authorization") if c["type"] == "mongodb"][0]
        self.authorization = authorization.create_authorization(self.authorization_config)

        assert isinstance(self.authorization, mongodb_authorization.MongoDBAuthorization)
        assert self.authorization.users.name == "test-users"

        self.realm = self.authorization_config.get("realm")

        # Create a test user using the identity service
        self.identity = identity.create_identity(config.get("identity"))
        assert self.identity.realm == self.realm
        self.username = "Bill"
        self.password = "Flowerpot"
        self.roles = ["some_role"]
        self.identity.wipe()
        self.identity.add_user(self.username, self.password, self.roles)

        # No need to authenticate here, we can just create the User ourselves
        self.user = User(self.username, self.realm)

    def teardown_method(self, method):
        pass

    def test_authorization_roles(self):
        roles = self.authorization.authorized_roles(self.user)
        assert roles == self.roles

    def test_authorization_invalid_user(self):
        user = User("not_a_user", self.realm)
        roles = self.authorization.authorized_roles(user)
        assert len(roles) == 0

    def test_authorization_invalid_realm(self):
        user = User(self.username, "not_a_realm")
        with pytest.raises(ValueError):
            self.authorization.authorized_roles(user)


@pytest.mark.authorization_any_type_ldap
class TestLDAPAuthorization(TestMongoAuthorization):
    def setup_method(self, method):

        config = copy.deepcopy(pytest.polytope_config_auth)

        self.authorization_config = [c for c in config.get("authorization") if c["type"] == "ldap"][0]
        self.authorization = authorization.create_authorization(self.authorization_config)

        assert isinstance(self.authorization, ldap_authorization.LDAPAuthorization)

        self.realm = self.authorization_config.get("realm")

        # We'll test the roles for ECMWF user 'max', which has more roles than most
        self.username = "max"

        # No need to authenticate here, we can just create the User ourselves
        self.user = User(self.username, self.realm)

    def test_authorization_roles(self):
        roles = self.authorization.authorized_roles(self.user)
        assert len(roles) > 0
        assert isinstance(roles[0], str)


@pytest.mark.authorization_any_type_plain
class TestPlainAuthorization(TestMongoAuthorization):
    def setup_method(self, method):

        config = copy.deepcopy(pytest.polytope_config_auth)
        config["authentication"].append(
            {
                "type": "plain",
                "realm": "testrealm",
                "users": [{"uid": "test-user1", "password": "t35t*!I"}, {"uid": "test-user2", "password": "t35t*!II"}],
            }
        )
        for auth in config["authorization"]:
            if auth["type"] == "plain":
                auth["roles"]["polytope-admin"] = ["test-user1", "Bill"]
        config["authorization"].append(
            {"type": "plain", "realm": "testrealm", "roles": {"testrealm-admin": ["test-user1", "Bill"]}}
        )

        self.authorization_config = [c for c in config.get("authorization") if c["type"] == "plain"][0]
        self.authorization = authorization.create_authorization(self.authorization_config)

        assert isinstance(self.authorization, plain_authorization.PlainAuthorization)

        self.realm = self.authorization_config.get("realm")

        self.username = "test-user1"

        # No need to authenticate here, we can just create the User ourselves
        self.user = User(self.username, self.realm)

    def test_authorization_roles(self):
        roles = self.authorization.authorized_roles(self.user)
        assert roles[0] == "polytope-admin"
