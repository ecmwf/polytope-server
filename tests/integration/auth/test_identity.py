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
    Tests the Identity abstraction for adding and removing users.

    Identity objects usually create users which are consumed by authentication and authorization handler.
    Where possible, we also check that the users can be authenticated and authorized after the identity
    service has craeted them.

"""

import base64
import copy

import pytest

from polytope_server.common.auth import AuthHelper
from polytope_server.common.exceptions import (
    Conflict,
    EndpointNotImplemented,
    ForbiddenRequest,
    UnauthorizedRequest,
)
from polytope_server.common.identity import identity, mongodb_identity, none_identity


@pytest.mark.identity_mongodb
class TestMongoDBIdentity:
    def setup_method(self, method):
        config = copy.deepcopy(pytest.polytope_config_auth)

        # Read identity config
        self.identity = identity.create_identity(config.get("identity"))

        # Create an authenticator and authorizer which will detect users created using this identity
        self.auth_config = {
            "authentication": {
                self.identity.realm: {
                    "authenticators": {
                        "mongodb": {
                            "type": "mongodb",
                            "host": self.identity.host,
                            "port": self.identity.port,
                            "collection": self.identity.collection,
                        }
                    },
                    "authorizers": {
                        "mongodb": {
                            "type": "mongodb",
                            "host": self.identity.host,
                            "port": self.identity.port,
                            "collection": self.identity.collection,
                        }
                    },
                }
            }
        }

        self.auth = AuthHelper(self.auth_config)

        assert isinstance(self.identity, mongodb_identity.MongoDBIdentity)
        assert self.identity.users.name == "test-users"

        self.username = "Bill"
        self.password = "Flowerpot"
        self.roles = ["some_role"]
        self.auth_header = self.make_header(self.username, self.password)

        # Check if the extra-user is there
        assert self.auth.authenticate(self.make_header("test-user2", "t35t*!II"))

        self.identity.wipe()

    def make_header(self, username, password):
        return "Basic " + base64.b64encode("{}:{}".format(username, password).encode()).decode()

    def teardown_method(self, method):
        pass

    def test_identity_add_user(self):
        assert self.identity.add_user(self.username, self.password, self.roles)
        with pytest.raises(Conflict):
            assert self.identity.add_user(self.username, self.password, self.roles)
        self.identity.wipe()

    def test_identity_remove_user(self):
        assert self.identity.add_user(self.username, self.password, self.roles)
        # assert self.identity.remove_user( self.username )
        # with pytest.raises(NotFound):
        #    assert not self.identity.remove_user( self.username )
        # with pytest.raises(NotFound):
        #    assert not self.identity.remove_user( self.username + 'x')
        self.identity.wipe()

    def test_identity_wipe(self):
        assert self.identity.add_user(self.username, self.password, self.roles)
        self.identity.wipe()

        with pytest.raises(UnauthorizedRequest):
            self.auth.authenticate(self.auth_header)

    def test_identity_authenticate(self):
        assert self.identity.add_user(self.username, self.password, self.roles)

        user = self.auth.authenticate(self.auth_header)
        assert user
        assert user.username == self.username
        assert user.realm == self.identity.realm

    def test_identity_authorize(self):
        assert self.identity.add_user(self.username, self.password, self.roles)
        assert self.auth.has_roles(self.auth_header, ["some_role", "not_this_role"]) is not None
        with pytest.raises(ForbiddenRequest):
            self.auth.has_roles(self.auth_header, ["not_this_role"])


@pytest.mark.basic
class TestNoneIdentity(TestMongoDBIdentity):
    def setup_method(self, method):
        self.identity = identity.create_identity()  # defaults to None
        assert isinstance(self.identity, none_identity.NoneIdentity)
        self.username = "Bill"
        self.password = "Flowerpot"
        self.roles = ["some_role"]

    def test_identity_add_user(self):
        with pytest.raises(EndpointNotImplemented):
            assert self.identity.add_user(self.username, self.password, self.roles)

    def test_identity_remove_user(self):
        with pytest.raises(EndpointNotImplemented):
            assert not self.identity.remove_user(self.username)

    def test_identity_wipe(self):
        with pytest.raises(EndpointNotImplemented):
            self.identity.wipe()

    def test_identity_authenticate(self):
        pass

    def test_identity_authorize(self):
        pass
