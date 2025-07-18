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

from polytope_server.common import collection
from polytope_server.common.auth import AuthHelper
from polytope_server.common.caching import cache
from polytope_server.common.exceptions import ForbiddenRequest, UnauthorizedRequest
from polytope_server.common.identity import identity


@pytest.mark.authentication_any_type_mongodb
class TestMongoAuthentication:
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

        cache.init(config.get("caching", {}))

        self.auth = AuthHelper(config)

        self.config = config

        # Create a MongoDB user
        self.identity = identity.create_identity(config.get("identity"))
        assert self.identity.realm == "ecmwf"
        self.realm = self.identity.realm
        self.mongo_username = "Bill"
        self.mongo_password = "Flowerpot"
        self.mongo_roles = ["some_role"]
        self.mongo_basic_header = self.make_basic_header(self.mongo_username, self.mongo_password)
        self.identity.wipe()
        self.identity.add_user(self.mongo_username, self.mongo_password, self.mongo_roles)

        # Config for an ECMWF API Key user
        self.ec_email = os.environ["POLYTOPE_USER_EMAIL"]
        self.ec_key = os.environ["POLYTOPE_USER_KEY"]
        self.ecmwf_header = self.make_ecmwf_header(self.ec_email, self.ec_key)

        # The premade user from Plain authentication
        self.plain_username = "test-user1"
        self.plain_password = "t35t*!I"
        self.plain_basic_header = self.make_basic_header(self.plain_username, self.plain_password)

    def make_basic_header(self, username, password):
        return "Basic " + base64.b64encode("{}:{}".format(username, password).encode()).decode()

    def make_ecmwf_header(self, email, key):
        return "EmailKey {}:{}".format(email, key)

    def teardown_method(self, method):
        cache.wipe()
        self.identity.wipe()

    # Authentication

    def test_authenticate_with_mongo(self):
        user = self.auth.authenticate(self.mongo_basic_header)
        assert user.username == self.mongo_username
        assert user.realm == self.realm

    def test_authenticate_with_ecmwfapi(self):
        user = self.auth.authenticate(self.ecmwf_header)
        assert user.username != ""
        assert user.realm == self.realm

    def test_authenticate_with_plain(self):
        user = self.auth.authenticate(self.plain_basic_header)
        assert user.username == self.plain_username
        assert user.realm == self.realm

    def test_authenticate_matches_multiple(self):
        # These credentials matche two authenticators in different realms, it should return the first in order of config
        user = self.auth.authenticate(self.plain_basic_header)
        assert user.username == self.plain_username
        assert user.realm == self.realm

    def test_authenticate_another_realm(self):
        header = self.make_basic_header("test-user2", "t35t*!II")
        user = self.auth.authenticate(header)
        assert user.username == "test-user2"
        assert user.realm == "testrealm"

    def test_authenticate_unknown_auth_type(self):
        header = "Bongo {}:{}".format(self.ec_email, self.ec_key)
        try:
            self.auth.authenticate(header)
        except UnauthorizedRequest as e:
            assert e.description.startswith("No authentication providers for authentication type")

    def test_authenticate_known_auth_type_but_wrong_credentials(self):
        header = self.make_basic_header("wrong", "credentials")
        try:
            self.auth.authenticate(header)
        except UnauthorizedRequest as e:
            assert e.description == "Invalid credentials"

    def test_authenticate_malformedheader(self):
        header = "Basic1"
        try:
            self.auth.authenticate(header)
        except UnauthorizedRequest as e:
            assert e.description.startswith("Could not read authorization header")

    def test_authenticate_missingheader(self):
        header = ""
        try:
            self.auth.authenticate(header)
        except UnauthorizedRequest as e:
            assert e.description.startswith("Could not read authorization header")

    def test_authenticate_multipleheaders(self):
        header = self.plain_basic_header + "," + self.mongo_basic_header
        user = self.auth.authenticate(header)
        # Should have got the plain user
        assert user.username == self.plain_username

    # Authorization

    def test_authorize_with_mongo_and_plain(self):
        # The mongodb user should have the roles it was created with
        # plus the extra role 'polytope-admin' defined by the plain authorization config
        user = self.auth.authenticate(self.mongo_basic_header)
        assert self.auth.is_authorized(user, [self.mongo_roles[0]])
        assert self.auth.is_authorized(user, self.mongo_roles[0])  # should accept non-list
        assert self.auth.is_authorized(user, "polytope-admin")

        # This should fail because the user does not belong to the realm 'testrealm'
        # that authorizer will not be checked
        with pytest.raises(ForbiddenRequest):
            self.auth.is_authorized(user, "testrealm-admin")

    def test_authhelper_has_admin_access(self):
        # The mongodb user has role 'polytope-admin', which matches the 'admin' section of the config
        assert self.auth.has_admin_access(self.mongo_basic_header)

        # This user belongs to testrealm, so should not match the 'admin' requirements
        header = self.make_basic_header("test-user2", "t35t*!II")
        with pytest.raises(ForbiddenRequest):
            assert self.auth.has_admin_access(header)

    def test_authhelper_has_collection_access(self):
        collections = collection.create_collections(self.config.get("collections"))

        # The mongodb user has role 'polytope-admin', which matches the collection requirements
        assert self.auth.can_access_collection(self.mongo_basic_header, collections.get("debug"))

        # This user belongs to testrealm, so should not match the colletion requirements
        header = self.make_basic_header("test-user2", "t35t*!II")
        with pytest.raises(ForbiddenRequest):
            assert self.auth.can_access_collection(header, collections.get("debug"))

    def test_authhelper_has_roles(self):
        # The mongodb user has role 'polytope-admin'
        assert self.auth.has_roles(self.mongo_basic_header, "polytope-admin")
        assert self.auth.has_roles(self.mongo_basic_header, ["polytope-admin"])
        assert self.auth.has_roles(self.mongo_basic_header, ["polytope-admin", self.mongo_roles[0]])
        assert self.auth.has_roles(self.mongo_basic_header, ["polytope-admin", self.mongo_roles[0], "not_this_one"])
