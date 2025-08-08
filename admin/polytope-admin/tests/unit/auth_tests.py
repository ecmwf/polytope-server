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

import os

import pytest
from polytope_admin.api import Client
from polytope_admin.api.helpers import PolytopeError

from .conftest import ValueStorage


class TestAuth:

    # def __init__(self):
    #    self.address = None
    #    self.port = None
    #    self.admin_username = None
    #    self.admin_password = None

    def setup_method(self, method):

        # Cache environment variables and remove them so they are not used to authenticate
        os.environ["POLYTOPE_USER_KEY"] = ""
        os.environ["POLYTOPE_USER_EMAIL"] = ""

        self.c = Client()
        self.file_config = self.c.config.file_config
        self.c.unset_config("all", persist=True)
        self.c.set_config("address", ValueStorage.address)
        self.c.set_config("port", ValueStorage.port)
        self.c.list_config()
        if ValueStorage.address and ValueStorage.port:
            self.c.ping()

    def teardown_method(self, method):

        # Log in and delete old test data
        self.c.set_config("username", ValueStorage.admin_username)
        self.c.set_config("password", ValueStorage.admin_password)
        self.c.unset_config("user_key", persist=True)
        self.c.list_config()
        try:
            self.c.delete_user("testing")
            self.c.delete_user("testing2")
        except Exception as e:
            print(e)
            pass

        # unset credentials
        # self.c.unset_credentials(username = self.ecmwf_username)
        # self.c.unset_credentials(....)

        # reset config
        self.c.unset_config("all", persist=True)
        for k, v in self.file_config.items():
            self.c.set_config(k, v, persist=True)

    def test_init(self, address, port, admin_username, admin_password, user_key, user_email):
        ValueStorage.address = address
        ValueStorage.port = port

        ValueStorage.admin_username = admin_username
        ValueStorage.admin_password = admin_password

        ValueStorage.user_key = user_key
        ValueStorage.user_email = user_email

    def test_auth_create_user_no_credentials_present(self):
        # when no credentials are present in an authenticated command, it will fail
        with pytest.raises(PolytopeError):
            self.c.create_user(username="testing", password="test_pwd", affiliation="test", role="test-role")
        # pass

    def test_auth_create_user_with_admin_credentials(self):
        self.c.set_config("username", ValueStorage.admin_username)
        self.c.set_config("password", ValueStorage.admin_password)
        self.c.create_user("testing", "test_pwd", "test", "test-role")

    def test_auth_create_user_then_login_to_it(self):
        self.c.set_config("username", ValueStorage.admin_username)
        self.c.set_config("password", ValueStorage.admin_password)
        self.c.create_user("testing", "test_pwd", "test", "test-role")

        # Tries to log in and get an ECMWF key, fails
        with pytest.raises(PolytopeError):
            self.c.login(username="testing", password="test_pwd")

        # Logs in to polytope to get an API key instead
        self.c.login(username="testing", password="test_pwd", key_type="bearer")

    def test_auth_non_admin_cannot_create_user(self):
        self.c.set_config("username", ValueStorage.admin_username)
        self.c.set_config("password", ValueStorage.admin_password)
        self.c.create_user("testing", "test_pwd", "test", "test-role")
        self.c.login(username="testing", password="test_pwd", key_type="bearer")

        # Fails because no admin rights
        with pytest.raises(PolytopeError):
            self.c.create_user("testing2", "test_pwd2", "test", "test-role")

    def test_auth_admin_can_create_another_admin(self):
        # Login as admin 1
        self.c.set_config("username", ValueStorage.admin_username)
        self.c.set_config("password", ValueStorage.admin_password)
        # Create admin 2
        self.c.create_user("testing", "test_pwd", "test", "polytope-admin")
        # Login as admin 2
        self.c.login(username="testing", password="test_pwd", key_type="bearer")
        # Remove admin 1 credentials
        self.c.unset_config("username")
        self.c.unset_config("password")
        # Create admin 3
        self.c.create_user("testing2", "test_pwd2", "test", "polytope-admin")

    def test_create_polytope_user_from_ecmwf_admin(self):
        # os.environ['POLYTOPE_USER_KEY'] = self.user_key
        # os.environ['POLYTOPE_USER_EMAIL'] = self.user_email

        self.c.set_config("user_email", ValueStorage.user_email)
        self.c.set_config("user_key", ValueStorage.user_key)
        self.c.create_user("testing", "test_pwd", "test", "some-role")
