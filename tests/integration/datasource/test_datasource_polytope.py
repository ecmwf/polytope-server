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

import polytope_server.common.config as polytope_config
from polytope_server.common.auth import AuthHelper
from polytope_server.common.datasource import create_datasource
from polytope_server.common.request import Request, Verb


class TestDataSourcePolytope:
    def setup_method(self, method):

        # Make a request to a local polytope datasource
        # ... which makes a request to a remote polytope (actually the currently deployed polytope, for testing)
        # The request is made with a service account, telling the remote polytope that it has already
        # authenticated the user.

        target = "http://" + polytope_config.global_config["frontend"]["host"]
        port = polytope_config.global_config["frontend"]["port"]

        polytope_config.global_config["datasources"]["polytope-test"] = {
            "type": "polytope",
            "url": target,
            "port": port,
            "secret": polytope_config.global_config["federation"]["test_federation"]["secret"],
            "api_version": "v1",
        }

        self.datasource_config_echo = {"name": "polytope-test", "collection": "debug"}
        self.datasource_config_raises = {"name": "polytope-test", "collection": "debug-raises"}

        auth = AuthHelper(polytope_config.global_config)

        self.user = auth.authenticate(
            "EmailKey {}:{}".format(os.environ["POLYTOPE_USER_EMAIL"], os.environ["POLYTOPE_USER_KEY"])
        )

        self.ds_echo = create_datasource(self.datasource_config_echo)
        self.ds_raises = create_datasource(self.datasource_config_raises)

    def test_datasource_polytope_retrieve(self):

        dummy_data = "this message will be echo'd"

        self.request = Request()
        self.request.user_request = dummy_data
        self.request.user = self.user
        self.request.verb = Verb.RETRIEVE

        success = self.ds_echo.dispatch(self.request, None)
        assert success

        data = b""
        for i in self.ds_echo.result(self.request):
            data += i

        assert data.decode() == dummy_data

    def test_datasource_polytope_archive(self):

        dummy_data = "this message will be echo'd"
        self.request = Request()
        self.request.user_request = dummy_data
        self.request.user = self.user
        self.request.verb = Verb.ARCHIVE
        success = self.ds_echo.dispatch(self.request, b"test_data")
        assert success

    def test_datasource_polytope_retrieve_error(self):

        dummy_data = "this message will be echo'd"

        self.request = Request()
        self.request.user_request = dummy_data
        self.request.user = self.user
        self.request.verb = Verb.RETRIEVE

        with pytest.raises(Exception):
            self.ds_raises.dispatch(self.request, None)

    def test_datasource_polytope_archive_error(self):

        dummy_data = "this message will be echo'd"
        self.request = Request()
        self.request.user_request = dummy_data
        self.request.user = self.user
        self.request.verb = Verb.ARCHIVE

        with pytest.raises(Exception):
            self.ds_raises.dispatch(self.request, b"test_data")
