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

import logging
import os
import subprocess

import pytest

import polytope_server.common.config as polytope_config
from polytope_server.common.datasource import create_datasource
from polytope_server.common.request import PolytopeRequest


@pytest.mark.skipif(subprocess.call(["which", "mars"]) != 0, reason="MARS not in path")
class TestDataSourceMars:
    def setup_method(self, method):

        polytope_config.global_config["datasources"]["mars"] = {
            "type": "mars",
            "command": "mars",
            "match": {},
            "override_email": os.getenv("POLYTOPE_USER_EMAIL", None),
            "override_apikey": os.getenv("POLYTOPE_USER_KEY", None),
        }

        self.mars_config = {"name": "mars"}

        self.request = PolytopeRequest()
        self.request.user_request = ""  # all default
        self.ds = create_datasource(self.mars_config)

    def test_datasource_mars(self):
        success = self.ds.dispatch(self.request, None)
        assert success

        # Check the FIFO exists and has data
        assert not os.path.isfile(self.ds.fifo.path)
        assert self.ds.fifo.ready()

        for r in self.ds.result(self.request):
            logging.info(len(r))
            assert len(r) > 0

        # Check the FIFO was deleted
        assert not os.path.isfile(self.ds.fifo.path)

    def test_datasource_mars_raises(self):

        self.request.user_request = b"213124141"

        with pytest.raises(Exception):
            self.ds.dispatch(self.request, None)

        # Check the FIFO was deleted
        assert not os.path.isfile(self.ds.fifo.path)
