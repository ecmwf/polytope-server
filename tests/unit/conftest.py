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

import tempfile

import pytest
import yaml

import polytope_server.common.config as polytope_config
import polytope_server.common.logging as logging

# TODO: flush argparse to make sure config files provided via -f do not
# conflict with basic_config

tmp = tempfile.NamedTemporaryFile()
with open(tmp.name, "w") as tf:
    test_conf = {
        "developer": {"disable_schema_check": True},
        "version": "1",
        "logging": {},
        "authentication": {"mongodb": {}},
        "request_store": {},
        "testrunner": {},
        "queue": {},
        "datasources": {},
    }
    tf.write(yaml.dump(test_conf))
pytest.basic_config = [tmp.name]

c = polytope_config.ConfigParser()
config = c.read(pytest.basic_config)
logging.setup(config, source_name="polytope_server.tests.unit")
