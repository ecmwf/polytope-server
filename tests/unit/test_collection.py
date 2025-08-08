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

import polytope_server.common.collection as collection
import polytope_server.common.config as polytope_config


class Test:
    def setup_method(self, method):
        self.config = {
            "datasources": {"a_datasource": {"type": "echo"}},
            "authentication": {"an_authentication": {"type": "none"}},
            "authorization": {"an_authorization": {"type": "none"}},
            "collections": {
                "a_collection": {
                    "authentication": "an_authentication",
                    "authorization": "an_authorization",
                    "roles": ["role1", "role2"],
                    "datasources": [{"name": "a_datasource", "match": "some_config"}],
                }
            },
        }
        polytope_config.global_config = self.config

    def test_collection(self):
        collection.create_collections(self.config["collections"])
