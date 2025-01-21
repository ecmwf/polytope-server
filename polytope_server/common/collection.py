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

from .datasource import create_datasource
from .exceptions import InvalidConfig


class Collection:
    def __init__(self, name, config):
        self.config = config
        self.name = name
        self.roles = config.get("roles")
        self.limits = config.get("limits", {})

        if len(self.config.get("datasources", [])) == 0:
            raise InvalidConfig("No datasources configured for collection {}".format(self.name))

    def datasources(self):
        for ds in self.config.get("datasources", []):
            yield create_datasource(ds)


def create_collections(config):
    collections = {}
    for k, v in config.items():
        collections[k] = Collection(k, v)
    return collections
