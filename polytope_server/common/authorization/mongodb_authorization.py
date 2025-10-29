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

from .. import mongo_client_factory
from ..auth import User
from . import authorization


class MongoDBAuthorization(authorization.Authorization):
    def __init__(self, name, realm, config):
        self.config = config
        assert self.config["type"] == "mongodb"
        self.uri = config.get("uri", "mongodb://localhost:27017")
        self.collection = config.get("collection", "users")
        username = config.get("username")
        password = config.get("password")

        self.mongo_client = mongo_client_factory.create_client(self.uri, username, password)
        self.database = self.mongo_client.authentication
        self.users = self.database[self.collection]

        super().__init__(name, realm, config)

    def get_roles(self, user: User) -> list:
        if user.realm != self.realm():
            raise ValueError(
                "Trying to authorize a user in the wrong realm, expected {}, got {}".format(self.realm(), user.realm)
            )

        res = self.users.find_one({"username": user.username})
        if res is None:
            return []
        return res["roles"]

    def get_attributes(self, user: User) -> dict:
        return {}
