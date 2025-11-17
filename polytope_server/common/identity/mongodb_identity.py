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
from ..authentication.mongodb_authentication import MongoAuthentication
from ..exceptions import Conflict, NotFound
from ..metric_collector import MetricCollector
from . import identity


class MongoDBIdentity(identity.Identity):
    def __init__(self, config):
        self.config = config
        self.uri = config.get("uri", "mongodb://localhost:27017")

        self.collection = config.get("collection", "users")
        username = config.get("username")
        password = config.get("password")

        self.mongo_client = mongo_client_factory.create_client(
            self.uri,
            username,
            password,
        )
        self.database = self.mongo_client.authentication
        self.users = self.database[self.collection]
        self.realm = config.get("realm")

        for u in config.get("extra-users", []):
            try:
                self.add_user(u["uid"], u["password"], u["roles"])
            except Conflict:
                # Likely that the user already exists
                pass

        self.identity_metric_collector = MetricCollector()

    def add_user(self, username: str, password: str, roles: list) -> bool:
        if self.users.find_one({"username": username}) is not None:
            raise Conflict("Username already registered")

        hashed_passwd = MongoAuthentication.hash_password(username, password)

        user = {
            "username": username,
            "password": hashed_passwd,
            "roles": roles,
            "realm": self.realm,
        }

        self.users.insert_one(user)

        return True

    def remove_user(self, username: str) -> bool:
        result = self.users.delete_one({"username": username})
        if result.deleted_count > 0:
            return True
        else:
            raise NotFound("User {} does not exist".format(username))

    def wipe(self) -> None:
        self.database.drop_collection(self.users.name)

    def collect_metric_info(self):
        metric = self.identity_metric_collector.collect().serialize()
        return metric
