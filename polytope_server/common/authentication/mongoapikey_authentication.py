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

from datetime import datetime

import pymongo

from ..auth import User
from ..exceptions import ForbiddenRequest
from ..metric_collector import MongoStorageMetricCollector
from . import authentication


class ApiKeyMongoAuthentication(authentication.Authentication):
    """
    Authenticates a user using a polytope API key. A polytope API key is an alias to a user that was previously
    authenticated. It allows user to authenticate once, retrieve a key, and use that for future authentication.

    Note that the realm reported to the user is 'polytope', since it is polytope that issued the API keys
    Internally, the key is mapped to the original user, and their original realm is the one to which they will be
    authenticated.
    """

    def __init__(self, name, realm, config):

        self.config = config
        host = config.get("host", "localhost")
        port = config.get("port", "27017")
        collection = config.get("collection", "keys")

        endpoint = "{}:{}".format(host, port)
        self.mongo_client = pymongo.MongoClient(endpoint, journal=True, connect=False)
        self.database = self.mongo_client.keys
        self.keys = self.database[collection]
        assert realm == "polytope"

        self.storage_metric_collector = MongoStorageMetricCollector(endpoint, self.mongo_client, "keys", collection)

        super().__init__(name, realm, config)

    def authentication_type(self):
        return "Bearer"

    def authentication_info(self):
        return "Authenticate with Polytope API Key from ../auth/keys"

    def authenticate(self, credentials: str) -> User:

        # credentials should be of the form '<ApiKey>'
        res = self.keys.find_one({"key.key": credentials})
        if res is None:
            raise ForbiddenRequest("Invalid credentials")

        if "key" not in res:
            raise ForbiddenRequest("Key corrupted, please generate a new key")

        key = res["key"]

        if "expiry" not in key:
            raise ForbiddenRequest("Key has no expiry, please generate a new key")

        now = datetime.utcnow().replace(second=0, microsecond=0)
        expires = datetime.fromisoformat(key["expiry"].rstrip("Z"))

        if now > expires:
            raise ForbiddenRequest("Key has expired")

        return User(res["username"], res["realm"])

    def collect_metric_info(self):
        return self.storage_metric_collector.collect().serialize()
