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
import uuid
from datetime import datetime, timedelta

import pymongo

from ..auth import User
from ..exceptions import ForbiddenRequest
from ..metric_collector import MongoStorageMetricCollector
from . import ApiKey, keygenerator


class MongoKeyGenerator(keygenerator.KeyGenerator):
    def __init__(self, config):
        self.config = config
        assert self.config["type"] == "mongodb"
        host = config.get("host", "localhost")
        port = config.get("port", "27017")
        collection = config.get("collection", "keys")
        endpoint = "{}:{}".format(host, port)
        self.mongo_client = pymongo.MongoClient(endpoint, journal=True, connect=False)
        self.database = self.mongo_client.keys
        self.keys = self.database[collection]
        self.realms = config.get("allowed_realms")

        self.storage_metric_collector = MongoStorageMetricCollector(endpoint, self.mongo_client, "keys", collection)

    def create_key(self, user: User) -> ApiKey:

        if user.realm not in self.realms:
            raise ForbiddenRequest("Not allowed to create an API Key for users in realm {}".format(user.realm))

        res = self.keys.delete_many({"user.id": user.id})
        if res:
            logging.debug("Removed {} previously issued keys for user {}".format(res.deleted_count, user.username))

        now = datetime.utcnow().replace(second=0, microsecond=0)
        expires = now + timedelta(days=365)
        expires_RFC3339 = expires.isoformat("T") + "Z"

        key = keygenerator.ApiKey()
        key.key = str(uuid.uuid4())
        key.timestamp = now
        key.expiry = expires_RFC3339

        self.keys.insert_one({**user.serialize(), "key": key.serialize()})

        return key

    def collect_metric_info(self):
        return self.storage_metric_collector.collect().serialize()
