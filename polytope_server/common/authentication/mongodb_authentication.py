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

import base64
import binascii
import hashlib

from .. import mongo_client_factory
from ..auth import User
from ..exceptions import ForbiddenRequest
from ..metric_collector import MongoStorageMetricCollector
from . import authentication


class MongoAuthentication(authentication.Authentication):
    def __init__(self, name, realm, config):
        self.config = config
        host = config.get("host", "localhost")
        port = config.get("port", "27017")
        collection = config.get("collection", "users")
        username = config.get("username")
        password = config.get("password")
        srv = bool(config.get("srv", False))
        tls = bool(config.get("tls", False))
        tlsCAFile = config.get("tlsCAFile", None)

        endpoint = "{}:{}".format(host, port)
        self.mongo_client = mongo_client_factory.create_client(host, port, username, password, srv, tls, tlsCAFile)
        self.database = self.mongo_client.authentication
        self.users = self.database[collection]

        self.storage_metric_collector = MongoStorageMetricCollector(
            endpoint, self.mongo_client, "authentication", collection
        )

        super().__init__(name, realm, config)

    def cache_id(self):
        return self.config

    def authentication_type(self):
        return "Basic"

    def authentication_info(self):
        return "Authenticate with username and password"

    def authenticate(self, credentials: str) -> User:
        # credentials should be of the form 'base64(<username>:<API_key>)'
        try:
            decoded = base64.b64decode(credentials).decode("utf-8")
            auth_user, auth_password = decoded.split(":", 1)
        except UnicodeDecodeError:
            raise ForbiddenRequest("Credentials could not be decoded")
        except ValueError:
            raise ForbiddenRequest("Credentials could not be unpacked")

        res = self.users.find_one({"username": auth_user})
        if res is None:
            raise ForbiddenRequest("Invalid credentials")

        if not self.verify_password(auth_user, auth_password, res["password"]):
            raise ForbiddenRequest("Invalid credentials")

        if not res["realm"] == self.realm():
            raise ForbiddenRequest("Invalid credentials")

        return User(auth_user, self.realm())

    @staticmethod
    def hash_password(password, username):
        salt = username.encode("ascii")
        hashed_binary = hashlib.pbkdf2_hmac("sha512", password.encode("utf-8"), salt, 2048)
        hashed_hex = binascii.hexlify(hashed_binary)
        return (salt + hashed_hex).decode("ascii")

    @staticmethod
    def verify_password(username, password, stored_password):
        password_hash = MongoAuthentication.hash_password(username, password)
        return password_hash == stored_password

    def collect_metric_info(self):
        return self.storage_metric_collector.collect().serialize()
