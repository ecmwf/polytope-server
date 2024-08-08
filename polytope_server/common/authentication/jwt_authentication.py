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

import requests
from jose import jwt

from ..auth import User
from ..caching import cache
from ..exceptions import ForbiddenRequest
from . import authentication


class JWTAuthentication(authentication.Authentication):
    def __init__(self, name, realm, config):
        self.config = config

        self.certs_url = config["cert_url"]
        self.client_id = config["client_id"]

        super().__init__(name, realm, config)

    def authentication_type(self):
        return "Bearer"

    def authentication_info(self):
        return "Authenticate with JWT token"

    @cache(lifetime=120)
    def get_certs(self):
        return requests.get(self.certs_url).json()

    @cache(lifetime=120)
    def authenticate(self, credentials: str) -> User:

        try:
            certs = self.get_certs()
            decoded_token = jwt.decode(
                token=credentials, algorithms=jwt.get_unverified_header(credentials).get("alg"), key=certs
            )

            logging.info("Decoded JWT: {}".format(decoded_token))

            user = User(decoded_token["sub"], self.realm())

            roles = decoded_token.get("resource_access", {}).get(self.client_id, {}).get("roles", [])
            user.roles.extend(roles)

            logging.info("Found user {} from decoded JWT".format(user))
        except Exception as e:
            logging.info("Failed to authenticate user from JWT")
            logging.info(e)
            raise ForbiddenRequest("Credentials could not be unpacked")
        return user

    def collect_metric_info(self):
        return {}
