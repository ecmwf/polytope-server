#
# Copyright 2025 European Centre for Medium-Range Weather Forecasts (ECMWF)
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

import requests
from jose import jwt

from .exceptions import UnauthorizedRequest
from .user import User


class Authotron:
    """A helper to encapsulate checking user authentication and authorization"""

    def __init__(self, config: dict):
        self.config = config.get("authentication", {}).get("auth-o-tron", {})
        self.url = self.config.get("url", "auth-o-tron-svc:8080")
        self.secret = self.config.get("secret")
        if not self.secret:
            raise ValueError("Missing secret key")

    def authenticate(self, auth_header: str) -> User:
        """Forwards the header to Auth-o-tron.
        Returns authenticated User, or raises UnauthorizedRequest"""

        logging.info("Authenticating user with header: {}".format(auth_header))

        if auth_header.startswith("EmailKey "):
            logging.debug("Converting EmailKey to Bearer token")
            auth_header = f"Bearer {auth_header.split(':')[1]}"

        logging.debug("Forwarding authentication header {}".format(auth_header))

        response = requests.get(f"{self.url}/authenticate", headers={"Authorization": auth_header})
        if response.status_code != 200:
            logging.error("Authentication failed with response: {}".format(response))
            raise UnauthorizedRequest(
                "Authentication failed", www_authenticate=response.headers.get("WWW-Authenticate", "")
            )

        logging.debug("Authentication request successful")
        # decode the jwt token in the authorization header of the response
        jwt_token = response.headers["Authorization"].split(" ")[1]
        decoded_token = jwt.decode(jwt_token, self.secret)
        logging.debug("Decoded JWT token: {}".format(decoded_token))
        user = User(decoded_token["username"], decoded_token["realm"])
        user.roles = list(set(decoded_token.get("roles", [])) | {"default"})
        user.attributes = decoded_token.get("attributes", {})

        logging.debug("User {} authenticated".format(user.username), extra=user.serialize())

        return user

    def has_admin_access(self, user: User) -> bool:
        """Authenticate and authorize user, testing if they have admin rights"""
        roles = self.admin_roles.get(user.realm, [])
        return user.is_authorized(roles)
