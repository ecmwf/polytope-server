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
from typing import Any

import requests
from jose import jwt

from ..auth import User
from ..caching import cache
from ..exceptions import ForbiddenRequest
from . import authentication


class OpenIDOfflineAuthentication(authentication.Authentication):
    def __init__(self, name, realm, config):
        self.config = config

        self.certs_url = config["cert_url"]
        self.public_client_id = config["public_client_id"]
        self.private_client_id = config["private_client_id"]
        self.private_client_secret = config["private_client_secret"]
        self.iam_url = config["iam_url"]
        self.iam_realm = config["iam_realm"]
        self.jwt_aud = config.get("jwt_aud", None)
        self.jwt_iss = config.get("jwt_iss", None)
        self.disable_check = config.get("disable_check", False)

        super().__init__(name, realm, config)

    def authentication_type(self):
        return "Bearer"

    def authentication_info(self):
        return "Authenticate with OpenID offline_access token"

    @cache(lifetime=120)
    def get_certs(self):
        return requests.get(self.certs_url).json()

    @cache(lifetime=120)
    def check_offline_access_token(self, token: str) -> bool:
        """
        We check if the token is recognised by the IAM service, and we cache this result.
        We cannot simply try to get the access token because we would spam the IAM server with invalid tokens, and the
        failure at that point would not be cached.
        """
        if self.disable_check is True:
            return True

        keycloak_token_introspection = (
            self.iam_url + "/realms/" + self.iam_realm + "/protocol/openid-connect/token/introspect"
        )
        introspection_data = {"token": token}
        b_auth = requests.auth.HTTPBasicAuth(self.private_client_id, self.private_client_secret)
        resp = requests.post(url=keycloak_token_introspection, data=introspection_data, auth=b_auth).json()
        if resp["active"] and resp["token_type"] == "Offline":
            return True
        else:
            return False

    @cache(lifetime=120)
    def get_token(self, credentials: str) -> dict[str, Any] | None:
        # Generate an access token from the offline_access token (like a refresh token)
        refresh_data = {
            "client_id": self.public_client_id,
            "grant_type": "refresh_token",
            "refresh_token": credentials,
        }
        keycloak_token_endpoint = self.iam_url + "/realms/" + self.iam_realm + "/protocol/openid-connect/token"
        resp = requests.post(url=keycloak_token_endpoint, data=refresh_data)

        if resp.ok:
            token = resp.json()["access_token"]
        elif resp.status_code == 400:
            # see RFC 6749 OAuth 2.0, Oct 12, Sect. 5.2 Error response, page 45
            logging.info("Failed to authenticate user from openid offline_access token")
            logging.info(resp.json()["error"])
            return None
        else:
            resp.raise_for_status()

        certs = self.get_certs()
        decoded_token = jwt.decode(
            token=token,
            algorithms=jwt.get_unverified_header(token).get("alg"),
            key=certs,
            audience=self.jwt_aud,
            issuer=self.jwt_iss,
        )

        logging.info("Decoded JWT: {}".format(decoded_token))

        return decoded_token

    @cache(lifetime=120)
    def authenticate(self, credentials: str) -> User:
        try:
            # Check if this is a valid offline_access token
            if not self.check_offline_access_token(credentials):
                raise ForbiddenRequest("Not a valid offline_access token")

            token = self.get_token(credentials)
            if token is None:
                raise ForbiddenRequest("Not a valid offline_access token")

            user = User(token["sub"], self.realm())

            key = self.jwt_aud if self.jwt_aud is not None else self.public_client_id
            roles = token.get("resource_access", {}).get(key, {}).get("roles", [])
            user.roles.extend(roles)
            roles = token.get("realm_access", {}).get("roles", [])
            user.roles.extend(roles)

            logging.info("Found user {} from openid offline_access token".format(user))

        except Exception as e:
            logging.info("Failed to authenticate user from openid offline_access token")
            logging.info(e)
            raise ForbiddenRequest("Could not authenticate user from openid offline_access token")
        return user
