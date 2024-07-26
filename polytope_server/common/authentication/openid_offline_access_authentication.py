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
from . import authentication
from ..exceptions import ForbiddenRequest


class OpenIDOfflineAuthentication(authentication.Authentication):
    def __init__(self, name, realm, config):
        self.config = config

        self.certs_url = config["cert_url"]
        self.public_client_id = config["public_client_id"]
        self.private_client_id = config["private_client_id"]
        self.private_client_secret = config["private_client_secret"]
        self.iam_url = config["iam_url"]
        self.iam_realm = config["iam_realm"]


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
        keycloak_token_introspection = self.iam_url + "/realms/" + self.iam_realm + "/protocol/openid-connect/token/introspect"
        introspection_data = {
            "token": token
        }
        b_auth = requests.auth.HTTPBasicAuth(self.private_client_id, self.private_client_secret)
        resp = requests.post(url=keycloak_token_introspection, data=introspection_data, auth=b_auth).json()
        if resp["active"] and resp["token_type"] == "Offline":
            return True
        else:
            return False

    @cache(lifetime=120)
    def authenticate(self, credentials: str) -> User:

        try:

            # Check if this is a valid offline_access token
            if not self.check_offline_access_token(credentials):
                raise ForbiddenRequest("Not a valid offline_access token")
            
            # Generate an access token from the offline_access token (like a refresh token)
            refresh_data = {
                "client_id": self.public_client_id,
                "grant_type": "refresh_token",
                "refresh_token": credentials
            }
            keycloak_token_endpoint = self.iam_url + "/realms/" + self.iam_realm + "/protocol/openid-connect/token"
            resp = requests.post(url=keycloak_token_endpoint, data=refresh_data)
            token = resp.json()['access_token']
            
            certs = self.get_certs()
            decoded_token = jwt.decode(token=token,
                algorithms=jwt.get_unverified_header(token).get('alg'),
                key=certs
            )

            logging.info("Decoded JWT: {}".format(decoded_token))

            user = User(decoded_token["sub"], self.realm())

            roles = decoded_token.get("resource_access", {}).get(self.public_client_id, {}).get("roles", [])
            user.roles.extend(roles)
            roles = decoded_token.get("realm_access", {}).get("roles", [])
            user.roles.extend(roles)

            logging.info("Found user {} from openid offline_access token".format(user))

        except Exception as e:
            logging.info("Failed to authenticate user from openid offline_access token")
            logging.info(e)
            raise ForbiddenRequest("Could not authenticate user from openid offline_access token")
        return user


    def collect_metric_info(self):
        return {}
