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
import logging
import os

from keycloak import KeycloakOpenID
from keycloak.exceptions import KeycloakConnectionError

from ..auth import User
from ..caching import cache
from ..exceptions import ForbiddenRequest
from . import authentication


class KeycloakAuthentication(authentication.Authentication):
    def __init__(self, name, realm, config):
        self.config = config

        # URL of the keycloak API: e.g. https://keycloak.insitute.org/auth/"
        self.url = config["url"]

        # Keycloak client id and secret
        self.client_id = config["client_id"]  # e.g. polytope
        self.client_secret = config["client_secret"]

        # The keycloak realm to look for users
        self.keycloak_realm = config["keycloak_realm"]

        self.skipTLS = config.get("skip_tls", False)

        # Connection parameters
        self.timeout = config.get("timeout", 3)

        # Mapping user attributes to keycloak attributes
        self.attribute_map = config.get("attributes", {})

        super().__init__(name, realm, config)

    def authentication_type(self):
        return "Basic"

    def authentication_info(self):
        return "Authenticate with Keycloak username and password"

    @cache(lifetime=120)
    def authenticate(self, credentials: str) -> User:
        # credentials should be of the form 'base64(<username>:<API_key>)'
        try:
            decoded = base64.b64decode(credentials).decode("utf-8")
            auth_user, auth_password = decoded.split(":", 1)
        except UnicodeDecodeError:
            raise ForbiddenRequest("Credentials could not be decoded")
        except ValueError:
            raise ForbiddenRequest("Credentials could not be unpacked")

        _environ = dict(os.environ)
        try:
            os.environ["http_proxy"] = os.getenv("POLYTOPE_PROXY", "")
            os.environ["https_proxy"] = os.getenv("POLYTOPE_PROXY", "")

            logging.debug("Setting HTTPS_PROXY to {}".format(os.environ["https_proxy"]))

            try:
                # Open a session as a registered client
                client = KeycloakOpenID(
                    server_url=self.url,
                    client_id=self.client_id,
                    realm_name=self.keycloak_realm,
                    client_secret_key=self.client_secret,
                    verify=(self.skipTLS is False),
                )

                client.connection.timeout = self.timeout

                # Obtain a session token on behalf of the user
                token = client.token(auth_user, auth_password)

            except KeycloakConnectionError:
                # Raise ForbiddenRequest rather than ServerError so that we are not blocked if Keycloak is down
                raise ForbiddenRequest("Could not connect to Keycloak")
            except Exception:
                raise ForbiddenRequest("Invalid Keycloak credentials")

            userinfo = client.userinfo(token["access_token"])

            user = User(auth_user, self.realm())

            logging.debug("Found user {} in keycloak".format(auth_user))

            for k, v in self.attribute_map.items():
                if v in userinfo:
                    user.attributes[k] = userinfo[v]
                    logging.debug("User {} has attribute {} : {}".format(user.username, k, user.attributes[k]))

            return user

        finally:
            os.environ.clear()
            os.environ.update(_environ)

    def collect_metric_info(self):
        return {}
