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
from typing import Dict, Union

from .authentication import authentication
from .authentication.federation_authentication import FederationAuthentication
from .authorization import authorization
from .exceptions import ForbiddenRequest, UnauthorizedRequest
from .metric import MetricType
from .request import Status
from .user import User


class AuthHelper:
    """A helper to encapsulate checking user authentication and authorization"""

    def __init__(self, config):
        self.config = config

        self.authenticators = []
        self.authorizers = []

        for realm in config.get("authentication", {}):

            logging.debug("Loading authenticators and authorizers for realm {}".format(realm))

            for name, authentication_config in config["authentication"][realm].get("authenticators", {}).items():
                self.authenticators.append(authentication.create_authentication(name, realm, authentication_config))

            for name, authorization_config in config["authentication"][realm].get("authorizers", {}).items():
                self.authorizers.append(authorization.create_authorization(name, realm, authorization_config))

        # If API Keys are enabled, we also try to authenticate using the API Key authenticator
        apikey_config = self.config.get("api-keys", {}).get("authenticator", None)
        if apikey_config:
            self.authenticators.append(authentication.create_authentication("api-keys", "polytope", apikey_config))

        # If federation is enabled, we try to authenticate with Federation authenticators
        for name, federation_config in config.get("federation", {}).items():
            self.authenticators.append(FederationAuthentication(name, "polytope", federation_config))

        self.admin_config = config.get("admin", {})
        self.admin_roles = self.admin_config.get("roles", {})

        self.auth_info = []
        for a in self.authenticators:
            self.auth_info.append(
                "{} realm={},info='{}'".format(a.authentication_type(), a.realm(), a.authentication_info())
            )

        self.auth_info = ",".join(self.auth_info)

    def authenticate(self, auth_header) -> User:
        """Returns authenticated User, or raises UnauthorizedRequest"""

        logging.info("Authenticating user with header:\n{}".format(auth_header))

        headers = auth_header.split(",")
        if len(headers) == 0:
            raise UnauthorizedRequest(
                "Could not read authorization header, expected 'Authorization: <type> <credentials>",
                details=None,
                www_authenticate=self.auth_info,
            )

        user = None
        details = []
        matched_type = 0

        # Extract authorization header and check authenticators for a match (first match policy)
        for h in headers:
            try:
                auth_type, auth_credentials = h.split(" ", 1)
            except ValueError:
                raise UnauthorizedRequest(
                    "Could not read authorization header, expected 'Authorization: <type> <credentials>",
                    details=None,
                    www_authenticate=self.auth_info,
                )

            for authenticator in self.authenticators:
                if authenticator.authentication_type() == auth_type:
                    matched_type += 1
                    try:
                        user = authenticator.authenticate(auth_credentials)
                    except ForbiddenRequest as e:
                        details.append(e.description)

        if matched_type == 0:
            raise UnauthorizedRequest(
                'No authentication providers for authentication type "{}"'.format(auth_type),
                details=None,
                www_authenticate=self.auth_info,
            )

        if user is None:
            raise UnauthorizedRequest(
                "Invalid credentials",
                details=details,
                www_authenticate=self.auth_info,
            )

        user.roles.append("default")

        # Visit all authorizers to append additional roles and attributes
        for authorizer in self.authorizers:
            if authorizer.realm() == user.realm:
                user.roles = list(set.union(set(user.roles), set(authorizer.get_roles(user))))
                user.attributes.update(authorizer.get_attributes(user))

        logging.info("User authenticated:\n {}".format(user.serialize()))

        return user

    def has_admin_access(self, auth_header):
        """Authenticate and authorize user, testing if they have admin rights"""
        user = self.authenticate(auth_header)
        roles = self.admin_roles.get(user.realm, [])
        if self.is_authorized(user, roles):
            return user

    def has_roles(self, auth_header, roles):
        """Authenticate and authorize user, testing if they have any of the provided roles"""
        user = self.authenticate(auth_header)
        if self.is_authorized(user, roles):
            return user

    def can_access_collection(self, auth_header, collection):
        """Authenticate and authorize a user, testing if they can access a collection"""
        user = self.authenticate(auth_header)
        roles = collection.roles.get(user.realm, [])
        if isinstance(roles, str) and roles == "any":
            return user
        if self.is_authorized(user, roles):
            return user

    def collect_metric_info(
        self,
    ) -> Dict[str, Dict[str, Union[None, int, float, str, Status, MetricType]]]:
        metrics = {}
        for a in self.authorizers:
            metrics["authorizer-" + a.realm() + "-" + a.name()] = a.collect_metric_info()
        for a in self.authenticators:
            metrics["authenticator-" + a.realm() + "-" + a.name()] = a.collect_metric_info()
        return metrics

    @staticmethod
    def is_authorized(user, roles):
        """Checks if the user has any of the provided roles"""

        # roles can be a dict of realm:[roles] mapping; find the relevant realm.
        if isinstance(roles, dict):
            if user.realm not in roles:
                raise ForbiddenRequest("Not authorized to access this resource.")
            roles = roles[user.realm]

        # roles can be a single value; convert to a list
        if not isinstance(roles, (tuple, list, set)):
            roles = [roles]

        for required_role in roles:
            if required_role in user.roles:
                return True

        raise ForbiddenRequest("Not authorized to access this resource.")
