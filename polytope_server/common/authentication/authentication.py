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
from abc import ABC, abstractmethod
from importlib import import_module
from typing import Dict, Union

from ..metric import MetricType
from ..request import Status
from ..user import User

#######################################################


class Authentication(ABC):
    def __init__(self, name, realm, config):
        """Initialize an Authentication object"""
        self._name = name
        self._realm = realm

    @abstractmethod
    def authenticate(self, credentials: str) -> User:
        """Validates if a user's authentication information is correct.
        credentials is the string provided by the user immediately after the
        authentication type in the 'Authorization' header.
        e.g. for 'Authorization: Bearer <key>', credentials will be <key>
        Returns the authenticated User, or raises ForbiddenRequest
        """

    @abstractmethod
    def authentication_type(self) -> str:
        """Returns the type of authentication expected (e.g. Basic, Bearer, ECMWF)"""

    @abstractmethod
    def authentication_info(self) -> str:
        """Returns a short description/hint to the user on how to authenticate"""

    @abstractmethod
    def collect_metric_info(
        self,
    ) -> Dict[str, Union[None, int, float, str, Status, MetricType]]:
        """Collect dictionary of metrics"""

    def realm(self) -> str:
        """Return the realm this authenticator is set up for (e.g. ECMWF)"""
        return self._realm

    def name(self) -> str:
        """Return the unique name of this authenticator"""
        return self._name


#######################################################

type_to_class_map = {
    "mongodb": "MongoAuthentication",
    "mongoapikey": "ApiKeyMongoAuthentication",
    "ecmwfapi": "ECMWFAuthentication",
    "plain": "PlainAuthentication",
    "keycloak": "KeycloakAuthentication",
    "federation": "FederationAuthentication",
    "jwt" : "JWTBearerAuthentication",
}


def create_authentication(name, realm, config):

    # Find the class matching config.type
    type = config.get("type")
    assert type is not None
    module = import_module("polytope_server.common.authentication." + type + "_authentication")
    authentication_class = type_to_class_map[type]

    # Call the constructor
    constructor = getattr(module, authentication_class)
    authentication = constructor(name, realm, config)

    logging.info("authentication {} of type {} initialized ({}).".format(name, type, authentication_class))

    return authentication
