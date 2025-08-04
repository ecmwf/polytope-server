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


class Authorization(ABC):
    def __init__(self, name, realm, config):
        """Initialize an Authorization object"""
        self._name = name
        self._realm = realm

    @abstractmethod
    def get_roles(self, user: User) -> list:
        """Get roles associated with this user"""

    @abstractmethod
    def get_attributes(self, user: User) -> dict:
        """Get attributes associated with this user"""

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
    "plain": "PlainAuthorization",
    "ldap": "LDAPAuthorization",
    "mongodb": "MongoDBAuthorization",
}


def create_authorization(name, realm, config):
    # Find the class matching config.type
    type = config.get("type")
    module = import_module("polytope_server.common.authorization." + type + "_authorization")
    authorization_class = type_to_class_map[type]

    # Call the constructor
    constructor = getattr(module, authorization_class)
    authorization = constructor(name, realm, config)

    logging.info("authorization {} of type {} initialized ({}).".format(name, type, authorization_class))

    return authorization
