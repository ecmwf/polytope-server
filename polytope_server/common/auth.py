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

from .authotron import Authotron
from .collection import Collection
from .legacy_auth import LegacyAuthHelper
from .user import User


class AuthHelper:
    """Temporary encapsulation of legacy auth and auth-o-tron adapters"""

    def __init__(self, config: dict):
        # if authotron in config, then internal object is authotron, else legacy helper
        if "auth-o-tron" in config.get("authentication", {}):
            logging.debug("Using Authotron for authentication")
            self.auth = Authotron(config)
        else:
            logging.debug("Using LegacyAuthHelper for authentication")
            self.auth = LegacyAuthHelper(config)

    def authenticate(self, auth_header: str) -> User:
        """Returns authenticated User, or raises UnauthorizedRequest"""

        return self.auth.authenticate(auth_header)

    def has_admin_access(self, auth_header: str):
        """Authenticate and authorize user, testing if they have admin rights"""
        return self.auth.has_admin_access(auth_header)

    def has_roles(self, auth_header: str, roles: list):
        """Authenticate and authorize user, testing if they have any of the provided roles"""
        return self.auth.has_roles(auth_header, roles)

    def can_access_collection(self, auth_header: str, collection: Collection):
        """Authenticate and authorize a user, testing if they can access a collection"""
        return self.auth.can_access_collection(auth_header, collection)

    def collect_metric_info(
        self,
    ) -> dict:
        return self.auth.collect_metric_info()
