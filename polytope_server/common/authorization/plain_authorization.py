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

import fnmatch
import logging

from ..auth import User
from .authorization import Authorization


class PlainAuthorization(Authorization):
    def __init__(self, name, realm, config):
        self.config = config
        assert self.config["type"] == "plain"
        self.roles = self.config.get("roles", {})
        self.user_attributes = self.config.get("user-attributes", {})
        self.role_attributes = self.config.get("role-attributes", {})
        super().__init__(name, realm, config)

    def get_roles(self, user: User) -> list:

        if user.realm != self.realm():
            raise ValueError(
                "Trying to authorize a user in the wrong realm, expected {}, got {}".format(self.realm, user.realm)
            )

        # return all roles this user belongs to
        authorized_roles = []
        for role, users in self.roles.items():
            if user.username in users:
                authorized_roles.append(role)
                logging.debug("User {} given role {}".format(user.username, role))

        return authorized_roles

    def get_attributes(self, user: User) -> dict:
        if user.realm != self.realm():
            raise ValueError(
                "Trying to authorize a user in the wrong realm, expected {}, got {}".format(self.realm, user.realm)
            )

        attributes = {}

        for user_pattern, extra_attributes in self.user_attributes.items():
            if fnmatch.fnmatch(user.username, user_pattern):
                attributes.update(extra_attributes)

        for r in user.roles:
            attributes.update(self.user_attributes.get(r, {}))

        return attributes
