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
import uuid

from .exceptions import ForbiddenRequest


class User:

    __slots__ = ["id", "username", "realm", "roles", "attributes"]

    def __init__(self, username=None, realm=None, from_dict=None):

        self.username = username
        self.realm = realm
        self.roles = []
        self.attributes = {}
        self.id = None
        if from_dict is not None:
            for k, v in from_dict.items():
                self.__setattr__(k, v)

        if self.username is None or self.realm is None:
            raise AttributeError("User object must be instantiated with username and realm attributes")

        self.create_uuid()

    def __setattr__(self, attr, value):
        if attr == "username" and getattr(self, "username", None) is not None:
            raise AttributeError("User username is immutable")
        if attr == "realm" and getattr(self, "realm", None) is not None:
            raise AttributeError("User realm is immutable")
        if attr == "id" and getattr(self, "id", None) is not None:
            raise AttributeError("User ID is immutable")

        super().__setattr__(attr, value)

    def __eq__(self, other):
        return isinstance(other, User) and self.id == other.id

    def create_uuid(self):
        if getattr(self, "id", None) is not None:
            return
        null_uuid = uuid.UUID(int=0)
        unique_string = "{}{}{}{}".format(self.username, len(self.username), self.realm, len(self.realm))
        id = str(uuid.uuid5(null_uuid, unique_string))
        super().__setattr__("id", id)

    def serialize(self):
        result = {}
        for k in self.__slots__:
            v = self.__getattribute__(k)
            result[k] = v
        return result

    def __str__(self):
        return f"User({self.realm}:{self.username})"

    def is_authorized(self, roles: list | set | dict | str) -> bool:
        """Checks if the user has any of the provided roles"""
        logging.debug(f"User roles: {self.roles}")
        logging.debug(f"Allowed roles {roles}")
        # roles can be a dict of realm:[roles] mapping; find the relevant realm.
        if isinstance(roles, dict):
            if self.realm not in roles:
                logging.info(
                    "User {} does not have access to realm {}, roles: {}".format(self.username, self.realm, roles)
                )
                raise ForbiddenRequest("Not authorized to access this resource.")
            roles = roles[self.realm]

        # roles can be a single value; convert to a list
        if not isinstance(roles, (tuple, list, set)):
            roles = [roles]

        for required_role in roles:
            if required_role in self.roles:
                logging.info(f"User {self.username} is authorized with role {required_role}")
                return True

        raise ForbiddenRequest("Not authorized to access this resource.")
