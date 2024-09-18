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

import json

from ldap3 import SUBTREE, Connection, Server

from ..auth import User
from . import authorization


class LDAPAuthorization(authorization.Authorization):
    def __init__(self, name, realm, config):
        self.config = config
        assert self.config["type"] == "ldap"
        self.url = config.get("url")
        self.search_base = config.get("search_base")
        self.filter = config.get("filter", "")
        self.ldap_user = config.get("ldap_user", "")
        self.ldap_password = config.get("ldap_password", "")

        # Alternative attribute to use instead of user's username
        self.username_attribute = config.get("username-attribute", None)
        super().__init__(name, realm, config)

    def get_roles(self, user: User) -> list:
        if user.realm != self.realm():
            raise ValueError(
                "Trying to authorize a user in the wrong realm, expected {}, got {}".format(self.realm(), user.realm)
            )
        try:
            if self.username_attribute is None:
                return retrieve_ldap_user_roles(
                    user.username, self.filter, self.url, self.search_base, self.ldap_user, self.ldap_password
                )
            else:
                return retrieve_ldap_user_roles(
                    user.attributes[self.username_attribute],
                    filter=self.filter,
                    url=self.url,
                    search_base=self.search_base,
                )
        except KeyError:
            return []

    def get_attributes(self, user: User) -> dict:
        return {}

    def collect_metric_info(self):
        return {}


#################################################


def retrieve_ldap_user_roles(
    uid: str, filter: str, url: str, search_base: str, ldap_user: str, ldap_password: str
) -> list:
    """
    Takes an ECMWF UID and returns all roles matching
    the provided filter 'filter'.
    """

    server = Server(url)

    connection = Connection(
        server,
        user="CN={},OU=Connectors,OU=Service Accounts,DC=ecmwf,DC=int".format(ldap_user),
        password=ldap_password,
        raise_exceptions=True,
    )

    with connection as conn:
        conn.search(
            search_base=search_base,
            search_filter="(&(objectClass=person)(cn={}))".format(uid),
            search_scope=SUBTREE,
            attributes=["memberOf"],
        )
        user_data = json.loads(conn.response_to_json())
        if len(user_data["entries"]) == 0:
            raise KeyError("User {} not found in LDAP.".format(uid))
        roles = user_data["entries"][0]["attributes"]["memberOf"]

    # Filter roles
    matches = []
    for role in roles:
        if filter is None or filter in role:
            matches.append(role)

    # Parse CN=x,OU=y,OU=z,... into dict and extract 'common name' (CN)
    for i, role in enumerate(matches):
        d = dict(s.split("=") for s in role.split(","))
        matches[i] = d["CN"]

    return matches
