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

import requests

from ..caching import cache
from ..exceptions import ForbiddenRequest
from ..user import User
from . import authentication


class FederationAuthentication(authentication.Authentication):
    def __init__(self, name, realm, config):
        self.config = config
        self.secret = self.config.get("secret")
        assert realm == "polytope"
        self.allowed_realms = self.config.get("allowed-realms", "any")
        super().__init__(name, realm, config)

    def cache_id(self):
        return self.config

    def authentication_type(self):
        return "Federation"

    def authentication_info(self):
        return "Authenticates on behalf of a user using a Polytope federated service account <secret>"

    @cache(lifetime=120)
    def authenticate(self, credentials: str) -> User:
        # credentials should be of the form 'Federation <secret>:<username>:<realm>'

        try:
            credentials, proxy_user, proxy_realm = credentials.split(":", 2)
        except ValueError:
            raise ForbiddenRequest("Credentials could not be unpacked")

        if self.allowed_realms != "any" and proxy_realm not in self.allowed_realms:
            raise ForbiddenRequest("Federation does not allow forwarding of users in realm {}".format(proxy_realm))

        if credentials != self.secret:
            raise ForbiddenRequest("Invalid credentials.")

        user = User(proxy_user, proxy_realm)
        return user

    def collect_metric_info(self):
        return {}


#################################################


def retrieve_ecmwfapi_user(key, url="https://api.ecmwf.int/v1", proxy=""):
    url = url.rstrip("/")
    proxies = {"http": proxy, "https": proxy}
    response = requests.get(url + "/who-am-i?token=" + key, proxies=proxies)

    if response.status_code == 403:
        raise KeyError("Invalid Key")
    response.raise_for_status()

    result = response.json()

    uid = result["uid"]
    email = result["email"]
    # can also get first_name, last_name, code, full_name

    return uid, email
