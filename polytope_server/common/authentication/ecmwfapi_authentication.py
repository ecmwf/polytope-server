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

import os

import requests

from ..auth import User
from ..caching import cache
from ..exceptions import ForbiddenRequest, ServiceUnavailable
from . import authentication


class ECMWFAuthentication(authentication.Authentication):
    def __init__(self, name, realm, config):
        self.config = config
        self.url = self.config.get("url")
        self.status_url = self.config.get("status_url")
        self._realm = realm
        self.proxy = os.environ.get("POLYTOPE_PROXY", "")
        super().__init__(name, realm, config)

    def cache_id(self):
        return self.config

    def realm(self):
        return self._realm

    def authentication_type(self):
        return "EmailKey"

    def authentication_info(self):
        return "Authenticate with ECMWF API credentials <email>:<key>"

    @cache(lifetime=120)
    def authenticate(self, credentials: str) -> User:

        # credentials should be of the form '<email>:<API_key>'
        try:
            auth_email, auth_key = credentials.split(":", 1)
        except ValueError:
            raise ForbiddenRequest("Credentials could not be unpacked")

        try:
            uid, email = retrieve_ecmwfapi_user(auth_key, self.url, self.status_url, self.proxy)
        except KeyError:
            raise ForbiddenRequest("Invalid credentials.")

        if auth_email.casefold() != email.casefold():
            raise ForbiddenRequest("Invalid credentials.")

        user = User(uid, self._realm)
        user.attributes["ecmwf-email"] = email
        user.attributes["ecmwf-apikey"] = auth_key
        return user


#################################################


def retrieve_ecmwfapi_user(key, url, status_url, proxy=""):

    url = url.rstrip("/")
    proxies = {"http": proxy, "https": proxy}
    response = requests.get(url + "/who-am-i?token=" + key, proxies=proxies)

    if response.status_code == 403:
        raise KeyError("Invalid Key")
    elif response.status_code >= 500:
        raise ServiceUnavailable("URL {} is temporarily unavailable, could not authenticate. \
             See {}".format(url, status_url))
    response.raise_for_status()

    result = response.json()

    uid = result["uid"]
    email = result["email"]
    # can also get first_name, last_name, code, full_name

    return uid, email
