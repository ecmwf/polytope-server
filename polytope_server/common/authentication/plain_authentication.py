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

from ..auth import User
from ..exceptions import ForbiddenRequest
from . import authentication


class PlainAuthentication(authentication.Authentication):
    def __init__(self, name, realm, config):
        self.config = config
        self.users = config["users"]

        super().__init__(name, realm, config)

    def authentication_type(self):
        return "Basic"

    def authentication_info(self):
        return "Authenticate with username and password"

    def authenticate(self, credentials: str) -> User:

        # credentials should be of the form 'base64(<username>:<API_key>)'
        try:
            decoded = base64.b64decode(credentials).decode("utf-8")
            auth_user, auth_password = decoded.split(":", 1)
        except UnicodeDecodeError:
            raise ForbiddenRequest("Credentials could not be decoded")
        except ValueError:
            raise ForbiddenRequest("Credentials could not be unpacked")

        for u in self.users:
            if u["uid"] == auth_user and u["password"] == auth_password:
                user = User(auth_user, self.realm())
                user.attributes = u.get("attributes", {})
                return user

        raise ForbiddenRequest("Invalid credentials")

    def collect_metric_info(self):
        return {}
