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
from getpass import getpass

import requests

from . import helpers


class Admin:
    def __init__(self, config, auth, logger=None):
        self.config = config
        self.auth = auth
        if logger:
            self._logger = logger
        else:
            self._logger = logging.getLogger(__name__)

    # POST /api/v1/auth/users
    def create_user(self, username, password=None, affiliation=None, role="guest", soft_limit=1, hard_limit=10):
        """
        Create a Polytope user.

        Creates a user on the Polytope server with the specified
        credentials, affiliation and soft/hard limits of requests.

        The user name, password and affiliation are prompted if not
        specifically provided via the relevant parameters.

        The soft limit, which is set to 1 by default, indicates the
        number of requests allowed to be in queued status (e.g.
        processed by the Polytope system) simultaneously for the
        created user. If a user submits more requests than the allowed
        soft limit, these will be accepted by the server but will not
        be queued until previous requests finish. Their status
        meanwhile will be 'waiting to be queued'.

        If a user submits more than 'hard_limit' requests, these will
        be rejected upfront by the Polytope server.

        After creting a user, it can only be used if configured as
        default Polytope client user via 'polytope set config username'.
        If the name of the created user matches the default UNIX user
        on the system running the client, that user name will be used
        automatically by default without needing to configure it
        explicitly.

        :param username: Name of the user to be created.
        :type username: str
        :param password: Password for the user.
        :type password: str
        :param affiliation: Affiliation of the user.
        :type affiliation: str
        :param role: Role assigned to the new user. Defaults to 'guest'
        :type role: str
        :param soft_limit: Soft limit of simultaneously 'queueable'
                           requests for the user.
        :type soft_limit: int
        :param hard_limit: Hard limit of simultaneously 'acceptable'
                           requests for the user.
        :type hard_limit: int
        :returns: None
        """
        situation = "trying to create a user"

        if not password:
            password = getpass(prompt="New user's password: ")
        if not affiliation:
            affiliation = input("Affiliation: ")

        self._logger.info(
            (
                "Creating user {} with password ****, " + "affiliation {}, soft limit {}, and hard limit {}" + "..."
            ).format(username, affiliation, soft_limit, hard_limit)
        )
        url = self.config.get_url("users")
        headers = {"Authorization": ", ".join(self.auth.get_auth_headers())}
        data = {
            "username": username,
            "password": password,
            "group": affiliation,
            "role": role,
            "soft_limit": str(soft_limit),
            "hard_limit": str(hard_limit),
        }
        method = "post"
        expected_responses = [requests.codes.ok]
        response, _ = helpers.try_request(
            method,
            situation=situation,
            expected=expected_responses,
            logger=self._logger,
            url=url,
            json=data,
            headers=headers,
            skip_tls=self.config.get()["skip_tls"],
        )
        self._logger.info("User created successfully.")

    # DELETE /api/v1/auth/users
    def delete_user(self, username):
        """
        Delete a Polytope user.

        Deletes a user from the Polytope server. If the password is
        not provided, it will be prompted.

        :param username: Name of the user to be deleted.
        :type username: str
        :returns: None
        """
        situation = "trying to delete a user"

        self._logger.info("Deleting user {}...".format(username))
        url = self.config.get_url("users")
        headers = {"Authorization": ", ".join(self.auth.get_auth_headers())}
        data = {"username": username}
        method = "delete"
        expected_responses = [requests.codes.ok]
        response, _ = helpers.try_request(
            method=method,
            situation=situation,
            expected=expected_responses,
            logger=self._logger,
            url=url,
            json=data,
            headers=headers,
            skip_tls=self.config.get()["skip_tls"],
        )
        self.auth.erase(username)
        self._logger.info("User deleted successfully.")

    # GET /api/v1/test
    def ping(self):
        """
        Check server availability.

        Informs whether the Polytope server (as specified in the Polytope
        client configuration address and port) is reachable and operating.

        See 'polytope list config' and 'polytope set config'.

        :returns: None
        """
        situation = "trying to ping the Polytope server for status"

        url = self.config.get_url("ping")
        method = "get"
        expected_responses = [requests.codes.ok]
        response, _ = helpers.try_request(
            method=method,
            situation=situation,
            expected=expected_responses,
            logger=self._logger,
            url=url,
            skip_tls=self.config.get()["skip_tls"],
        )
        message = "The Polytope server is operating and accessible."
        self._logger.info(message)
