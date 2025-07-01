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


class InvalidConfig(Exception):
    pass


class HTTPException(Exception):
    """Baseclass for custom HTTP exceptions."""

    code = None
    description = None

    def __init__(self, description=None, response=None):
        super(HTTPException, self).__init__()
        if description is not None:
            self.description = description
        self.response = response

    def __repr__(self):
        return f"{self.__class__.__name__}: {self.description}"


class BadRequest(HTTPException):
    code = 400


class UnauthorizedRequest(HTTPException):
    code = 401

    def __init__(self, message, details, www_authenticate=""):
        super().__init__(message)
        self.www_authenticate = www_authenticate
        self.extra_headers = {"WWW-Authenticate": www_authenticate}


class ForbiddenRequest(HTTPException):
    code = 403


class NotFound(HTTPException):
    code = 404


class Conflict(HTTPException):
    code = 409


class ServerError(HTTPException):
    code = 500


class EndpointNotImplemented(HTTPException):
    code = 501


class ServiceUnavailable(HTTPException):
    code = 503
