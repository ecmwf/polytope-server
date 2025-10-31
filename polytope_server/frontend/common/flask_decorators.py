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

import collections.abc
import json
import logging

from flask import Response

handler_dict = {
    "flask": "FlaskHandler",
    "restplus": "RestplusHandler",
    "falcon": "FalconHandler",
}


def RequestSucceeded(response: collections.abc.Mapping | str) -> Response:
    if not isinstance(response, collections.abc.Mapping):
        response = {"message": response}
    status = 200
    logging.info("Request succeeded", extra={"response": response, "http.status": status})
    return Response(response=json.dumps(response), status=status, mimetype="application/json")


def RequestAccepted(response: collections.abc.Mapping | str) -> Response:
    if not isinstance(response, collections.abc.Mapping):
        response = {"message": response}
    if response["message"] == "":
        response["message"] = "Request {}".format(response["status"])
    if response["location"]:
        headers = {"Location": response["location"], "Retry-After": 5}
        response.pop("location")
    status = 202
    logging.info("Request accepted", extra={"response": response, "http.status": status})
    return Response(
        response=json.dumps(response),
        status=status,
        mimetype="application/json",
        headers=headers,
    )


def RequestRedirected(response: collections.abc.Mapping) -> Response:
    headers = {"Location": response["location"]}
    response.pop("message")  # Remove message from successful requests
    assert response["status"] == "processed"
    response.pop("status")
    status = 303
    logging.info("Request redirected", extra={"response": response, "http.status": status})
    return Response(
        response=json.dumps(response),
        status=status,
        mimetype="application/json",
        headers=headers,
    )
