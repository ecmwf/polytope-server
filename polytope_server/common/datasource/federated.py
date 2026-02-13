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

import hashlib
import logging
import time
from http import HTTPStatus

import requests

from . import datasource


class FederatedDataSource(datasource.DataSource):
    def __init__(self, config):
        self.type = config["type"]
        assert self.type == "federated"

        self.url = config["url"]
        self.port = config.get("port", 443)
        self.secret = config["secret"]
        self.collection = config["collection"]
        self.api_version = config.get("api_version", "v1")
        self.result_url = None
        self.mime_type_result = "application/octet-stream"

    def get_type(self):
        return self.type

    def archive(self, request):

        url = "/".join(
            [
                self.url + ":" + str(self.port),
                "api",
                self.api_version,
                "requests",
                self.collection,
            ]
        )
        logging.info("Built URL for request: {}".format(url))

        body = {
            "verb": "archive",
            "request": request.user_request,
        }

        headers = {
            "Authorization": "Federation {}:{}:{}".format(self.secret, request.user.username, request.user.realm)
        }

        # Post the initial request

        response = requests.post(url, json=body, headers=headers)

        if response.status_code != HTTPStatus.ACCEPTED:
            raise Exception("Request could not be POSTed to remote Polytope at {}.\n\
                             HTTP error code {}.\n\
                             Message: {}".format(url, response.status_code, response.content))

        url = response.headers["location"]

        # Post the data to the upload location

        response = requests.post(
            url,
            self.input_data,
            headers={
                **headers,
                "X-Checksum": hashlib.md5(self.input_data).hexdigest(),
            },
        )

        if response.status_code != HTTPStatus.ACCEPTED:
            raise Exception("Data could not be POSTed for upload to remote Polytope at {}.\n\
                             HTTP error code {}.\n\
                             Message: {}".format(url, response.status_code, response.content))

        url = response.headers["location"]
        time.sleep(int(float(response.headers["retry-after"])))

        status = HTTPStatus.ACCEPTED

        # Poll until the request fails or returns 200
        while status == HTTPStatus.ACCEPTED:
            response = requests.get(url, headers=headers, allow_redirects=False)
            status = response.status_code
            logging.info(response.json())
            if "location" in response.headers:
                url = response.headers["location"]
            if "retry-after" in response.headers:
                time.sleep(int(float(response.headers["retry-after"])))

        if status != HTTPStatus.OK:
            raise Exception("Request failed on remote Polytope at {}.\n\
                            HTTP error code {}.\n\
                            Message: {}".format(url, status, response.json()["message"]))

        return True

    def retrieve(self, request):

        url = "/".join(
            [
                self.url + ":" + str(self.port),
                "api",
                self.api_version,
                "requests",
                self.collection,
            ]
        )
        logging.info("Built URL for request: {}".format(url))

        body = {
            "verb": "retrieve",
            "request": request.user_request,
        }

        headers = {
            "Authorization": "Federation {}:{}:{}".format(self.secret, request.user.username, request.user.realm)
        }

        # Post the initial request

        response = requests.post(url, json=body, headers=headers)

        if response.status_code != HTTPStatus.ACCEPTED:
            raise Exception("Request could not be POSTed to remote Polytope at {}.\n\
                             HTTP error code {}.\n\
                             Message: {}".format(url, response.status_code, response.content))

        url = response.headers["location"]
        time.sleep(int(float(response.headers["retry-after"])))

        status = HTTPStatus.ACCEPTED

        # Poll until the request fails or returns 303
        while status == HTTPStatus.ACCEPTED:
            response = requests.get(url, headers=headers, allow_redirects=False)
            status = response.status_code
            if "location" in response.headers:
                url = response.headers["location"]
            if "retry-after" in response.headers:
                time.sleep(int(float(response.headers["retry-after"])))

        if status != HTTPStatus.SEE_OTHER:
            raise Exception("Request failed on remote Polytope at {}.\n\
                            HTTP error code {}.\n\
                            Message: {}".format(url, status, response.json()["message"]))

        self.result_url = url

        return True

    def result(self, request):

        response = requests.get(self.result_url, stream=True)

        self.mime_type_result = response.headers["Content-Type"]

        if response.status_code != HTTPStatus.OK:
            raise Exception(
                "Request could not be downloaded from remote Polytope at {}.\n\
                            HTTP error code {}.\n\
                            Message: {}".format(
                    self.result_url,
                    response.status_code,
                    response.json()["message"],
                )
            )

        try:
            for chunk in response.iter_content(chunk_size=1024):
                yield chunk
        finally:
            response.close()

    def mime_type(self) -> str:
        return self.mime_type_result

    def destroy(self, request) -> None:
        return

    def match(self, request):
        return
