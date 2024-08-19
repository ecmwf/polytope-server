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
from dataclasses import dataclass

import requests
import yaml
from requests import Request

from . import datasource


@dataclass
class IonBeamAPI:
    endpoint: str

    def __post_init__(self):
        assert not self.endpoint.endswith("/")
        self.session = requests.Session()

    def get(self, path: str, **kwargs) -> requests.Response:
        return self.session.get(f"{self.endpoint}/{path}", stream=True, **kwargs)

    def get_bytes(self, path: str, **kwargs) -> requests.Response:
        kwargs["headers"] = kwargs.get("headers", {}) | {"Accept": "application/octet-stream"}
        return self.get(path, **kwargs)

    def get_json(self, path, **kwargs):
        return self.get(path, **kwargs).json()

    def list(self, request: dict[str, str] = {}):
        return self.get_json("list", params=request)

    def head(self, request: dict[str, str] = {}):
        return self.get_json("head", params=request)

    def retrieve(self, request: dict[str, str]) -> requests.Response:
        return self.get_bytes("retrieve", params=request)

    def archive(self, request, file) -> requests.Response:
        files = {"file": file}
        return self.session.post(f"{self.endpoint}/archive", files=files, params=request)


class IonBeamDataSource(datasource.DataSource):
    """
    Retrieve data from the IonBeam REST backend that lives here:
    https://github.com/ecmwf/IonBeam-Deployment/tree/main/docker/rest_api
    """

    read_chunk_size = 2 * 1024 * 1024

    def __init__(self, config):
        """Instantiate a datasource for the IonBeam REST API"""
        self.type = config["type"]
        assert self.type == "ionbeam"

        self.match_rules = config.get("match", {})
        endpoint = config.get("api_endpoint", "http://iotdev-001:18201/api/v1/")
        self.api = IonBeamAPI(endpoint)

    def mime_type(self) -> str:
        """Returns the mimetype of the result"""
        return "application/octet-stream"

    def get_type(self):
        return self.type

    def archive(self, request: Request):
        """Archive data, returns nothing but updates datasource state"""
        r = yaml.safe_load(request.user_request)
        keys = r["keys"]

        with open(r["path"], "rb") as f:
            return self.api.archive(keys, f)

    def list(self, request: Request) -> list:
        request_keys = yaml.safe_load(request.user_request)
        return self.api.list(request_keys)

    def retrieve(self, request: Request) -> bool:
        """Retrieve data, returns nothing but updates datasource state"""

        request_keys = yaml.safe_load(request.user_request)
        self.response = self.api.retrieve(request_keys)
        return True

    def result(self, request: Request):
        """Returns a generator for the resultant data"""
        return self.response.iter_content(chunk_size=self.read_chunk_size, decode_unicode=False)

    def destroy(self, request) -> None:
        """A hook to do essential freeing of resources, called upon success or failure"""

        # requests response objects with stream=True can remain open indefinitely if not read to completion
        # or closed explicitly
        if self.response:
            self.response.close()

    def match(self, request: Request) -> None:
        """Checks if the request matches the datasource, raises on failure"""

        r = yaml.safe_load(request.user_request) or {}

        for k, v in self.match_rules.items():
            # Check that all required keys exist
            if k not in r:
                raise Exception("Request does not contain expected key {}".format(k))
            # Process date rules
            if k == "date":
                # self.date_check(r["date"], v)
                continue
            # ... and check the value of other keys
            v = [v] if isinstance(v, str) else v
            if r[k] not in v:
                raise Exception("got {} : {}, but expected one of {}".format(k, r[k], v))
