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
import logging

import requests

from . import staging


class PolytopeStaging(staging.Staging):
    def __init__(self, config):
        self.host = config.get("host", "0.0.0.0")
        self.port = config.get("port", "8000")
        self.root_dir = config.get("root_dir", "/home/polytope/data")

        self.url = config.get("url", None)
        self.internal_url = "http://%s:%s" % (self.host, self.port)

        logging.info("Opened data staging at {}".format(self.internal_url))

    def create(self, name, data, content_type):

        headers = {"Content-Type": content_type}
        buffer = b""
        for d in data:
            buffer += d
        logging.info("Creating resource: {}".format(name))
        response = requests.put(self.get_internal_url(name), headers=headers, data=buffer)
        if response.status_code != 201:
            raise Exception(
                "Could not create resource {},returned with status code: {}".format(name, response.status_code)
            )
        return self.get_url(name)

    def read(self, name):
        response = requests.get(self.get_internal_url(name), headers={})
        if response.status_code != 200:
            raise Exception(
                "Could not read resource {}, returned with status code: {}".format(name, response.status_code)
            )
        return response._content

    def delete(self, name):
        response = requests.delete(self.get_internal_url(name), headers={})
        if response.status_code == 200:
            return True
        elif response.status_code == 401:
            raise KeyError()
        else:
            raise Exception(
                "Could not delete resource {}, returned with status code: {}".format(name, response.status_code)
            )

    def query(self, name):
        response = requests.head(self.get_internal_url(name), headers={})
        if response.status_code == 200:
            return True
        return False

    def stat(self, name):
        response = requests.head(self.get_internal_url(name), headers={})
        if response.status_code == 200:
            return str(response.headers["Content-Type"]), int(response.headers["Content-Length"])
        elif response.status_code == 404:
            raise KeyError()
        else:
            raise Exception(
                "Could not query size of resource {}, returned with status code: {}".format(name, response.status_code)
            )

    def list(self):
        response = requests.get(self.internal_url, headers={})
        if response.status_code == 200:
            resources = []
            for k, v in json.loads(response.content.decode()).items():
                resources.append(staging.ResourceInfo(k, v))
            return resources
        else:
            raise Exception("Could not list resources, returned with status code: {}".format(response.status_code))

        resources = []
        for k, v in response.json().items():
            resources.append(staging.ResourceInfo(k, v))
        return resources

    def wipe(self):
        raise NotImplementedError()

    def get_url(self, name):
        if self.url is None:
            return None
        return "{}/{}".format(self.url, name)

    def get_internal_url(self, name):
        return "{}/{}".format(self.internal_url, name)

    def get_url_prefix(self):
        return ""

    def get_type(self):
        return "PolytopeStaging"
