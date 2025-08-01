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

import copy
import logging
import os
import tempfile

from ecmwfapi import ECMWFDataServer

from . import datasource

# from .datasource import convert_to_mars_request


class WebMARSDataSource(datasource.DataSource):
    def __init__(self, config):
        self.type = config["type"]
        self.config = config
        assert self.type == "webmars"
        self.email = ""  # not required
        self.url = config.get("url", "https://api.ecmwf.int/v1")
        self.key = config.get("key", "")
        self.tmp_dir = config.get("tmp_dir", None)
        self.override_mars_email = config.get("override_email")
        self.override_mars_apikey = config.get("override_apikey")

    def get_type(self):
        return self.type

    def archive(self, request):
        raise NotImplementedError("Archiving not implemented for webmars data source")

    def retrieve(self, request):

        email, key = self.get_user(request)

        self.server = ECMWFDataServer(email=email, url=self.url, key=key)

        self.data = tempfile.NamedTemporaryFile(delete=False, dir=self.tmp_dir)
        r = copy.deepcopy(request.coerced_request)
        r["target"] = self.data.name
        _environ = dict(os.environ)
        try:
            os.environ["http_proxy"] = os.getenv("POLYTOPE_PROXY", "")
            os.environ["HTTP_PROXY"] = os.getenv("POLYTOPE_PROXY", "")
            os.environ["https_proxy"] = os.getenv("POLYTOPE_PROXY", "")
            os.environ["HTTPS_PROXY"] = os.getenv("POLYTOPE_PROXY", "")
            self.server.retrieve(r)
        finally:
            os.environ.clear()
            os.environ.update(_environ)

        return True

    def result(self, request):
        f = open(self.data.name, "rb")

        while True:
            d = f.read(2 * 1024 * 1024)
            if d:
                yield d
            else:
                break

        f.close()
        os.remove(self.data.name)

        return

    def destroy(self, request) -> None:
        pass

    def mime_type(self) -> str:
        return "application/x-grib"

    def get_user(self, request):
        try:
            if self.override_mars_email:
                logging.info("Overriding MARS_USER_EMAIL with {}".format(self.override_mars_email))
                mars_user = self.override_mars_email
            else:
                mars_user = request.user.attributes["ecmwf-email"]

            if self.override_mars_apikey:
                logging.info("Overriding MARS_USER_TOKEN with {}".format(self.override_mars_apikey))
                mars_token = self.override_mars_apikey
            else:
                mars_token = request.user.attributes["ecmwf-apikey"]

            # logging.info("Accessing MARS on behalf of user {} with token {}".format(mars_user, mars_token))

        except Exception:
            logging.error("MARS request aborted because user does not have associated ECMWF credentials")
            raise Exception()

        return mars_user, mars_token
