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

from . import datasource


class EchoDataSource(datasource.DataSource):
    def __init__(self, config):
        self.type = config["type"]
        assert self.type == "echo"

    def get_type(self):
        return self.type

    def archive(self, request):
        self.data = self.input_data
        return True

    def retrieve(self, request):
        try:
            self.data = request.user_request.encode("utf-8")
        except (UnicodeDecodeError, AttributeError):
            self.data = request.user_request
        return True

    def repr(self):
        return self.config.get("repr", "echo")

    def result(self, request):
        yield self.data

    def match(self, request):
        return

    def destroy(self, request) -> None:
        pass

    def mime_type(self) -> str:
        return "text"
