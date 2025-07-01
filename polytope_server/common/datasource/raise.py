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


class RaiseDataSourceException(Exception):
    pass


class RaiseDataSource(datasource.DataSource):
    def __init__(self, config):
        self.config = config
        self.type = config["type"]
        self.error_message = self.config.get("error_message", "Datasource raised an error!")
        assert self.type == "raise"

    def get_type(self):
        return self.type

    def archive(self, request):
        raise RaiseDataSourceException(self.error_message)

    def retrieve(self, request):
        raise RaiseDataSourceException(self.error_message)

    def result(self, request):
        yield None

    def destroy(self, request) -> None:
        pass

    def mime_type(self) -> str:
        return "text"
