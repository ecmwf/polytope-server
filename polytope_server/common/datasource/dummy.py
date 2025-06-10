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


class DummyDataSource(datasource.DataSource):
    def __init__(self, config):
        self.config = config
        self.type = config["type"]
        assert self.type == "dummy"

    def get_type(self):
        return self.type

    def archive(self, request):
        raise NotImplementedError()

    def retrieve(self, request):
        try:
            self.size = int(request.user_request.encode("utf-8"))
        except ValueError:
            raise ValueError("Request should be an integer (size of random data to generate)")

        if self.size < 0:
            raise ValueError("Size must be non-negative")

        return True

    def result(self, request):
        chunk_size = 2 * 1024 * 1024
        data_generated = 0
        while data_generated < self.size:
            remaining_size = self.size - data_generated
            current_chunk_size = min(chunk_size, remaining_size)
            yield b"x" * current_chunk_size
            data_generated += current_chunk_size

    def destroy(self, request) -> None:
        pass

    def mime_type(self) -> str:
        return "application/x-grib"
