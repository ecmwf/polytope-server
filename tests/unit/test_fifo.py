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

import threading

import pytest

from polytope_server.common.io.fifo import FIFO


class Test:
    def setup_method(self, method):
        pass

    def test_fifo(self):

        fifo = FIFO("test-fifo")

        assert not fifo.ready()

        f = open(fifo.path, "wb")

        assert not fifo.ready()

        f.write(b"abc")
        f.flush()

        assert fifo.ready()
        assert fifo.read_raw(1) == b"a"
        assert fifo.read_raw(2) == b"bc"

        f.write(b"111")
        f.flush()

        assert fifo.ready()
        count = 0
        for x in fifo.data(1):
            assert x == b"1"
            count += 1

            # this loop would read forever because we haven't closed the pipe, close it now
            if count == 3:
                f.write(b"111")
                f.close()

        assert count == 6

        # the fifo has been deleted because we finished reading it
        with pytest.raises(OSError):
            assert fifo.ready()

    def read_all(self, fifo, result):
        while not fifo.ready():
            pass
        for x in fifo.data():
            result[0] += x

    def test_fifo_buffered(self):

        # write 1 MiB of data, we need two threads

        fifo = FIFO("test-fifo")

        data = [b""]

        thread = threading.Thread(target=Test.read_all, args=(self, fifo, data))
        thread.start()

        f = open(fifo.path, "wb")
        for _ in range(1 * 1024):
            f.write(b"x" * 1024)
            f.flush()

        f.close()
        thread.join()

        assert data[0] == b"x" * 1 * 1024 * 1024
        assert len(data[0]) == 1 * 1024 * 1024

        fifo.delete()
