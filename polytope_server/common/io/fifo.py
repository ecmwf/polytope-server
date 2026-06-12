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

import errno
import logging
import os
import select
import tempfile


class FIFO:
    """Creates a named pipe (FIFO) and reads data from it"""

    def __init__(self, name, dir=None):

        if dir is None:
            dir = tempfile.gettempdir()

        self.path = dir + "/" + name

        os.mkfifo(self.path, 0o600)
        self.fifo = os.open(self.path, os.O_RDONLY | os.O_NONBLOCK)
        logging.debug("FIFO created")

    def ready(self):
        return len(select.select([self.fifo], [], [], 0)[0]) == 1

    def data(self, buffer_size=2 * 1024 * 1024, poll_interval=None, on_poll=None):
        """Yield FIFO data in chunks.

        If poll_interval is set, poll for new data every poll_interval seconds and call
        on_poll() after each poll attempt. poll_interval is expressed in seconds. This
        lets callers perform side effects such as checking subprocess health while
        waiting for more bytes.
        """
        buffer = b""

        while True:
            if poll_interval is None:
                data = self.read_raw()
            else:
                data = self.read_raw(wait=False, timeout=poll_interval)
                if on_poll is not None:
                    on_poll()
                if data == b"":
                    continue
            if data is None:
                break
            buffer += data
            while len(buffer) >= buffer_size:
                output, leftover = buffer[:buffer_size], buffer[buffer_size:]
                buffer = leftover
                yield output

        if buffer != b"":
            yield buffer

    def delete(self):
        """Close and delete FIFO"""
        logging.info("Deleting FIFO.")
        try:
            os.close(self.fifo)
        except Exception as e:
            logging.info(f"Closing FIFO had an exception {e}")
            pass
        try:
            os.unlink(self.path)
        except Exception as e:
            logging.info(f"Deleting FIFO had an exception {e}")
            pass

    def read_raw(self, max_read=2 * 1024 * 1024, wait=True, timeout=0):
        """Read up to max_read bytes.

        When wait is False, wait up to timeout seconds for readability and return b"" if
        no data is available yet. Return None on EOF.
        """
        if not wait and not select.select([self.fifo], [], [], timeout)[0]:
            return b""

        while True:
            try:
                buf = os.read(self.fifo, max_read)
                break
            except OSError as err:
                # Because we opened in non-blocking mode we have to filter out these errors
                if err.errno == errno.EAGAIN or err.errno == errno.EWOULDBLOCK:
                    if not wait:
                        return b""
                else:
                    raise

        if buf != b"":
            return buf
        else:
            return None
