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

import logging
import os
import select
import subprocess
from subprocess import CalledProcessError


class Subprocess:
    def __init__(self):
        self.subprocess = None
        self._stdout_buffer = []
        self._stderr_buffer = []

    def run(self, cmd, cwd=None, env=None):
        env = {**os.environ, **(env or {})}
        logging.info("Calling {} in directory {}".format(cmd, cwd), extra={"env": env})
        self.subprocess = subprocess.Popen(
            cmd,
            env=env,
            cwd=cwd,
            shell=False,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )

    def read_output(self, request, err_filter=None):
        """Read and log output from the subprocess without blocking"""
        reads = [i for i in [self.subprocess.stdout, self.subprocess.stderr] if i]
        ret = select.select(reads, [], [], 0)
        while ret[0]:
            for fd in ret[0]:
                line = fd.readline()
                if line:
                    line = line.decode().strip()
                    self._handle_line(fd, line, request, err_filter)
            if not self.running():
                break
            ret = select.select(reads, [], [], 0)

    def running(self):
        return self.subprocess.poll() is None

    def returncode(self):
        return self.subprocess.poll()

    def finalize(self, request, err_filter):
        """Close subprocess and decode output"""
        # fifo has been closed so this process should finish, but sometimes hangs so we set a timeout
        try:
            returncode = self.subprocess.wait(60)
        except subprocess.TimeoutExpired:
            logging.error("Subprocess did not finish in time, killing it")
            self.subprocess.kill()
            returncode = self.subprocess.returncode
        logging.info("Subprocess finished with return code: {}".format(returncode))
        for line in self.subprocess.stdout:
            line = line.decode().strip()
            self._handle_line(self.subprocess.stdout, line, request, err_filter)
        for line in self.subprocess.stderr:
            line = line.decode().strip()
            self._handle_line(self.subprocess.stderr, line, request, err_filter)

        self._flush_buffers(request, err_filter)

        if returncode != 0:
            raise CalledProcessError(returncode, self.subprocess.args)

    def _handle_line(self, fd, line, request, err_filter):
        buffer, log_func = self._get_buffer_and_logger(fd)
        if line.startswith("mars") and buffer:
            self._flush_buffer(buffer, log_func, request, err_filter)
        buffer.append(line)

    def _flush_buffers(self, request, err_filter):
        for buffer, log_func in [
            (self._stdout_buffer, logging.info),
            (self._stderr_buffer, logging.error),
        ]:
            self._flush_buffer(buffer, log_func, request, err_filter)

    def _flush_buffer(self, buffer, log_func, request, err_filter):
        if not buffer:
            return
        message = "\n".join(buffer)
        log_method = logging.error if err_filter and err_filter in message else log_func
        log_method(message)
        if err_filter and err_filter in message:
            request.user_message += message + "\n"
        buffer.clear()

    def _get_buffer_and_logger(self, fd):
        if fd == self.subprocess.stdout:
            return self._stdout_buffer, logging.info
        return self._stderr_buffer, logging.error
