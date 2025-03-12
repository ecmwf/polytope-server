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
        self.output = None
        self.err = None

    def run(self, cmd, cwd=None, env=None):
        env = {**os.environ, **(env or None)}
        logging.debug("Calling {} in directory {} with env {}".format(cmd, cwd, env))
        self.subprocess = subprocess.Popen(
            cmd,
            env=env,
            cwd=cwd,
            shell=False,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )

    def running(self):
        return self.subprocess.poll() is None

    def returncode(self):
        return self.subprocess.poll()

    def read_output(self):
        """Read and log output from the subprocess without blocking"""
        reads = [self.subprocess.stdout.fileno(), self.subprocess.stderr.fileno()]
        ret = select.select(reads, [], [], 0.1)
        for fd in ret[0]:
            if fd == self.subprocess.stdout.fileno():
                line = self.subprocess.stdout.readline()
                if line:
                    if not logging.isEnabledFor(logging.DEBUG):
                        self.output += line.decode().strip() + "\n"
                    logging.info(line.decode().strip())
            if fd == self.subprocess.stderr.fileno():
                line = self.subprocess.stderr.readline()
                if line:
                    self.err += line.decode().strip() + "\n"
                    logging.error(line.decode().strip())

    def finalize(self, request, filter=None):
        """Close subprocess and decode output"""
        while self.running():
            self.read_output()  # Ensure all output is read before finalizing

        request.user_message += self.output

        if self.returncode() != 0:
            raise CalledProcessError(self.returncode(), self.subprocess.args, stderr=self.err)
