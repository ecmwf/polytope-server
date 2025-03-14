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

    def run(self, cmd, cwd=None, env=None):
        env = {**os.environ, **(env or None)}
        logging.debug("Calling {} in directory {} with env {}".format(cmd, cwd, env))
        self.subprocess = subprocess.Popen(
            cmd,
            env=env,
            cwd=cwd,
            shell=False,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE if not env.get("FDB_DEBUG") == "1" else None,
        )

    def read_output(self, request, filter=None):
        """Read and log output from the subprocess without blocking"""
        reads = [self.subprocess.stdout, self.subprocess.stderr]
        ret = select.select(reads, [], [], 0)
        while ret[0]:
            for fd in ret[0]:
                if fd == self.subprocess.stdout:
                    line = self.subprocess.stdout.readline()
                    if line:
                        logging.info(line.decode().strip())
                        if filter and filter in line.decode():
                            request.user_message += line.decode() + "\n"
                if fd == self.subprocess.stderr:
                    line = self.subprocess.stderr.readline()
                    if line:
                        logging.error(line.decode().strip())
                        if filter and filter in line.decode():
                            request.user_message += line.decode() + "\n"
            ret = select.select(reads, [], [], 0)

    def running(self):
        return self.subprocess.poll() is None

    def returncode(self):
        return self.subprocess.poll()

    def finalize(self, request, filter):
        """Close subprocess and decode output"""

        returncode = self.subprocess.wait()

        if returncode != 0:
            raise CalledProcessError(returncode, self.subprocess.args)
