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
                    if fd == self.subprocess.stdout:
                        logging.info(line)
                    elif fd == self.subprocess.stderr:
                        logging.error(line)
                    if err_filter and err_filter in line:
                        request.user_message += line + "\n"
            if not self.running():
                break
            ret = select.select(reads, [], [], 0)

    def running(self):
        return self.subprocess.poll() is None

    def returncode(self):
        return self.subprocess.poll()

    def finalize(self, request, err_filter):
        """Close subprocess and decode output"""

        returncode = self.subprocess.wait()
        for line in self.subprocess.stdout:
            line = line.decode().strip()
            if err_filter and err_filter in line:
                request.user_message += line + "\n"
                logging.error(line)
            else:
                logging.info(line)
        for line in self.subprocess.stderr:
            line = line.decode().strip()
            if err_filter and err_filter in line:
                request.user_message += line + "\n"
            logging.error(line)

        if returncode != 0:
            raise CalledProcessError(returncode, self.subprocess.args)
