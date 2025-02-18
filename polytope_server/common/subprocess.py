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
import subprocess
from subprocess import CalledProcessError


class Subprocess:
    def __init__(self):
        self.subprocess = None
        self.output = None

    def run(self, cmd, cwd=None, env=None):
        env = {**os.environ, **(env or None)}
        logging.debug("Calling {} in directory {} with env {}".format(cmd, cwd, env))
        self.subprocess = subprocess.Popen(
            cmd,
            env=env,
            cwd=cwd,
            shell=False,
            stderr=subprocess.STDOUT,
            stdout=subprocess.PIPE,
        )

    def running(self):
        return self.subprocess.poll() is None

    def returncode(self):
        return self.subprocess.poll()

    def finalize(self, request, filter=None):
        """Close subprocess and decode output"""

        out, err = self.subprocess.communicate()
        logging.info(out.decode())
        self.output = out.decode().splitlines()

        for line in self.output:
            if filter and filter in line:
                request.user_message += line + "\n"
        self.subprocess.args
        if self.returncode() != 0:
            raise CalledProcessError(self.returncode(), self.subprocess.args, out, err)
