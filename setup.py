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

import io
import re

from setuptools import find_packages, setup

__version__ = re.search(
    r'__version__\s*=\s*[\'"]([^\'"]*)[\'"]',  # It excludes inline comment too
    io.open("polytope_server/version.py", encoding="utf_8_sig").read(),
).group(1)

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name="polytope_server",
    version=__version__,
    description="Polytope server.",
    url="https://github.com/ecmwf-projects/polytope-server",
    author="ECMWF",
    author_email="james.hawkes@ecmwf.int",
    packages=find_packages(),
    install_requires=requirements,
    zip_safe=False,
    include_package_data=True,
)
