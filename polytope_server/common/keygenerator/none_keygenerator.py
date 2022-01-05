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

from ..auth import User
from ..exceptions import EndpointNotImplemented
from . import ApiKey, keygenerator


class NoneKeyGenerator(keygenerator.KeyGenerator):
    def __init__(self, config):
        pass

    def create_key(self, user: User) -> ApiKey:
        raise EndpointNotImplemented("API key generation is not enabled.")

    def collect_metric_info(self):
        return {}
