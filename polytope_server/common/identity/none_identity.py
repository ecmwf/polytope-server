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

from ..exceptions import EndpointNotImplemented
from . import identity


class NoneIdentity(identity.Identity):
    def __init__(self, config):
        self.config = config

    def add_user(self, username: str, password: str, roles: list) -> bool:
        raise EndpointNotImplemented("This Polytope server is not configured with identity management.")

    def remove_user(self, username: str) -> bool:
        raise EndpointNotImplemented("This Polytope server is not configured with identity management.")

    def wipe(self) -> None:
        raise EndpointNotImplemented("This Polytope server is not configured with identity management.")

    def collect_metric_info(self):
        return {}
