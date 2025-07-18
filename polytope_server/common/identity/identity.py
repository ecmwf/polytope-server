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

from abc import ABC, abstractmethod
from importlib import import_module
from typing import Dict, Union

from ..metric import MetricType
from ..request import Status

#######################################################


class Identity(ABC):
    def __init__(self, config):
        """Initialize an Identity object"""

    @abstractmethod
    def add_user(self, username: str, password: str, roles: list) -> bool:
        """Add a user"""

    @abstractmethod
    def remove_user(self, username: str) -> bool:
        """Remove a user"""

    @abstractmethod
    def wipe(self) -> None:
        """Wipe all users and authentication information"""

    @abstractmethod
    def collect_metric_info(
        self,
    ) -> Dict[str, Union[None, int, float, str, Status, MetricType]]:
        """Collect dictionary of metrics"""


#######################################################

type_to_class_map = {"mongodb": "MongoDBIdentity", "none": "NoneIdentity"}


def create_identity(identity_config=None):
    if identity_config is None:
        identity_config = {"none": {}}

    identity_type = next(iter(identity_config.keys()))

    assert identity_type in type_to_class_map.keys()

    RequestStoreClass = import_module("polytope_server.common.identity." + identity_type + "_identity")
    return getattr(RequestStoreClass, type_to_class_map[identity_type])(identity_config.get(identity_type))
