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

from ..auth import User
from ..metric import MetricType
from ..request import Status


class ApiKey:
    def __init__(self):
        self.key = ""
        self.expiry = ""

    def serialize(self):
        return self.__dict__


#######################################################


class KeyGenerator(ABC):
    def __init__(self, config):
        """Initialize an Identity object"""

    @abstractmethod
    def create_key(self, user: User) -> ApiKey:
        """Create an API Key"""

    @abstractmethod
    def collect_metric_info(
        self,
    ) -> Dict[str, Union[None, int, float, str, Status, MetricType]]:
        """Collect dictionary of metrics"""


#######################################################

type_to_class_map = {"mongodb": "MongoKeyGenerator", "none": "NoneKeyGenerator"}  #


def create_keygenerator(generator_config=None):

    if generator_config is None:
        generator_config = {"type": "none"}

    keygenerator_type = generator_config.get("type")

    assert keygenerator_type in type_to_class_map.keys()

    RequestStoreClass = import_module("polytope_server.common.keygenerator." + keygenerator_type + "_keygenerator")
    return getattr(RequestStoreClass, type_to_class_map[keygenerator_type])(generator_config)
