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

import importlib
from abc import ABC, abstractmethod
from typing import AnyStr, Dict, Iterator, List, Tuple, Union

from ..metric import MetricType
from ..request import Status

type_to_class_map = {"polytope": "PolytopeStaging", "s3": "S3Staging"}


class ResourceInfo:
    def __init__(self, name, size):
        self.name = name
        self.size = size


class Staging(ABC):
    def __init__(self, staging_config=None):
        """Initialize a data store"""

    @abstractmethod
    def create(self, name: str, data: Iterator[bytes], content_type: str) -> str:
        """Create new resource. If the resource already exists, update it.
        :param name: name of the resource to create
        :data: a python object
        :content_type: a string corresponding to the HTTP 'content-type' header
        :returns: fully-qualified URL to the resource, eg. "http://polytope.com/downloads/{name}"
        """

    @abstractmethod
    def read(self, name: str) -> AnyStr:
        """Read resource, returning the data
        :return: data
        """

    @abstractmethod
    def delete(self, name: str) -> bool:
        """Delete an object, return true on success"""

    @abstractmethod
    def query(self, name: str) -> bool:
        """Query if an object exists in data staging"""

    @abstractmethod
    def stat(self, name: str) -> Tuple[str, int]:
        """Query size of an object"""

    @abstractmethod
    def get_url(self, name: str) -> str:
        """Get url corresponding to object_name"""

    @abstractmethod
    def get_internal_url(self, name: str) -> str:
        """Get native url corresponding to object_name"""

    @abstractmethod
    def get_type(self) -> str:
        """Returns the type of the staging in use"""

    @abstractmethod
    def list(self) -> List[ResourceInfo]:
        """List all resources"""

    @abstractmethod
    def wipe(self) -> None:
        """Delete all resources"""

    @abstractmethod
    def collect_metric_info(
        self,
    ) -> Dict[str, Union[None, int, float, str, Status, MetricType]]:
        """Collect dictionary with metrics"""

    @abstractmethod
    def get_url_prefix(self) -> str:
        """Get url prefix for all objects (e.g. bucket name or other static URL segment)"""


def create_staging(staging_config=None):

    if staging_config is None:
        staging_config = {"polytope": {}}

    staging_type = next(iter(staging_config.keys()))

    StagingClass = importlib.import_module("polytope_server.common.staging." + staging_type + "_staging")
    return getattr(StagingClass, type_to_class_map[staging_type])(staging_config[staging_type])
