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
import warnings
from abc import ABC, abstractmethod
from typing import AnyStr, Iterator, List, Tuple

deprecated_staging_types = {
    "s3_boto3": "s3",
}

type_to_class_map = {
    "polytope": "PolytopeStaging",
    "s3": "S3Staging",
    # 's3_boto3' is no longer supported, but we keep it here for backward compatibility
}


class ResourceInfo:
    def __init__(self, name, size, last_modified=None):
        self.name = name
        self.size = size
        self.last_modified = last_modified

    def __repr__(self):
        return f"ResourceInfo({self.name}, {self.size}, {self.last_modified})"


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
    def get_url_prefix(self) -> str:
        """Get url prefix for all objects (e.g. bucket name or other static URL segment)"""


def create_staging(staging_config=None) -> Staging:

    if staging_config is None:
        staging_config = {"polytope": {}}

    staging_type = next(iter(staging_config.keys()))

    # Check if the staging type is deprecated
    if staging_type in deprecated_staging_types:
        new_staging_type = deprecated_staging_types[staging_type]
        warnings.warn(
            f"'{staging_type}' is deprecated. Please use '{new_staging_type}' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        # Replace the deprecated key with the new one in the config
        staging_config[new_staging_type] = staging_config.pop(staging_type)
        staging_type = new_staging_type

    # Dynamically import the correct module based on the updated staging_type
    module_name = f"polytope_server.common.staging.{staging_type}_staging"
    StagingModule = importlib.import_module(module_name)

    # Retrieve the class name from the type_to_class_map
    class_name = type_to_class_map[staging_type]
    StagingClass = getattr(StagingModule, class_name)

    # Instantiate and return the staging object with the appropriate config
    return StagingClass(staging_config[staging_type])
