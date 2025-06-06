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
from typing import Dict, List, Union

from ..metric import MetricType
from ..request import Request, Status


class RequestStore(ABC):
    """RequestStore is an interface for database-based storage for Request objects"""

    def __init__(self):
        """Initialize a request store"""

    @abstractmethod
    def add_request(self, request: Request) -> None:
        """Add a request to the request store"""

    @abstractmethod
    def get_request(self, id: str) -> Request:
        """Fetch request from the request store"""

    @abstractmethod
    def get_requests(self, ascending=None, descending=None, limit=None, **kwargs) -> List[Request]:
        """Returns [limit] requests which match kwargs, ordered by
        ascending/descenging keys (e.g. ascending = 'timestamp')"""

    @abstractmethod
    def remove_request(self, id: str) -> None:
        """Remove a request from the request store"""

    @abstractmethod
    def update_request(self, request: Request) -> None:
        """Updates a stored request"""

    @abstractmethod
    def get_type(self) -> str:
        """Returns the type of the request_store in use"""

    @abstractmethod
    def wipe(self) -> None:
        """Wipe the request store"""

    @abstractmethod
    def collect_metric_info(
        self,
    ) -> Dict[str, Union[None, int, float, str, Status, MetricType]]:
        """Collect dictionary of metrics"""
