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

import datetime
import importlib
from abc import ABC, abstractmethod
from typing import Dict, List, Union

from ..metric import MetricType
from ..request import Request, Status
from ..user import User


class RequestStore(ABC):
    """RequestStore is an interface for database-based storage for Request objects"""

    def __init__(self):
        """Initialize a request store"""

    @abstractmethod
    def add_request(self, request: Request) -> None:
        """Add a request to the request store"""

    @abstractmethod
    def get_request(self, id: str) -> Request | None:
        """Fetch request from the request store"""

    @abstractmethod
    def get_requests(self, ascending=None, descending=None, limit=None, **kwargs) -> List[Request]:
        """Returns [limit] requests which match kwargs, ordered by
        ascending/descenging keys (e.g. ascending = 'timestamp')"""

    @abstractmethod
    def remove_request(self, id: str) -> None:
        """Remove a request from the request store."""

    @abstractmethod
    def revoke_request(self, user: User, id: str) -> int:
        """
        Revoke a queued but unstarted request from the request store.

        Only the user who created the request can revoke it.

        Only requests with status 'waiting' or 'queued' can be removed.

        Args:
            user: User who is revoking the request
            id: ID of the request to be revoked. Alternatively "all" can be used to
                revoke all revokeable requests of the user.

        Returns:
            int: Number of requests revoked.

        Raises:
            NotFound: if the request is not in the request store
            UnauthorizedRequest: if the request belongs to a different user
            ForbiddenRequest: if the request has started processing.
        """

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

    @abstractmethod
    def remove_old_requests(self, cutoff: datetime.datetime) -> int:
        """Remove FAILED and PROCESSED requests older than cutoff date.

        Returns:
            int: Number of removed requests.
        """


type_to_class_map = {"mongodb": "MongoRequestStore", "dynamodb": "DynamoDBRequestStore"}


def create_request_store(request_store_config=None, metric_store_config=None):
    if request_store_config is None:
        request_store_config = {"mongodb": {}}

    db_type = next(iter(request_store_config.keys()))

    assert db_type in type_to_class_map.keys()

    RequestStoreClass = importlib.import_module("polytope_server.common.request_store." + db_type + "_request_store")
    return getattr(RequestStoreClass, type_to_class_map[db_type])(
        request_store_config.get(db_type), metric_store_config
    )
