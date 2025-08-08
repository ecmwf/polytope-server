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

from ..metric import Metric, MetricType
from ..request import Status


class MetricStore(ABC):
    """MetricStore is an interface for database-based storage for Metric objects"""

    def __init__(self):
        """Initialize a metric store"""

    @abstractmethod
    def add_metric(self, metric: Metric) -> None:
        """Add a metric to the request store"""

    @abstractmethod
    def get_metric(self, uuid: str) -> Metric:
        """Fetch metric from the metric store"""

    @abstractmethod
    def get_metrics(self, ascending=None, descending=None, limit=None, exclude_fields=None, **kwargs) -> List[Metric]:
        """Returns [limit] metrics which match kwargs, ordered by
        ascending/descenging keys (e.g. ascending = 'timestamp')"""

    @abstractmethod
    def remove_metric(self, uuid: str) -> None:
        """Remove a metric from the metric store"""

    # @abstractmethod
    # def update_metric(self, metric: Metric) -> None:
    #    """ Updates a stored metric """

    @abstractmethod
    def get_type(self) -> str:
        """Returns the type of the metric_store in use"""

    @abstractmethod
    def wipe(self) -> None:
        """Wipe the metric store"""

    @abstractmethod
    def collect_metric_info(
        self,
    ) -> Dict[str, Union[None, int, float, str, Status, MetricType]]:
        """Collect dictionary of metrics"""

    @abstractmethod
    def remove_old_metrics(self, cutoff: datetime.datetime) -> int:
        """Remove metrics older than cutoff date.

        Args:
            cutoff: datetime object representing the cutoff date.

        Returns:
            int: Number of removed metrics.
        """


type_to_class_map = {"mongodb": "MongoMetricStore", "dynamodb": "DynamoDBMetricStore"}


def create_metric_store(metric_store_config=None):
    if metric_store_config is None:
        metric_store_config = {"mongodb": {}}

    db_type = next(iter(metric_store_config.keys()))

    assert db_type in type_to_class_map.keys()

    MetricStoreClass = importlib.import_module("polytope_server.common.metric_store." + db_type + "_metric_store")
    return getattr(MetricStoreClass, type_to_class_map[db_type])(metric_store_config.get(db_type))
