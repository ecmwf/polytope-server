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
import enum
import socket
import uuid

from .request import Status


class MetricType(enum.Enum):
    GENERIC = "generic"
    WORKER_STATUS_CHANGE = "worker_status_change"
    WORKER_INFO = "worker_info"
    REQUEST_STATUS_CHANGE = "request_status_change"
    STORAGE_INFO = "storage_info"
    CACHE_INFO = "cache_info"
    QUEUE_INFO = "queue_info"


class Metric:
    """A sealed class representing a metric"""

    __slots__ = ["uuid", "timestamp", "type"]

    def __init__(self, from_dict=None, **kwargs):
        for slot in self.get_slots():
            self.__setattr__(slot, None)

        self.uuid = str(uuid.uuid4())
        self.timestamp = datetime.datetime.utcnow().timestamp()
        self.type = MetricType.GENERIC

        for k, v in kwargs.items():
            self.__setattr__(k, v)

        if from_dict:
            self.deserialize(from_dict)

    def get_slots(self):
        res = []
        for slots in [getattr(cls, "__slots__", []) for cls in type(self).__mro__]:
            res += slots
        return res

    @classmethod
    def serialize_slot(cls, key, value, ndigits=None):
        if key == "type" and value:
            return value.value
        if type(value) is float and ndigits:
            return round(value, ndigits)
        return value

    @classmethod
    def deserialize_slot(cls, key, value):
        if key == "type" and value:
            return MetricType(value)
        return value

    def serialize(self, ndigits=None):
        """Serialize the metric object to a dictionary with plain data types.
        A round factor 'ndigits' can be spcified for rounding float values.
        """
        result = {}
        for k in self.get_slots():
            v = self.__getattribute__(k)
            result[k] = self.serialize_slot(k, v, ndigits=ndigits)
        return result

    def deserialize(self, dict):
        """Modify the metric by deserializing a dictionary into it"""
        for k, v in dict.items():
            self.__setattr__(k, self.deserialize_slot(k, v))

    def update(self, **kwargs):
        for k, v in kwargs.items():
            self.__setattr__(k, v)
        self.timestamp = datetime.datetime.utcnow().timestamp()

    def __eq__(self, other):
        if isinstance(other, Metric):
            return other.uuid == self.uuid
        return False


class WorkerStatusChange(Metric):
    __slots__ = [
        "host",
        "status",
    ]

    def __init__(self, **kwargs):
        super().__init__(type=MetricType.WORKER_STATUS_CHANGE, host=socket.gethostname(), **kwargs)


class WorkerInfo(Metric):
    __slots__ = [
        "host",
        "status",
        "status_time",
        "request_id",
        "requests_processed",
        "requests_failed",
        "total_idle_time",
        "total_processing_time",
    ]

    def __init__(self, **kwargs):
        super().__init__(type=MetricType.WORKER_INFO, host=socket.gethostname(), **kwargs)


class RequestStatusChange(Metric):
    __slots__ = ["host", "status", "request_id", "user_id"]

    def __init__(self, **kwargs):
        super().__init__(type=MetricType.REQUEST_STATUS_CHANGE, host=socket.gethostname(), **kwargs)

    @classmethod
    def serialize_slot(cls, key, value, **kwargs):
        if key == "status" and value:
            return value.value
        return super().serialize_slot(key, value, **kwargs)

    @classmethod
    def deserialize_slot(cls, key, value):
        if key == "status" and value:
            return Status(value)
        return super().deserialize_slot(key, value)


class QueueInfo(Metric):
    __slots__ = [
        "queue_host",
        "total_queued",
    ]

    def __init__(self, **kwargs):
        super().__init__(type=MetricType.QUEUE_INFO, **kwargs)


class CacheInfo(Metric):
    __slots__ = [
        "hits",
        "misses",
    ]

    def __init__(self, **kwargs):
        super().__init__(type=MetricType.CACHE_INFO, **kwargs)


class StorageInfo(Metric):
    __slots__ = [
        "storage_host",
        "storage_type",
        "storage_space_used",
        "storage_space_limit",
        "device_space_used",
        "device_space_limit",
        "entries",
    ]

    def __init__(self, **kwargs):
        super().__init__(type=MetricType.STORAGE_INFO, **kwargs)


class MongoStorageInfo(StorageInfo):
    __slots__ = [
        "collection_name",
        "db_space_used",
        "db_space_limit",
        "db_name",
    ]


class S3StorageInfo(StorageInfo):
    __slots__ = ["bucket_space_used", "bucket_space_limit", "bucket_name"]
