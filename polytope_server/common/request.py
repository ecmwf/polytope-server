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
import logging
import uuid

from .dirty_mixin import DirtyTrackingMixin
from .user import User


class Status(enum.Enum):
    WAITING = "waiting"
    UPLOADING = "uploading"
    QUEUED = "queued"
    PROCESSING = "processing"
    PROCESSED = "processed"
    FAILED = "failed"


class Verb(enum.Enum):
    RETRIEVE = "retrieve"
    ARCHIVE = "archive"


class PolytopeRequest(DirtyTrackingMixin):
    """A sealed class representing a request"""

    __slots__ = [
        "id",
        "timestamp",
        "last_modified",
        "user",
        "verb",
        "url",
        "md5",
        "collection",
        "status",
        "user_message",
        "user_request",
        "coerced_request",
        "content_length",
        "content_type",
        "status_history",
        "datasource",
    ]

    def __init__(self, from_dict=None, **kwargs):

        self.id = str(uuid.uuid4())
        self.timestamp = datetime.datetime.now(datetime.timezone.utc).timestamp()
        self.last_modified = datetime.datetime.now(datetime.timezone.utc).timestamp()
        self.user = None
        self.verb = Verb.RETRIEVE
        self.url = ""
        self.collection = ""
        self.status = Status.WAITING
        self.md5 = None
        self.user_message = ""
        self.user_request = ""
        self.coerced_request = {}
        self.content_length = None
        self.content_type = "application/octet-stream"
        self.datasource = ""

        now_ts = datetime.datetime.now(datetime.timezone.utc).timestamp()
        self.status_history = {self.status.value: now_ts}

        if from_dict:
            self.deserialize(from_dict)

        for k, v in kwargs.items():
            self.__setattr__(k, v)

        # After initialization, clear dirty fields, request store has a separate add_request method
        self.clear_dirty()

    def set_status(self, value: Status) -> None:
        self.status = value
        now_ts = datetime.datetime.now(datetime.timezone.utc).timestamp()
        if self.status_history is None:
            self.status_history = {}
        self.status_history.setdefault(value.value, now_ts)
        self.mark_dirty("status_history")
        logging.info("Request %s status set to %s.", self.id, value.value)

    @classmethod
    def serialize_slot(cls, key, value):
        if value is None:
            return None
        if key == "verb":
            return value.value
        if key == "status":
            return value.value
        if key == "user":
            return value.serialize()
        if key == "status_history":
            return value
        return value

    @classmethod
    def deserialize_slot(cls, key, value):
        if value is None:
            return None
        if key == "verb":
            return Verb(value)
        if key == "status":
            return Status(value)
        if key == "user":
            return User(from_dict=value)
        if key == "status_history":
            return value
        return value

    def serialize(self):
        """Serialize the request object to a dictionary with plain data types"""
        result = {}
        for k in self.__slots__:
            v = self.__getattribute__(k)
            result[k] = self.serialize_slot(k, v)
        return result

    def serialize_logging(self):
        """Serialize the request object to a reduced dictionary for logging purposes"""
        result = self.serialize()
        # unnecessary for request logging
        result.pop("user", None)
        # omit user_request if request has been coerced
        if self.coerced_request:
            result.pop("user_request", None)
        return result

    def deserialize(self, dict):
        """Modify the request by deserializing a dictionary into it"""
        for k, v in dict.items():
            self.__setattr__(k, self.deserialize_slot(k, v))

    def __eq__(self, other):
        if isinstance(other, PolytopeRequest):
            return other.id == self.id
        return False

    def __hash__(self):
        return uuid.UUID(self.id).int
