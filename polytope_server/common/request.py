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


class Request:

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
        "content_length",
        "content_type",
    ]

    def __init__(self, from_dict=None, **kwargs):

        self.id = str(uuid.uuid4())
        self.timestamp = datetime.datetime.utcnow().timestamp()
        self.last_modified = datetime.datetime.utcnow().timestamp()
        self.user = None
        self.verb = Verb.RETRIEVE
        self.url = ""
        self.collection = ""
        self.status = Status.WAITING
        self.md5 = None
        self.user_message = ""
        self.user_request = ""
        self.content_length = None
        self.content_type = "application/octet-stream"

        if from_dict:
            self.deserialize(from_dict)

        for k, v in kwargs.items():
            self.__setattr__(k, v)

    def set_status(self, value):
        self.status = value
        logging.info("Request ID {} status set to {}.".format(self.id, value.value))

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
        return value

    def serialize(self):
        """Serialize the request object to a dictionary with plain data types"""
        result = {}
        for k in self.__slots__:
            v = self.__getattribute__(k)
            result[k] = self.serialize_slot(k, v)
        return result

    def deserialize(self, dict):
        """Modify the request by deserializing a dictionary into it"""
        for k, v in dict.items():
            self.__setattr__(k, self.deserialize_slot(k, v))

    def __eq__(self, other):
        if isinstance(other, Request):
            return other.id == self.id
        return False

    def __hash__(self):
        return uuid.UUID(self.id).int
