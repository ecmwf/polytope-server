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
import logging

import pymongo

from .. import metric_store, mongo_client_factory
from ..exceptions import ForbiddenRequest, NotFound, UnauthorizedRequest
from ..metric import MetricType, RequestStatusChange
from ..metric_collector import MongoRequestStoreMetricCollector
from ..request import PolytopeRequest, Status
from . import request_store


class MongoRequestStore(request_store.RequestStore):
    def __init__(self, config=None, metric_store_config=None):
        uri = config.get("uri", "mongodb://localhost:27017")
        request_collection = config.get("collection", "requests")
        log_level = config.get("log_level", logging.WARNING)
        logging.getLogger("pymongo").setLevel(log_level)
        username = config.get("username")
        password = config.get("password")

        self.mongo_client = mongo_client_factory.create_client(uri, username, password)
        self.database = self.mongo_client.request_store
        self.store = self.database[request_collection]

        self.metric_store = None
        if metric_store_config:
            self.metric_store = metric_store.create_metric_store(metric_store_config)

        self.request_store_metric_collector = MongoRequestStoreMetricCollector()

        logging.debug("MongoClient configured to open at {}".format(uri))

    def get_type(self):
        return "mongodb"

    def add_request(self, request):
        if self.get_request(request.id) is not None:
            raise ValueError("Request already exists in request store")
        self.store.insert_one(request.serialize())

        if self.metric_store and request.status == Status.PROCESSED:
            self.metric_store.add_metric(
                RequestStatusChange(request_id=request.id, status=request.status, user_id=request.user.id)
            )

        logging.info("Request ID {} status set to {}.".format(request.id, request.status))

    def remove_request(self, id):
        result = self.store.find_one_and_delete({"id": id})
        if result is None:
            raise KeyError("Request does not exist in request store")
        if self.metric_store:
            res = self.metric_store.get_metrics(type=MetricType.REQUEST_STATUS_CHANGE, request_id=id)
            for i in res:
                self.metric_store.remove_metric(i.uuid)
        logging.info("Request ID %s removed.", id)

    def revoke_request(self, user, id):
        if id == "all":
            # Revoke all requests of the user that are waiting or queued
            result = self.store.delete_many(
                {"status": {"$in": [Status.WAITING.value, Status.QUEUED.value]}, "user.id": user.id}
            )
            return result.deleted_count

        result = self.store.find_one_and_delete(
            {"id": id, "status": {"$in": [Status.WAITING.value, Status.QUEUED.value]}, "user.id": user.id}
        )
        if result is None:
            # Check if the request exists to distinguish error cause
            request = self.get_request(id)
            if request is None:
                raise NotFound("Request does not exist in request store")
            elif request.user != user:
                raise UnauthorizedRequest("Request belongs to a different user")
            elif request.status not in [Status.WAITING, Status.QUEUED]:
                raise ForbiddenRequest("Request has started processing and can no longer be revoked.", None)
            else:
                raise
        logging.info("Request ID %s revoked.", id)
        return 1  # Successfully revoked one request

    def get_request(self, id):
        result = self.store.find_one({"id": id}, {"_id": False})
        if result:
            request = PolytopeRequest(from_dict=result)
            return request
        else:
            return None

    def get_requests(self, ascending=None, descending=None, limit=None, **kwargs):
        if ascending:
            if ascending not in PolytopeRequest.__slots__:
                raise KeyError("Request has no key {}".format(ascending))

        if descending:
            if descending not in PolytopeRequest.__slots__:
                raise KeyError("Request has no key {}".format(descending))

        query = {}
        for k, v in kwargs.items():
            if k not in PolytopeRequest.__slots__:
                raise KeyError("Request has no key {}".format(k))

            if v is None:
                continue

            # Querying of mongodb subdocuments behaves unintuitively.
            # Prefer to use an objects custom 'id' attribute if it exists.
            # https://www.oreilly.com/library/view/mongodb-the-definitive/9781449344795/ch04.html

            sub_doc_id = getattr(v, "id", None)
            if sub_doc_id is not None:
                query[k + ".id"] = sub_doc_id
                continue

            query[k] = PolytopeRequest.serialize_slot(k, v)

        cursor = self.store.find(query, {"_id": False})

        if ascending is not None and descending is not None:
            raise ValueError("Cannot sort by ascending and descending at the same time.")
        if ascending is not None:
            cursor.sort(ascending, pymongo.ASCENDING)
        elif descending is not None:
            cursor.sort(descending, pymongo.DESCENDING)
        if limit is not None:
            cursor.limit(limit)

        cursor_list = list(cursor)
        if cursor_list:
            res = []
            for i in cursor_list:
                request = PolytopeRequest(from_dict=i)
                res.append(request)
            return res
        return []

    def update_request(self, request):
        request.last_modified = datetime.datetime.now(datetime.timezone.utc).timestamp()
        res = self.store.find_one_and_update(
            {"id": request.id},
            {"$set": request.serialize()},
            return_document=pymongo.ReturnDocument.AFTER,
        )

        if res is None:
            raise NotFound("Request {} not found in request store".format(request.id))

        if self.metric_store and request.status == Status.PROCESSED:
            self.metric_store.add_metric(
                RequestStatusChange(request_id=request.id, status=request.status, user_id=request.user.id)
            )

        logging.info(
            "Request ID {} updated on request store. Status set to {}.".format(request.id, request.status),
            extra={"request": request},
        )

        return res

    def wipe(self):
        if self.metric_store:
            res = self.get_requests()
            for i in res:
                self.metric_store.remove_metric(
                    type=MetricType.REQUEST_STATUS_CHANGE, request_id=i.id, include_processed=True
                )

        self.database.drop_collection(self.store.name)

    def collect_metric_info(self):
        metric = self.request_store_metric_collector.collect().serialize()
        return metric

    def remove_old_requests(self, cutoff):
        cutoff = cutoff.timestamp()
        result = self.store.delete_many(
            {"status": {"$in": [Status.FAILED.value, Status.PROCESSED.value]}, "last_modified": {"$lt": cutoff}}
        )
        return result.deleted_count
