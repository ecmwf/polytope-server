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

import logging
import time
from typing import Iterable

from ..common import collection, queue, request_store
from ..common.logging import with_baggage_items
from ..common.request import PolytopeRequest, Status


class Broker:
    def __init__(self, config: dict):

        queue_config = config.get("queue")
        self.queue = queue.create_queue(queue_config)

        self.max_queue_size = config.get("deployment", {}).get("worker", {}).get("replicas", 40)

        self.broker_config = config.get("broker", {})
        self.scheduling_interval = self.broker_config.get("interval", 10)

        self.request_store = request_store.create_request_store(config.get("request_store"), config.get("metric_store"))

        self.collections = collection.create_collections(config.get("collections"))

    def run(self):

        logging.info("Starting broker...")
        logging.info("Maximum Queue Size: {}".format(self.max_queue_size))

        while not time.sleep(self.scheduling_interval):
            self.check_requests()

    def check_requests(self):

        self.queue.keep_alive()

        # Don't queue if full. We don't need to query request_store.
        if self.queue.count() >= self.max_queue_size:
            logging.info("Queue is full")
            return

        # Find all requests that are waiting to be queued (oldest first)
        waiting_requests = self.request_store.get_requests(ascending="timestamp", status=Status.WAITING)
        logging.debug("Found {} waiting requests".format(len(waiting_requests)))

        if len(waiting_requests) == 0:
            return

        # Find all requests which have already been queued
        active_requests = self.request_store.get_active_requests()

        # if the queue is empty, then the "active" requests are stuck and should be put back to waiting
        requeued_requests = []
        if self.queue.count() == 0:
            for ar in active_requests:
                logging.info(
                    f"Request {ar.id} appears stuck in {ar.status}, setting back to WAITING",
                    extra={"request_id": ar.id},
                )
                self.request_store.set_request_status(ar, Status.WAITING)
                requeued_requests.append(ar)
            active_requests = []
        requeued_requests.sort(key=lambda r: r.timestamp)
        waiting_requests = requeued_requests + waiting_requests

        if len(active_requests) > self.max_queue_size:
            logging.warning(
                f"Number of active requests ({len(active_requests)}) exceeds max queue size ({self.max_queue_size}). "
                + "This suggests some requests may be stuck."
            )

        # Loop through requests queuing anything that meets QoS requirements
        for wr in waiting_requests:  # should break if queue full

            if self.check_limits(active_requests, wr):
                assert wr.status == Status.WAITING
                active_requests.append(wr)
                self.enqueue(wr)

            if self.queue.count() >= self.max_queue_size:
                logging.info("Queue is full")
                return

    def check_limits(self, active_requests: Iterable, request: PolytopeRequest):
        with with_baggage_items({"request_id": request.id}):
            logging.debug(f"Checking limits for request {request.id}")

            # Get collection limits and calculate active requests
            collection = self.collections[request.collection]
            collection_limits = collection.limits
            collection_total_limit = collection_limits.get("total")
            collection_active_requests = sum(qr.collection == request.collection for qr in active_requests)
            logging.debug(f"Collection {request.collection} has {collection_active_requests} active requests")

            # Check collection total limit
            if collection_total_limit is not None and collection_active_requests >= collection_total_limit:
                logging.info(
                    f"Collection has {collection_active_requests} of {collection_total_limit} total active requests"
                )
                return False

            # Determine the effective limit based on role or per-user setting
            role_limits = collection_limits.get("per-role", {}).get(request.user.realm, {})
            limit = max((role_limits.get(role, 0) for role in request.user.roles), default=0)
            if limit == 0:  # Use collection per-user limit if no role-specific limit
                limit = collection_limits.get("per-user", 0)

            # Check if user exceeds the effective limit
            if limit > 0:
                user_active_requests = sum(
                    qr.collection == request.collection and qr.user == request.user for qr in active_requests
                )
                user_limit_message = (
                    f"User {request.user} has {user_active_requests} of {limit} "
                    f"active requests in collection {request.collection}"
                )
                logging.info(user_limit_message)
                if user_active_requests >= limit:
                    return False
                else:
                    return True

            # Allow if no limits are exceeded
            logging.debug(f"No limit for user {request.user} in collection {request.collection}")
            return True

    def enqueue(self, request: PolytopeRequest):
        with with_baggage_items({"request_id": request.id}):
            logging.info("Queuing request")

            try:
                # Must update request_store before queue, worker checks request status immediately
                request.set_status(Status.QUEUED)
                self.request_store.update_request(request)
                msg = queue.Message(request.serialize())
                self.queue.enqueue(msg)
            except Exception as e:
                # If we fail to call this, the request will be stuck (POLY-21)
                logging.exception("Failed to queue, error: {}".format(repr(e)))
                self.request_store.set_request_status(request, Status.WAITING)
            else:
                logging.info("Queued request")
