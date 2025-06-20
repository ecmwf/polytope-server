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

import asyncio as aio
import logging
import os
import signal
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor

import requests

from ..common import collection, metric_store
from ..common import queue as polytope_queue
from ..common import request_store, staging
from ..common.metric import WorkerInfo, WorkerStatusChange
from ..common.request import Status


class TaskGroupTermination(Exception):
    pass


class Worker:
    """The worker:
    - Listens for incoming requests on the queue
    - Spawns a thread to process the request
    - Maintains a keep-alive with the queue while processing
    - Acks the message on completion
    - Repeats
    """

    def __init__(self, config, debug=False, is_webmars=False):
        self.config = config
        self.worker_config = config.get("worker", {})
        self.datasource_configs = config.get("datasources", {})
        self.poll_interval = self.worker_config.get("poll_interval", 0.1)
        self.proxies = {
            "http": os.environ.get("POLYTOPE_PROXY", ""),
            "https": os.environ.get("POLYTOPE_PROXY", ""),
        }

        # TODO: use enum for statuses and types
        self.status = "starting"
        self.status_time = 0.0
        self.processing_id = None
        self.requests_processed = 0
        self.requests_failed = 0
        self.total_idle_time = 0.0
        self.total_processing_time = 0.0

        self.metric_store = None
        self.metric = WorkerInfo()
        self.update_metric()

        if self.config.get("metric_store"):
            self.metric_store = metric_store.create_metric_store(self.config.get("metric_store"))
            self.metric_store.add_metric(self.metric)

        self.collections = collection.create_collections(self.config.get("collections"))
        self.staging = staging.create_staging(self.config.get("staging"))
        self.request_store = request_store.create_request_store(
            self.config.get("request_store"), self.config.get("metric_store")
        )

        self.future = None
        self.queue_msg = None
        self.request = None
        self.queue = None

        signal.signal(signal.SIGINT, self.on_process_terminated)
        signal.signal(signal.SIGTERM, self.on_process_terminated)

    def update_status(self, new_status, time_spent=None, request_id=None):
        if time_spent is None:
            time_spent = self.poll_interval

        self.status_time += time_spent

        if self.status == "processing":
            self.total_processing_time += time_spent
        else:
            self.total_idle_time += time_spent

        if self.status == new_status:
            return

        if new_status == "processing":
            self.processing_id = request_id
        else:
            self.processing_id = None

        self.status = new_status
        self.status_time = 0.0

        logging.info(
            "Worker status update",
            extra=WorkerStatusChange(status=self.status).serialize(),
        )
        self.update_metric()

    def update_metric(self):
        self.metric.update(
            status=self.status,
            status_time=self.status_time,
            request_id=self.processing_id,
            requests_processed=self.requests_processed,
            requests_failed=self.requests_failed,
            total_idle_time=self.total_idle_time,
            total_processing_time=self.total_processing_time,
        )
        if self.metric_store:
            self.metric_store.update_metric(self.metric)

    async def keep_alive(self):
        if self.queue is None:
            raise RuntimeError("queue was not initialised")

        while True:
            if self.future is not None:
                self.queue.keep_alive()

            await aio.sleep(self.poll_interval)

    async def listen_queue(self, executor):
        if self.queue is None:
            raise RuntimeError("queue was not initialised")

        loop = aio.get_running_loop()

        while True:
            self.queue_msg = self.queue.dequeue()
            if self.queue_msg is None:
                await aio.sleep(self.poll_interval)
                continue

            id = self.queue_msg.body["id"]
            self.request = self.request_store.get_request(id)

            # This occurs when a request has been revoked while it was on the queue
            if self.request is None:
                logging.info(
                    "Request no longer exists, ignoring",
                    extra={"request_id": id},
                )
                self.update_status("idle")
                self.queue.ack(self.queue_msg)
                continue

            # Occurs if a request crashed a worker and the message gets requeued (status will be PROCESSING)
            # We do not want to try this request again
            if self.request.status != Status.QUEUED:
                logging.info(
                    "Request has unexpected status %s, setting to failed",
                    self.request.status,
                    extra={"request_id": id},
                )
                self.request.set_status(Status.FAILED)
                msg = "Request was not processed due to an unexpected worker crash. Please contact support."
                self.request.user_message += msg
                self.request_store.update_request(self.request)
                self.update_status("idle")
                self.queue.ack(self.queue_msg)
                continue

            logging.info(
                "Popped request from the queue, beginning worker thread.",
                extra={"request_id": id},
            )
            self.request.set_status(Status.PROCESSING)
            self.update_status("processing", request_id=self.request.id)
            self.request_store.update_request(self.request)
            try:
                await loop.run_in_executor(executor, self.process_request, self.request)
            except Exception as e:
                self.on_request_fail(self.request, e)
            else:
                self.on_request_complete(self.request)

            self.queue.ack(self.queue_msg)

            self.update_status("idle")
            self.request_store.update_request(self.request)
            sys.exit(0)

            self.future = None
            self.queue_msg = None
            self.request = None

    async def handle_termination(self):
        def terminate():
            raise TaskGroupTermination
        loop = aio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, terminate)
        loop.add_signal_handler(signal.SIGTERM, terminate)

    async def schedule(self, executor):
        try:
            async with aio.TaskGroup() as group:
                group.create_task(self.keep_alive())
                group.create_task(self.listen_queue(executor))
                group.create_task(self.handle_termination())
        except* TaskGroupTermination:
            # We must force threads to shutdown in case of failure, otherwise the worker won't exit
            executor.shutdown(wait=False)
            self.on_process_terminated()

    def run(self):
        self.queue = polytope_queue.create_queue(self.config.get("queue"))

        self.update_status("idle", time_spent=0)

        with ThreadPoolExecutor(max_workers=1) as executor:
            aio.run(self.schedule(executor))

    def process_request(self, request):
        """Entrypoint for the worker thread."""

        id = request.id
        collection = self.collections[request.collection]

        logging.info(
            "Processing request on collection {}".format(collection.name),
            extra={"request_id": id},
        )
        logging.info("Request is: {}".format(request.serialize()))

        input_data = self.fetch_input_data(request.url)

        # Dispatch to listed datasources for this collection until we find one that handles the request
        datasource = None
        for ds in collection.datasources():
            logging.info(
                "Processing request using datasource {}".format(ds.get_type()),
                extra={"request_id": id},
            )
            if ds.dispatch(request, input_data):
                datasource = ds
                request.user_message += "Datasource {} accepted request.\n".format(ds.repr())
                break

        # Clean up
        try:
            # delete input data if it exists in staging (input data can come from external URLs too)
            if input_data is not None:
                if self.staging.query(id):
                    self.staging.delete(id)

            # upload result data
            if datasource is not None:
                request.url = self.staging.create(id, datasource.result(request), datasource.mime_type())

        except Exception as e:
            logging.exception("Failed to finalize request", extra={"request_id": id, "exception": str(e)})
            raise

        # Guarantee destruction of the datasource
        finally:
            if datasource is not None:
                datasource.destroy(request)

        if datasource is None:
            # request.user_message += "Failed to process request."
            logging.info(request.user_message, extra={"request_id": id})
            raise Exception("Request was not accepted by any datasources.")
        else:
            request.user_message += "Success"

        return

    def fetch_input_data(self, url):
        """Downloads input data from external URL or staging"""
        if url != "":
            try:
                response = requests.get(url, proxies=self.proxies)
                response.raise_for_status()
            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.HTTPError,
            ):
                logging.info("Retrying requests.get without proxies after failure")
                response = requests.get(url)
                response.raise_for_status()

            if response.status_code == 200:
                logging.info("Downloaded data of size {} from {}".format(sys.getsizeof(response._content), url))
                return response._content
            else:
                raise Exception(
                    "Could not download data from {}, got {} : {}".format(url, response.status_code, response._content)
                )
        return None

    def on_request_complete(self, request):
        """Called when the future exits cleanly"""

        request.user_message = "Success"  # Do not report log history on successful request
        logging.info("Request completed successfully.", extra={"request_id": request.id})
        request.set_status(Status.PROCESSED)
        self.requests_processed += 1

    def on_request_fail(self, request, exception):
        """Called when the future thread raises an exception"""

        _, v, _ = sys.exc_info()
        tb = traceback.format_exception(None, exception, exception.__traceback__)
        logging.info(tb, extra={"request_id": request.id})
        error_message = request.user_message + "\n" + str(v)
        request.set_status(Status.FAILED)
        request.user_message = error_message
        logging.exception("Request failed with exception.", extra={"request_id": request.id})
        self.requests_failed += 1

    def on_process_terminated(self, signumm=None, frame=None):
        """Called when the worker is asked to exit whilst processing a request, and we want to reschedule the request"""

        if self.request is not None:
            logging.info(
                "Request being rescheduled due to worker shutdown.",
                extra={"request_id": self.request.id},
            )
            error_message = self.request.user_message + "\n" + "Worker shutdown, rescheduling request."
            self.request.user_message = error_message
            self.request.set_status(Status.QUEUED)
            self.request_store.update_request(self.request)
            self.queue.nack(self.queue_msg)

        exit(0)
