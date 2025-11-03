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
import concurrent
import functools
import logging
import os
import signal
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import NoReturn

import requests
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider

from ..common import collection
from ..common import queue as polytope_queue
from ..common import request_store, staging
from ..common.logging import with_baggage_items
from ..common.request import PolytopeRequest, Status

trace.set_tracer_provider(TracerProvider(resource=Resource.create({"service.name": "worker"})))

tracer = trace.get_tracer(__name__)


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

    def __init__(self, config: dict):
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

        self.collections = collection.create_collections(self.config.get("collections"))
        self.staging = staging.create_staging(self.config.get("staging"))
        self.request_store = request_store.create_request_store(
            self.config.get("request_store"), self.config.get("metric_store")
        )

        self.queue_msg = None
        self.request = None
        self.queue = None

    def update_status(self, new_status: str, time_spent: float = None, request_id: str = None) -> None:
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
            extra={
                "worker": {
                    "status": self.status,
                    "request_id": self.processing_id,
                    "requests_processed": self.requests_processed,
                    "requests_failed": self.requests_failed,
                    "total_idle_time": self.total_idle_time,
                    "total_processing_time": self.total_processing_time,
                }
            },
        )

    async def keep_alive(self) -> NoReturn:
        if self.queue is None:
            raise RuntimeError("queue was not initialised")

        while True:
            if self.request is not None:
                self.queue.keep_alive()

            await aio.sleep(self.poll_interval)

    async def listen_queue(self, executor: concurrent.futures.Executor) -> None:
        if self.queue is None:
            raise RuntimeError("queue was not initialised")

        loop = aio.get_running_loop()

        while self.status != "draining":
            self.queue_msg = self.queue.dequeue()
            if self.queue_msg is None:
                # Only sleep if system is idling
                await aio.sleep(self.poll_interval)
                continue

            id = self.queue_msg.body["id"]
            with with_baggage_items({"request_id": id}):
                self.request = self.request_store.get_request(id)

                # This occurs when a request has been revoked while it was on the queue
                if self.request is None:
                    logging.info("Request no longer exists, ignoring")
                    self.queue.ack(self.queue_msg)
                    self.update_status("idle")
                    continue

                # Occurs if a request crashed a worker and the message gets requeued (status will be PROCESSING)
                # We do not want to try this request again
                if self.request.status != Status.QUEUED:
                    logging.info(
                        "Request has unexpected status %s, setting to failed",
                        self.request.status,
                    )
                    msg = "Request was not processed due to an unexpected worker crash. Please contact support."
                    self.request.user_message += msg
                    self.request_store.set_request_status(self.request, Status.FAILED)
                    self.queue.ack(self.queue_msg)
                    self.update_status("idle")
                    continue

                self.request_store.set_request_status(self.request, Status.PROCESSING)
                self.update_status("processing", request_id=self.request.id)
                try:
                    await loop.run_in_executor(executor, self.process_request, self.request)
                except Exception as e:
                    self.on_request_fail(e)
                else:
                    self.on_request_complete()

                self.queue.ack(self.queue_msg)

                self.update_status("idle")
                await self.terminate()

                self.queue_msg = None
                self.request = None

    async def terminate(self) -> NoReturn:
        if timeout := self.config.get("timeout"):
            self.update_status("draining")
            await aio.sleep(timeout)
        raise TaskGroupTermination()

    async def schedule(self, executor: concurrent.futures.Executor) -> NoReturn:

        def handle_termination(group: aio.TaskGroup) -> None:
            logging.info("Termination signal received, exiting...")
            group.create_task(self.terminate())

        loop = aio.get_running_loop()

        try:
            async with aio.TaskGroup() as group:
                group.create_task(self.keep_alive())
                group.create_task(self.listen_queue(executor))
                cbk = functools.partial(handle_termination, group)
                loop.add_signal_handler(signal.SIGINT, cbk)
                loop.add_signal_handler(signal.SIGTERM, cbk)

        except* TaskGroupTermination:
            # We must force threads to shutdown in case of failure, otherwise the worker won't exit
            executor.shutdown(wait=False)
            self.on_process_terminated()
        finally:
            loop.remove_signal_handler(signal.SIGINT)
            loop.remove_signal_handler(signal.SIGTERM)

    def run(self):
        self.queue = polytope_queue.create_queue(self.config.get("queue"))

        self.update_status("idle", time_spent=0)

        with ThreadPoolExecutor(max_workers=1) as executor:
            aio.run(self.schedule(executor))

    def process_request(self, request: PolytopeRequest) -> None:
        """Entrypoint for the worker thread."""

        id = request.id
        collection = self.collections[request.collection]

        logging.info(
            "Processing request on collection {}".format(collection.name),
            extra={"collection": collection.name, "request": request.serialize()},
        )

        input_data = self.fetch_input_data(request.url)

        # Dispatch to collection
        datasource = collection.dispatch(request, input_data)
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
            logging.exception("Failed to finalize request", extra={"exception": repr(e)})
            raise

        # Guarantee destruction of the datasource
        finally:
            if datasource is not None:
                datasource.destroy(request)

        if datasource is None:
            # request.user_message += "Failed to process request."
            logging.info(request.user_message)
            raise Exception("Request was not accepted by any datasources.")
        else:
            request.user_message += "Success"

        return

    def fetch_input_data(self, url: str) -> bytes | None:
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

    def on_request_complete(self) -> None:
        """Called when the request processing exits cleanly"""

        self.request.user_message = "Success"  # Do not report log history on successful request
        logging.info("Request completed successfully.")
        self.request_store.set_request_status(self.request, Status.PROCESSED)
        self.requests_processed += 1

    def on_request_fail(self, exception: Exception) -> None:
        """Called when the request processing raises an exception"""

        logging.exception("Request failed with exception.")
        error_message = self.request.user_message + "\n" + str(exception)
        self.request.user_message = error_message
        self.request_store.set_request_status(self.request, Status.FAILED)
        self.requests_failed += 1

    def on_process_terminated(self) -> None:
        """Called when the worker is asked to exit whilst processing a request, and we want to reschedule the request"""

        if self.request is not None:
            with with_baggage_items({"request_id": self.request.id}):
                logging.info("Rescheduling request due to worker shutdown.")
                error_message = self.request.user_message + "\n" + "Worker shutdown, rescheduling request."
                self.request.user_message = error_message
                if self.request.status == Status.PROCESSING:
                    self.request_store.set_request_status(self.request, Status.QUEUED)
                    self.queue.nack(self.queue_msg)
                else:
                    self.request_store.update_request(self.request)
