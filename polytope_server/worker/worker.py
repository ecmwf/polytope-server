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
import os
import signal
import sys
import time
import traceback
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor

import requests

from ..common import collection, metric_store
from ..common import queue as polytope_queue
from ..common import request_store, staging
from ..common.metric import WorkerInfo, WorkerStatusChange
from ..common.request import Status


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

    def run(self):
        """
        Entrypoint for the worker.
        This method will run until the worker is terminated.
        """

        self.thread_pool = ThreadPoolExecutor(1)

        try:
            self.queue = polytope_queue.create_queue(self.config.get("queue"))

            self.update_status("idle", time_spent=0)
            # self.update_metric()

            while not time.sleep(self.poll_interval):
                self.queue.keep_alive()

                # No active request: try to pop from queue and process request in future thread
                if self.future is None:
                    self.queue_msg = self.queue.dequeue()
                    if self.queue_msg is not None:
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

                        # Occurs if a request crashed a worker and the message gets requeued (status will be PROCESSING)
                        # We do not want to try this request again
                        elif self.request.status != Status.QUEUED:
                            logging.info(
                                "Request has unexpected status {}, setting to failed".format(self.request.status),
                                extra={"request_id": id},
                            )
                            self.request.set_status(Status.FAILED)
                            self.request.user_message += (
                                "Request was not processed due to an unexpected worker crash. Please contact support."
                            )
                            self.request_store.update_request(self.request)
                            self.update_status("idle")
                            self.queue.ack(self.queue_msg)

                        # OK, process the request
                        else:
                            logging.info(
                                "Popped request from the queue, beginning worker thread.",
                                extra={"request_id": id},
                            )
                            self.request.set_status(Status.PROCESSING)
                            self.update_status("processing", request_id=self.request.id)
                            self.request_store.update_request(self.request)
                            self.future = self.thread_pool.submit(self.process_request, (self.request))
                    else:
                        self.update_status("idle")

                # Future completed: do callback, ack message and reset state
                elif self.future.done():
                    try:
                        self.future.result(0)
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

                # Future running: keep checking
                else:
                    self.update_status("processing")

                # self.update_metric()
        except Exception:
            # We must force threads to shutdown in case of failure, otherwise the worker won't exit
            self.thread_pool.shutdown(wait=False)
            raise

    def process_request(self, request) -> bool:
        """
        Dispatch the request to each datasource in the request collection until one matches and
            succesfully submits a request. Then get the result and upload to staging.

        Returns True if successful, False otherwise.
        """

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
        datasource_matches = OrderedDict()
        for ds in collection.datasources():
            logging.debug(
                "Attempting to process request using datasource {}".format(ds.get_type()),
                extra={"request_id": id},
            )
            try:
                datasource_matches[ds.repr()] = ds.dispatch(request, input_data)
            except Exception as e:
                logging.exception(
                    "Failed to process request using datasource {}, {}".format(ds.get_type(), repr(e)),
                    extra={"request_id": id},
                    stack_info=True,
                )
                datasource_matches[ds.repr()] = f"Datasource {ds.repr()} failed to process request.\n {repr(e)}\n"
                continue
            if datasource_matches[ds.repr()][0]:
                logging.info(f"Using datasource {ds.get_type()} to process request", extra={"request_id": id})
                datasource = ds
                break
            else:
                logging.debug(
                    "Datasource {} did not match request, continuing to next datasource".format(ds.get_type()),
                    extra={"request_id": id},
                )

        # If no datasource is found, or if the datasource is not able to process the request, set the request to failed
        if not any([v[0] for k, v in datasource_matches.items()]):  # no datasource matched
            logging.info("No datasource matched request, setting to failed", extra={"request_id": id})
            request.user_message += "Failed to process request, no matching datasource found.\n"
            for k, v in datasource_matches.items():
                request.user_message += v[2]
            request.set_status(Status.FAILED)
            self.request_store.update_request(request)
            return False
        elif not any([v[1] for k, v in datasource_matches.items()]):  # no datasource submitted request successfully
            logging.info("Datasource matched but request was not successful", extra={"request_id": id})
            request.user_message += "Request was matched but request was not successful.\n"
            # add all matched datasource messages
            for k, v in datasource_matches.items():
                if v[0]:
                    request.user_message += v[2]
            request.set_status(Status.FAILED)
            self.request_store.update_request(request)
            return False

        # Clean up
        try:
            # delete input data if it exists in staging (input data can come from external URLs too)
            if input_data is not None:
                if self.staging.query(id):
                    self.staging.delete(id)

            # upload result data
            if datasource is not None and datasource_matches[datasource.repr()][1]:
                logging.info("Uploading result data to staging", extra={"request_id": id})
                request.url = self.staging.create(id, datasource.result(request), datasource.mime_type())

        except Exception as e:
            logging.exception("Failed to finalize request", extra={"request_id": id, "exception": str(e)})
            raise

        # Guarantee destruction of the datasource resources
        finally:
            if datasource is not None:
                datasource.destroy(request)

        # Set request to processed
        request.set_status(Status.PROCESSED)
        request.user_message += "Request was processed successfully"
        self.request_store.update_request(request)
        logging.info("Request processed successfully", extra={"request_id": id})
        return True

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

        # request.user_message = "Success"  # Do not report log history on successful request
        # logging.info("Request completed successfully.", extra={"request_id": request.id})
        # request.set_status(Status.PROCESSED)
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
