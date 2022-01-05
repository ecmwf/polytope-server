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
import json
import logging

from flask import Flask, request

from ..common.exceptions import HTTPException, NotFound, ServerError
from ..common.metric import MetricType
from ..common.request import Status
from ..frontend.common.application_server import GunicornServer
from ..frontend.common.flask_decorators import RequestSucceeded
from . import telemetry


class FlaskHandler(telemetry.TelemetryHandler):
    def create_handler(
        self,
        request_store,
        keygenerator,
        staging,
        identity,
        metric_store,
        queue,
        auth,
        cache,
    ):

        handler = Flask(__name__)

        def service_status():
            response_message = {
                "request_store": request_store.collect_metric_info(),
                "keygenerator": keygenerator.collect_metric_info(),
                "staging": staging.collect_metric_info(),
                "identity": identity.collect_metric_info(),
                "metric_store": metric_store.collect_metric_info(),
                "queue": queue.collect_metric_info(),
                "auth": auth.collect_metric_info(),
                "cache": cache.collect_metric_info(),
            }
            return response_message

        def all_requests(status=None, id=None):

            if status and not (status in (["active"] + [e.value for e in Status])):
                raise NotFound("Status '%s' not recognized" % status)

            if status == "active":
                statuses = [
                    Status.WAITING,
                    Status.UPLOADING,
                    Status.QUEUED,
                    Status.PROCESSING,
                ]
            elif status:
                statuses = [Status(status)]
            else:
                statuses = [None]

            user_requests = []
            for status in statuses:
                query = {"status": status, "id": id}
                user_requests += request_store.get_requests(**query)

            response_message = []
            for i in user_requests:
                res = i.serialize()
                if id is not None:
                    metrics = metric_store.get_metrics(type=MetricType.REQUEST_STATUS_CHANGE, request_id=id)
                    res["trace"] = [metric.serialize() for metric in metrics]
                res["user"]["details"] = "**hidden**"
                response_message.append(res)

            return response_message

        def active_workers(uuid=None, host=None):

            if not metric_store:
                raise ServerError("Cannot provide result as no metric store is available.")

            query = {"uuid": uuid, "host": host, "type": MetricType.WORKER_INFO}

            worker_statuses = metric_store.get_metrics(**query)
            response_message = []
            for i in worker_statuses:
                res = i.serialize(ndigits=2)
                res["timestamp_served"] = datetime.datetime.utcnow().timestamp()
                response_message.append(res)

            return response_message

        @handler.errorhandler(Exception)
        def default_error_handler(error):
            logging.exception(str(error))
            return (
                json.dumps({"message": str(error)}),
                getattr(error, "code", 500),
                {"Content-Type": "application/json"},
            )

        @handler.errorhandler(HTTPException)
        def handle_error(error):
            logging.exception(str(error))
            return (
                json.dumps({"message": str(error.description)}),
                error.code,
                {"Content-Type": "application/json"},
            )

        @handler.route("/telemetry/v1", methods=["GET"])
        def listEndpoints():
            if request.method == "GET":
                return RequestSucceeded(["test", "summary", "all", "requests", "workers"])

        @handler.route("/telemetry/v1/test", methods=["GET"])
        def test():
            if request.method == "GET":
                return RequestSucceeded("Polytope telemetry server is alive")

        @handler.route("/telemetry/v1/summary", methods=["GET"])
        def serviceStatus():
            if request.method == "GET":

                return RequestSucceeded(service_status())

        @handler.route("/telemetry/v1/requests", methods=["GET"], defaults={"status": None})
        @handler.route("/telemetry/v1/requests/<status>", methods=["GET"])
        def allRequests(status):
            if request.method == "GET":

                id = request.args.get("id")

                return RequestSucceeded(all_requests(status, id))

        @handler.route("/telemetry/v1/workers", methods=["GET"])
        def activeWorkers():
            if request.method == "GET":

                uuid = request.args.get("uuid")
                host = request.args.get("host")

                return RequestSucceeded(active_workers(uuid, host))

        @handler.route("/telemetry/v1/all", methods=["GET"])
        def allMetrics():
            if request.method == "GET":

                response_message = []
                response_message += service_status()
                response_message += all_requests()
                response_message += active_workers()

                return RequestSucceeded(response_message)

        return handler

    def run_server(self, handler, server_type, host, port):

        if server_type == "flask":
            # flask internal server for non-production environments
            # should only be used for testing and debugging
            handler.run(host=host, port=port, debug=True)
        elif server_type == "gunicorn":
            options = {"bind": "%s:%s" % (host, port), "workers": 1}
            GunicornServer(handler, options).run()
        else:
            logging.error("server_type %s not supported" % server_type)
            raise NotImplementedError
