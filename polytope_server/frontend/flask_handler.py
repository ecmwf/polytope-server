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

import json
import logging
import pathlib
import tempfile
from typing import Dict

import flask
import yaml
from flask import Flask, Request, g, request
from flask_swagger_ui import get_swaggerui_blueprint
from opentelemetry import baggage
from opentelemetry.context import attach, detach
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from werkzeug.exceptions import default_exceptions
from werkzeug.middleware.proxy_fix import ProxyFix

from ..common.auth import AuthHelper
from ..common.collection import Collection
from ..common.exceptions import BadRequest, ForbiddenRequest, HTTPException, NotFound
from ..common.logging import with_baggage_items
from ..common.request_store import RequestStore
from ..common.staging import Staging
from ..version import __version__
from . import frontend
from .common.application_server import GunicornServer
from .common.data_transfer import DataTransfer
from .common.flask_decorators import RequestSucceeded

instrumentor = FlaskInstrumentor()


class FlaskHandler(frontend.FrontendHandler):
    def create_handler(
        self,
        request_store: RequestStore,
        auth: AuthHelper,
        staging: Staging,
        collections: Dict[str, Collection],
        proxy_support: bool,
    ):
        handler = Flask(__name__)

        instrumentor.instrument_app(handler, excluded_urls="/api/v1/test")

        if proxy_support:
            handler.wsgi_app = ProxyFix(handler.wsgi_app, x_for=1, x_proto=1, x_host=1)

        openapi_spec = "static/openapi.yaml"
        spec_path = pathlib.Path(__file__).parent.absolute() / openapi_spec
        with spec_path.open("r+", encoding="utf8") as f:
            spec = yaml.safe_load(f)
        spec["info"]["version"] = __version__
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            with open(tmp.name, "w") as f:
                yaml.dump(spec, f)
        SWAGGER_URL = "/openapi"
        SWAGGERUI_BLUEPRINT = get_swaggerui_blueprint(
            SWAGGER_URL, tmp.name, config={"app_name": "Polytope", "spec": spec}
        )
        handler.register_blueprint(SWAGGERUI_BLUEPRINT, name="openapi", url_prefix=SWAGGER_URL)
        handler.register_blueprint(SWAGGERUI_BLUEPRINT, name="home", url_prefix="/")

        data_transfer = DataTransfer(request_store, staging)

        @handler.errorhandler(Exception)
        def default_error_handler(error):
            logging.exception("Unexpected error: %s %s", error, str(error))
            return (
                json.dumps({"message": str(error)}),
                500,
                {"Content-Type": "application/json"},
            )

        @handler.errorhandler(HTTPException)
        def handle_error(error):
            logging.exception("HTTP error: %s %s", error, error.description)
            return (
                json.dumps({"message": str(error.description)}),
                error.code,
                {"Content-Type": "application/json"},
            )

        for code, ex in default_exceptions.items():
            handler.errorhandler(code)(handle_error)

        def get_auth_header(request):
            return request.headers.get("Authorization", "")

        @handler.route("/api/v1/test", methods=["GET"])
        def test():
            if request.method == "GET":
                return RequestSucceeded("Polytope server is alive")

        @handler.route(
            "/api/v1/user",
            methods=[
                "GET",
            ],
        )
        def requestLimits():
            user = auth.authenticate(get_auth_header(request))
            with with_baggage_items({"user.username": user.username}) as _:
                n_user_requests = len(request_store.get_requests(user=user))
                return RequestSucceeded({"live requests": "%s" % n_user_requests})

        @handler.route(
            "/api/v1/requests",
            methods=[
                "GET",
            ],
        )
        def allRequests():
            user = auth.authenticate(get_auth_header(request))
            with with_baggage_items({"user.username": user.username}) as _:
                user_requests = request_store.get_requests(user=user)
                response_message = []
                for i in user_requests:
                    response_message.append(i.serialize())
                return RequestSucceeded(response_message)

        # corresponds to:
        # @handler.route("/api/v1/requests/<collection>", methods = ['POST'])
        # see: @handler.route("/api/v1/requests/<collection_or_request_id>", methods = ['GET','POST','DELETE'])
        def handle_requests(request: Request, collection: str):
            user = auth.authenticate(get_auth_header(request))
            with with_baggage_items({"user.username": user.username}) as _:
                if request.method == "POST":
                    if not user.has_access(collections[collection].roles):
                        raise ForbiddenRequest("User %s cannot access collection %s" % (user.username, collection))

                    if "verb" not in request.json:
                        raise BadRequest("HTTP request content is missing 'verb' (e.g. retrieve)")

                    if "request" not in request.json:
                        raise BadRequest("HTTP request content is missing 'request'")

                    verb = request.json["verb"]
                    if verb == "retrieve":
                        return data_transfer.request_download(request, user, collection)
                    elif verb == "archive":
                        return data_transfer.request_upload(request, user, collection)
                    else:
                        raise BadRequest("Transfer type %s not supported" % verb)
                elif request.method == "GET":
                    user_requests = request_store.get_requests(user=user, collection=collection)
                    response_message = []
                    for i in user_requests:
                        response_message.append(i.serialize())
                    return RequestSucceeded(response_message)
                else:
                    raise BadRequest("Collections do not support %s" % request.method)

        # corresponds to:
        # @handler.route("/api/v1/requests/<request_id>", methods = ['GET','DELETE'])
        # see: @handler.route("/api/v1/requests/<collection_or_request_id>", methods = ['GET','POST','DELETE'])
        def handle_specific_request(request: Request, request_id: str):
            user = auth.authenticate(get_auth_header(request))
            with with_baggage_items({"user.username": user.username, "request_id": request_id}) as _:
                if request.method == "GET":
                    return data_transfer.query_request(user, request_id)
                elif request.method == "POST":
                    raise NotFound("Unsupported collection type: %s" % request_id)
                elif request.method == "DELETE":
                    return data_transfer.revoke_request(user, request_id)

        @handler.route(
            "/api/v1/requests/<collection_or_request_id>",
            methods=["GET", "POST", "DELETE"],
        )
        def collectionRequests(collection_or_request_id):
            if collection_or_request_id in collections:
                return handle_requests(request, collection_or_request_id)
            else:
                return handle_specific_request(request, collection_or_request_id)

        @handler.route("/api/v1/downloads/<path:request_id>", methods=["GET", "HEAD"])
        def downloads(request_id):
            with with_baggage_items({"request_id": request_id}) as _:
                if request.method == "GET":
                    return handle_specific_request(request, request_id)

        @handler.route("/api/v1/uploads/<request_id>", methods=["GET", "POST"])
        def uploads(request_id):
            user = auth.authenticate(get_auth_header(request))
            with with_baggage_items({"user.username": user.username, "request_id": request_id}) as _:
                if request.method == "GET":
                    return data_transfer.query_request(user, request_id)
                elif request.method == "POST":
                    return data_transfer.upload(request_id, request)

        @handler.route("/api/v1/collections", methods=["GET"])
        def list_collections():
            user = auth.authenticate(get_auth_header(request))
            with with_baggage_items({"user.username": user.username}) as _:
                authorized_collections = [name for name, col in collections.items() if user.has_access(col.roles)]
                return RequestSucceeded(authorized_collections)

        # New handler
        # @handler.route("/api/v1/collection/<collection>", methods=["GET"])
        # def describe_collection(collection):
        #     auth_header = get_auth_header(request)
        #     authorized_collections = []
        #     for name, collection in collections.items():
        #         try:
        #             if auth.can_access_collection(auth_header, collection):
        #                 authorized_collections.append(name)
        #         except ForbiddenRequest:
        #             pass
        #     return RequestSucceeded(authorized_collections)

        @handler.before_request
        def add_route_to_baggage():
            route = request.url_rule.rule if request.url_rule else request.path
            ctx = baggage.set_baggage("http.route", route)
            ctx = baggage.set_baggage("http.method", request.method, context=ctx)
            g._baggage_token = attach(ctx)

        @handler.before_request
        def only_json():
            if request.method != "POST":
                return
            if request.is_json:
                return
            if "/uploads/" in request.path:
                return
            raise BadRequest("Request must be JSON")

        @handler.after_request
        def add_header(response: flask.Response):
            response.cache_control.no_cache = True
            response.cache_control.no_store = True
            response.headers["X-Content-Type-Options"] = "nosniff"
            response.headers["X-Frame-Options"] = "DENY"
            response.headers["X-XSS-Protection"] = "1; mode=block"
            return response

        @handler.teardown_request
        def remove_route_from_baggage(exc):
            token = getattr(g, "_baggage_token", None)
            if token:
                detach(token)

        return handler

    def run_server(self, handler, server_type, host, port):
        if server_type == "flask":
            # flask internal server for non-production environments
            # should only be used for testing and debugging
            handler.run(host=host, port=port, debug=True)
        elif server_type == "gunicorn":
            options = {"bind": "%s:%s" % (host, port), "workers": 1}
            GunicornServer(handler, options).run()
        elif server_type == "werkzeug":
            pass
            # werkzeug_server(host, port, handler, use_reloader=True)
        else:
            logging.error("server_type %s not supported" % server_type)
            raise NotImplementedError
