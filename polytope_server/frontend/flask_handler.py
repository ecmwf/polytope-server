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
import os
import pathlib
import tempfile

import flask
import yaml
from flask import Flask, request
from flask_swagger_ui import get_swaggerui_blueprint
from werkzeug.exceptions import default_exceptions
from werkzeug.middleware.proxy_fix import ProxyFix

from ..common.exceptions import BadRequest, ForbiddenRequest, HTTPException, NotFound
from ..version import __version__
from . import frontend
from .common.application_server import GunicornServer
from .common.data_transfer import DataTransfer
from .common.flask_decorators import RequestSucceeded


class FlaskHandler(frontend.FrontendHandler):
    def create_handler(
        self,
        request_store,
        auth,
        staging,
        collections,
        identity,
        apikeygenerator,
        proxy_support: bool,
    ):
        handler = Flask(__name__)

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
        handler.register_blueprint(SWAGGERUI_BLUEPRINT, url_prefix=SWAGGER_URL)

        data_transfer = DataTransfer(request_store, staging)

        @handler.errorhandler(Exception)
        def default_error_handler(error):
            logging.exception(str(error))
            return (
                json.dumps({"message": str(error)}),
                500,
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

        for code, ex in default_exceptions.items():
            handler.errorhandler(code)(handle_error)

        @handler.route("/", methods=["GET"])
        def root():
            this_dir = os.path.dirname(os.path.abspath(__file__)) + "/"
            with open(this_dir + "web/index.html") as fh:
                content = fh.read()
            return content

        def get_auth_header(request):
            return request.headers.get("Authorization", "")

        @handler.route("/api/v1/test", methods=["GET"])
        def test():
            if request.method == "GET":
                return RequestSucceeded("Polytope server is alive")

        @handler.route("/api/v1/auth/users", methods=["POST", "DELETE"])
        def addUser():
            auth.has_admin_access(get_auth_header(request))
            username = request.json["username"]
            if request.method == "POST":
                password = request.json["password"]
                role = request.json["role"]
                if identity.add_user(username, password, [role]):
                    return RequestSucceeded("Successfully added user")
            elif request.method == "DELETE":
                if identity.remove_user(username):
                    return RequestSucceeded("Successfully removed user")

        @handler.route("/api/v1/auth/keys", methods=["POST"])
        def getToken():
            user = auth.authenticate(get_auth_header(request))
            if request.method == "POST":
                apikey = apikeygenerator.create_key(user)
                return RequestSucceeded({"key": apikey.key, "expires": apikey.expiry})

        @handler.route(
            "/api/v1/user",
            methods=[
                "GET",
            ],
        )
        def requestLimits():
            user = auth.authenticate(get_auth_header(request))
            if request.method == "GET":
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
            if request.method == "GET":
                user_requests = request_store.get_requests(user=user)
                response_message = []
                for i in user_requests:
                    response_message.append(i.serialize())
                return RequestSucceeded(response_message)

        # corresponds to:
        # @handler.route("/api/v1/requests/<collection>", methods = ['POST'])
        # see: @handler.route("/api/v1/requests/<collection_or_request_id>", methods = ['GET','POST','DELETE'])
        def handle_requests(request, collection):
            if request.method == "POST":
                user = auth.can_access_collection(get_auth_header(request), collections[collection])

                if "verb" not in request.json:
                    raise BadRequest("HTTP request content is missing 'verb' (e.g. retrieve)")

                if "request" not in request.json:
                    raise BadRequest("HTTP request content is missing 'request'")

                verb = request.json["verb"]
                if verb == "retrieve":
                    return data_transfer.request_download(request, user, collection, verb)
                elif verb == "archive":
                    return data_transfer.request_upload(request, user, collection, verb)
                else:
                    raise BadRequest("Transfer type %s not supported" % verb)
            elif request.method == "GET":
                user = auth.authenticate(get_auth_header(request))
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
        def handle_specific_request(request, request_id):
            user = auth.authenticate(get_auth_header(request))
            if request.method == "GET":
                return data_transfer.query_request(user, request_id)
            elif request.method == "POST":
                raise NotFound("Unsupported collection type: %s" % request_id)
            elif request.method == "DELETE":
                return data_transfer.delete_request(user, request_id)

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
            logging.warning("Serving download data directly through frontend")
            if request.method == "GET":
                return data_transfer.download(request_id)

        @handler.route("/api/v1/uploads/<request_id>", methods=["GET", "POST"])
        def uploads(request_id):
            user = auth.authenticate(get_auth_header(request))
            if request.method == "GET":
                return data_transfer.query_request(user, request_id)
            elif request.method == "POST":
                return data_transfer.upload(request_id, request)

        @handler.route("/api/v1/collections", methods=["GET"])
        def list_collections():
            auth_header = get_auth_header(request)
            authorized_collections = []
            for name, collection in collections.items():
                try:
                    if auth.can_access_collection(auth_header, collection):
                        authorized_collections.append(name)
                except ForbiddenRequest:
                    pass
            return RequestSucceeded(authorized_collections)

        @handler.after_request
        def add_header(response: flask.Response):
            response.cache_control.no_cache = True
            response.cache_control.no_store = True
            response.headers["X-Content-Type-Options"] = "nosniff"
            response.headers["X-Frame-Options"] = "DENY"
            response.headers["X-XSS-Protection"] = "1; mode=block"
            return response

        @handler.before_request
        def only_json():
            if request.method != "POST":
                return
            if request.is_json:
                return
            if "/uploads/" in request.path:
                return
            raise BadRequest("Request must be JSON")

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
