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

from flask import Flask, request, send_from_directory
from flask_restplus import Api, Resource

from . import frontend
from .common.application_server import GunicornServer
from .common.data_transfer import DataTransfer
from .common.flask_decorators import (
    check_authentication,
    check_request_limit,
    delete_request,
)

httpStatus = {True: 200, False: 400}


class RestplusHandler(frontend.FrontendHandler):
    def create_handler(self, request_store, authentication, staging):
        handler = Flask(__name__)
        handler_api = Api(handler)

        data_transfer = DataTransfer(request_store, staging)

        @handler_api.route("/api/v1/test")
        class Test(Resource):
            def get(self):
                response, status = json.dumps({"result": "hello world"}), 200
                return response, status

        @handler_api.route("/api/v1/auth/users")
        class Users(Resource):
            def post(self):
                response, status = authentication.add_user(request)
                return json.loads(response), status

            def delete(self):
                response, status = authentication.remove_user(request)
                return json.loads(response), status

        @handler_api.route("/api/v1/auth/keys")
        class Token(Resource):
            def post(self):
                response, status = authentication.authenticate(request)
                return json.loads(response), status

        @handler_api.route("/api/v1/requests")
        class AllRequests(Resource):
            @check_authentication(authentication)
            def get(self):
                user_requests = request_store.fetch_requests(username=request.headers["username"])
                return {"result": user_requests}, httpStatus[user_requests is not None]

            @check_authentication(authentication)
            @check_request_limit(authentication, request_store)
            def post(self):
                if request.headers["verb"] == "retrieve":
                    response, status = data_transfer.download(request)
                elif request.headers["verb"] == "archive":
                    response, status = data_transfer.upload(request)
                else:
                    response, status = (
                        json.dumps({"error": "transfer type %s not supported" % request.json["method"]}),
                        202,
                    )
                return response, status

        @handler_api.route("/api/v1/requests/<request_id>")
        class SingleRequests(Resource):
            @check_authentication(authentication)
            def get(self, request_id):
                retrieved_request = request_store.fetch_request(id=request_id)
                return retrieved_request, httpStatus[retrieved_request is not None]

            @check_authentication(authentication)
            def delete(self, request_id):
                is_request_removed, is_file_removed = delete_request(request_id, data_transfer, staging, request_store)
                if is_file_removed and is_request_removed:
                    return (
                        json.dumps("result: successfully deleted request"),
                        httpStatus[is_request_removed],
                    )
                else:
                    if not is_file_removed:
                        return (
                            json.dumps("error: failed to remove file from staging area"),
                            400,
                        )
                    else:
                        return (
                            json.dumps("error: failed to delete request"),
                            400,
                        )

        @handler_api.route("/api/v1/downloads/<request_id>")
        class Downloads(Resource):
            def get(self, request_id):
                if request_store.fetch_request(request_id):
                    if data_transfer.check_staging(request_id, request_store, staging):
                        (
                            object_path,
                            object_name,
                        ) = data_transfer.get_staging_path(request_id, request_store, staging)
                        return (
                            send_from_directory(str(object_path), object_name),
                            200,
                        )
                    else:
                        return (
                            json.dumps("result: Not ready for download yet"),
                            202,
                        )
                else:
                    return (
                        json.dumps("error: request_id %s does not exist" % request_id),
                        400,
                    )

        @handler_api.route("/api/v1/uploads/<request_id>")
        class Uploads(Resource):
            def get(self, request_id):
                if request_store.fetch_request(request_id):
                    if data_transfer.check_staging(request_id, request_store, staging):
                        return (
                            json.dumps("result: File still being uploaded"),
                            202,
                        )
                    else:
                        return (
                            json.dumps("result: File has been successfully uploaded"),
                            202,
                        )
                else:
                    return (
                        json.dumps("error: request_id %s does not exist" % request_id),
                        400,
                    )

        return handler

    def run_server(self, handler, server_type, host, port):
        if server_type == "gunicorn":
            options = {"bind": "%s:%s" % (host, port), "workers": 1}
            GunicornServer(handler, options).run()
        elif server_type == "werkzeug":
            pass
            # werkzeug_server(host, port, handler, use_reloader=True)
        else:
            handler.run(debug=True)
