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
import types

import falcon
from flask import request

from . import frontend
from .common.application_server import GunicornServer
from .common.data_transfer import DataTransfer
from .common.flask_decorators import delete_request

httpStatus = {
    True: falcon.HTTP_200,
    False: falcon.HTTP_400,
    200: falcon.HTTP_200,
    201: falcon.HTTP_201,
    202: falcon.HTTP_202,
    300: falcon.HTTP_300,
    301: falcon.HTTP_301,
    302: falcon.HTTP_302,
    400: falcon.HTTP_400,
    403: falcon.HTTP_403,
    404: falcon.HTTP_404,
    405: falcon.HTTP_405,
    500: falcon.HTTP_500,
}


def from_falcon(falcon_request):
    http_request = types.SimpleNamespace()
    headers_lowercase = {k.lower(): v for k, v in falcon_request.headers.items()}
    http_request.headers = headers_lowercase
    http_request.json = json.loads(falcon_request.bounded_stream.read().decode("utf-8"))
    return http_request


def to_falcon(falcon_response, response, status):
    if status:
        falcon_response.status = httpStatus[status]
    else:
        falcon_response.status = httpStatus[response is not None]

    try:
        json.loads(response)
        falcon_response.body = response
    except Exception:
        falcon_response.body = json.dumps({"result": response})

    return falcon_response


def check_authentication(req, resp, resource, params, authentication):
    http_request = types.SimpleNamespace()
    headers_lowercase = {k.lower(): v for k, v in req.headers.items()}
    http_request.headers = headers_lowercase
    http_request.json = None
    if not authentication.check(http_request):
        raise falcon.HTTPError(falcon.HTTP_403, "error: not authenticated")


def check_request_limit(req, resp, resource, params, authentication, request_store):
    http_request = types.SimpleNamespace()
    headers_lowercase = {k.lower(): v for k, v in req.headers.items()}
    http_request.headers = headers_lowercase
    http_request.json = None
    user_hard_limit = authentication.get_limit_per_user([http_request.headers["username"]], limit_type="soft_limit")
    n_user_requests = len(request_store.get_requests(username=http_request.headers["username"]))
    if n_user_requests > user_hard_limit:
        raise falcon.HTTPError(falcon.HTTP_403, "error: request limit reached")


class FalconHandler(frontend.FrontendHandler):
    def create_handler(self, request_store, authentication, staging):
        handler = falcon.API()

        data_transfer = DataTransfer(request_store, staging)

        class Test(object):
            def on_get(self, falcon_request, falcon_response):
                falcon_response = to_falcon(falcon_response, "result: hello world", 200)

        handler.add_route("/api/v1/test", Test())

        class Users(object):
            def on_post(self, falcon_request, falcon_response):
                http_request = from_falcon(falcon_request)
                response, status = authentication.add_user(http_request)
                falcon_response = to_falcon(falcon_response, response, status)

            def on_delete(self, falcon_request, falcon_response):
                http_request = from_falcon(falcon_request)
                response, status = authentication.remove_user(http_request)
                falcon_response = to_falcon(falcon_response, response, status)

        handler.add_route("/api/v1/auth/users", Users())

        class Token(object):
            def on_post(self, falcon_request, falcon_response):
                http_request = from_falcon(falcon_request)
                response, status = authentication.authenticate(http_request)
                falcon_response = to_falcon(falcon_response, response, status)

        handler.add_route("/api/v1/auth/keys", Token())

        @falcon.before(check_authentication, authentication)
        class AllRequests(object):
            def on_get(self, falcon_request, falcon_response):
                # http_request = from_falcon(falcon_request)
                response = request_store.fetch_requests(username=request.headers["username"])
                falcon_response = to_falcon(falcon_response, response, None)

            @falcon.before(check_request_limit, authentication, request_store)
            def on_post(self, falcon_request, falcon_response):
                http_request = from_falcon(falcon_request)
                if request.headers["verb"] == "retrieve":
                    response, status = data_transfer.download(http_request)
                elif request.headers["verb"] == "archive":
                    response, status = data_transfer.upload(http_request)
                else:
                    response, status = (
                        json.dumps({"error": "transfer type %s not supported" % request.json["method"]}),
                        202,
                    )
                falcon_response = to_falcon(falcon_response, response, status)

        handler.add_route("/api/v1/requests", AllRequests())

        @falcon.before(check_authentication, authentication)
        class SingleRequests(object):
            def on_get(self, falcon_request, falcon_response, request_id):
                response = request_store.fetch_request(id=request_id)
                falcon_response = to_falcon(falcon_response, response, None)

            def on_delete(self, falcon_request, falcon_response, request_id):
                is_request_removed, is_file_removed = delete_request(request_id, data_transfer, staging, request_store)
                if is_file_removed and is_request_removed:
                    falcon_response = to_falcon(
                        falcon_response,
                        "result: successfully deleted request",
                        200,
                    )
                else:
                    if not is_file_removed:
                        falcon_response = to_falcon(
                            falcon_response,
                            "error: failed to remove file from staging area",
                            400,
                        )
                    else:
                        falcon_response = to_falcon(
                            falcon_response,
                            "error: failed to delete request",
                            400,
                        )

        handler.add_route("/api/v1/requests/{id}", SingleRequests())

        class Downloads(object):
            def on_get(self, falcon_response, request_id):
                if request_store.fetch_request(request_id):
                    if data_transfer.check_staging(request_id, request_store, staging):
                        (
                            object_path,
                            object_name,
                        ) = data_transfer.get_staging_path(request_id, request_store, staging)
                        data = staging.read_iter(str(object_path / object_name), "rb")
                        falcon_response.set_header(
                            "Content-Disposition",
                            'attachment; filename="%s"' % object_name,
                        )
                        falcon_response.data = data
                        falcon_response.status = falcon.HTTP_200
                    else:
                        falcon_response = to_falcon(
                            falcon_response,
                            "result: not ready for download yet",
                            202,
                        )
                else:
                    falcon_response = to_falcon(
                        falcon_response,
                        "error: request_id %s does not exist" % request_id,
                        400,
                    )

        handler.add_route("/api/v1/downloads/{request_id}", Downloads)

        class Uploads(object):
            def on_get(self, falcon_response, request_id):
                if request_store.fetch_request(request_id):
                    if data_transfer.check_staging(request_id, request_store, staging):
                        falcon_response = to_falcon(
                            falcon_response,
                            "result: file still being uploaded",
                            202,
                        )
                    else:
                        falcon_response = to_falcon(
                            falcon_response,
                            "result: file has been successfully uploaded",
                            202,
                        )
                else:
                    falcon_response = to_falcon(
                        falcon_response,
                        "error: request_id %s does not exist" % request_id,
                        400,
                    )

        handler.add_route("/api/v1/uploads/{request_id}", Downloads)

        return handler

    def run_server(self, handler, server_type, host, port):

        if server_type == "gunicorn":
            options = {"bind": "%s:%s" % (host, port), "workers": 1}
            GunicornServer(handler, options).run()
        else:
            pass
            # werkzeug_server(host, port, handler, use_reloader=True)
