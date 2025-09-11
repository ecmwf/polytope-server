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

import hashlib
import sys
from pathlib import PurePosixPath
from urllib.parse import urlparse

# TODO: Remove flask from this module, it should be agnostic
from flask import Response

from ...common.exceptions import BadRequest, NotFound, ServerError
from ...common.request import Request, Status, Verb
from ...common.user import User
from .flask_decorators import RequestAccepted, RequestRedirected, RequestSucceeded


class DataTransfer:
    def __init__(self, request_store, staging):
        self.request_store = request_store
        self.staging = staging

    def request_download(self, http_request, user, collection, verb):
        payload = http_request.json
        request = Request(
            user=user,
            collection=collection,
            status=Status.WAITING,
            verb=Verb.RETRIEVE,
            user_request=str(payload["request"]),
        )
        try:
            self.request_store.add_request(request)
        except Exception:
            raise ServerError("Error while attempting to add new request to request store")

        response = self.construct_response(request)
        return RequestAccepted(response)

    def request_upload(self, http_request, user, collection, verb):
        payload = http_request.json
        url = payload.get("url", None)
        request = Request(
            user=user,
            collection=collection,
            url=url,
            status=Status.UPLOADING,
            verb=Verb.ARCHIVE,
            user_request=str(payload["request"]),
        )

        if url not in (None, ""):
            request.set_status(Status.WAITING)

        try:
            self.request_store.add_request(request)
        except Exception:
            raise ServerError("Error while attempting to add new request to request store")

        response = self.construct_response(request)
        return RequestAccepted(response)

    def query_request(self, user, id):
        request = self.get_request(id)
        if not request:
            raise NotFound("Request {} not found".format(id))
        if request.user != user:
            raise NotFound("Request {} not found".format(id))
        if request.status == Status.FAILED:
            raise BadRequest("Request failed with error:\n{}".format(request.user_message))

        if request.status == Status.PROCESSED:
            if request.verb == Verb.RETRIEVE:
                return self.process_download(request)
            else:
                assert request.verb == Verb.ARCHIVE
                response = self.construct_response(request)
                return RequestSucceeded(response)

        response = self.construct_response(request)
        return RequestAccepted(response)

    def download(self, id):

        if id.startswith(self.staging.get_url_prefix()):
            id = id.replace(self.staging.get_url_prefix(), "", 1)

        request = self.get_request(id)
        if request:
            if request.verb != Verb.RETRIEVE:
                raise BadRequest("Request {} is not a download".format(id))
            if request.status == Status.PROCESSED:
                return self.create_download_response(id)
        raise BadRequest("Request {} not ready for download yet".format(id))

    def upload(self, id, http_request):
        request = self.get_request(id)
        if not request:
            raise BadRequest("Request {} does not exist".format(id))
        if request.verb != Verb.ARCHIVE:
            raise BadRequest("Request {} is not an upload".format(id))
        if request.status == Status.PROCESSED:
            return RequestSucceeded("Data has already been uploaded")

        data = http_request.data
        checksum = http_request.headers["X-Checksum"]
        if checksum != hashlib.md5(data).hexdigest():
            raise BadRequest("Uploaded data checksum does not agree with header X-Checksum")

        self.upload_to_staging(data, id)

        request.set_status(Status.WAITING)
        request.url = self.staging.get_internal_url(id)
        self.request_store.update_request(request)
        response = self.construct_response(request)
        return RequestAccepted(response)

    def process_download(self, request):
        try:
            object_id = request.id
            
            if request.url is not None and request.url != "":
                # TODO: temporary fix for Content-Disposition earthkit issues
                url_path = PurePosixPath(urlparse(request.url).path)
                extension = url_path.suffix

                if extension is not None and len(extension) > 0:
                    object_id = request.id + extension

            request.content_type, request.content_length = self.staging.stat(object_id)
        except Exception:
            raise ServerError("Error while querying data staging with {}".format(object_id))

        response = self.construct_response(request)
        return RequestRedirected(response)

    def upload_to_staging(self, data, id):
        url = None

        try:
            url = self.staging.create(id, [data], "application/octet-stream")
            assert url is not None
        except Exception:
            raise ServerError("Error writing to data staging")

        try:
            staged_content_type, staged_size = self.staging.stat(id)
            if staged_size != (sys.getsizeof(data) - sys.getsizeof(b"")):
                raise ServerError("Size of data uploaded to staging area did not match size of user-uploaded data")
        except Exception:
            raise ServerError("Error reading uploaded data from data staging")

        return url

    def construct_response(self, request):

        location = "./{}".format(request.id)

        response = {}
        # Request is completed
        if request.verb == Verb.RETRIEVE and request.content_length is not None:
            response["contentLength"] = request.content_length
            response["contentType"] = request.content_type
            # No URL provided, serve through frontend
            if request.url is None:
                location = "../downloads/{}".format(request.id)
            # Relative URL
            elif request.url.startswith("./"):
                location = "../{}".format(request.url.strip("./"))
            # Absolute URL
            else:
                location = request.url
                assert "://" in location

        if request.verb == Verb.ARCHIVE and request.status == Status.UPLOADING:
            location = "../uploads/{}".format(request.id)

        response["location"] = location
        response["message"] = request.user_message
        response["status"] = request.status.value
        if response["status"] == "waiting":
            response["status"] = "queued"
        return response

    def revoke_request(self, user: User, id: str):
        n = self.request_store.revoke_request(user, id)

        return RequestSucceeded(f"Successfully revoked {n} requests")

    def get_request(self, id):
        try:
            request = self.request_store.get_request(id)
        except Exception:
            raise ServerError("Error while fetching from the request store")
        return request

    def create_download_response(self, id):
        content_type, content_size = self.staging.stat(id)
        try:
            data = self.staging.read(id)
        except Exception:
            raise ServerError("Error while reading data from data staging")

        data_checksum = hashlib.md5(data).hexdigest()
        response = Response(data)
        response.headers.set("Content-Type", content_type)
        response.headers["Content-MD5"] = data_checksum
        response.status_code = 200
        return response
