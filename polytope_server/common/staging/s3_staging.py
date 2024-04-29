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


#######################################################################
#
#       S3 Client using the python module designed for MinIO
#
#     https://docs.min.io/docs/python-client-api-reference.html
#
#######################################################################

import json
import logging

import minio
from minio import Minio
from minio.definitions import UploadPart
from minio.error import BucketAlreadyExists, BucketAlreadyOwnedByYou, NoSuchKey

from ..metric_collector import S3StorageMetricCollector
from . import staging


class S3Staging(staging.Staging):
    def __init__(self, config):
        self.host = config.get("host", "0.0.0.0")
        self.port = config.get("port", "8000")
        endpoint = "{}:{}".format(self.host, self.port)
        access_key = config.get("access_key", "")
        secret_key = config.get("secret_key", "")
        self.bucket = config.get("bucket", "default")
        secure = config.get("secure", False) == True
        self.url = config.get("url", None)
        internal_url = "{}:{}".format(self.host, self.port)
        secure = config.get("use_ssl", False)
        self.client = Minio(
            internal_url,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )
        self.internal_url = ("https://" if secure else "http://") + internal_url

        try:
            self.client.make_bucket(self.bucket)
            self.client.set_bucket_policy(self.bucket, self.bucket_policy())
        except BucketAlreadyExists:
            pass
        except BucketAlreadyOwnedByYou:
            pass

        self.storage_metric_collector = S3StorageMetricCollector(endpoint, self.client, self.bucket, self.get_type())

        logging.info(
            "Opened data staging at {}:{}/{}, locatable from {}".format(self.host, self.port, self.bucket, self.url)
        )

    def create(self, name, data, content_type):
        url = self.get_url(name)
        logging.info("Putting to staging: {}".format(name))

        metadata = minio.helpers.amzprefix_user_metadata({})
        metadata["Content-Type"] = content_type

        upload_id = self.client._new_multipart_upload(self.bucket, name, metadata)

        i = 1
        parts = {}
        total_size = 0
        for buf in self.iterator_buffer(data, 200 * 1024 * 1024):
            if len(buf) == 0:
                break
            try:
                logging.info("Uploading part {} ({} bytes) of {}".format(i, len(buf), name))
                etag = self.client._do_put_object(
                    self.bucket,
                    name,
                    buf,
                    len(buf),
                    part_number=i,
                    metadata=metadata,
                    upload_id=upload_id,
                )
                total_size += len(buf)
                parts[i] = UploadPart(self.bucket, name, upload_id, i, etag, None, len(buf))
                i += 1
            except Exception:
                self.client._remove_incomplete_upload(self.bucket, name, upload_id)
                raise

        if len(parts) == 0:
            try:
                logging.info("Uploading single empty part of {}".format(name))
                etag = self.client._do_put_object(
                    self.bucket,
                    name,
                    b"",
                    0,
                    part_number=i,
                    metadata=metadata,
                    upload_id=upload_id,
                )
                total_size += 0
                parts[i] = UploadPart(self.bucket, name, upload_id, i, etag, None, 0)
                i += 1
            except Exception:
                self.client._remove_incomplete_upload(self.bucket, name, upload_id)
                raise

        try:
            self.client._complete_multipart_upload(self.bucket, name, upload_id, parts)
        except Exception:
            self.client._remove_incomplete_upload(self.bucket, name, upload_id)
            raise

        logging.info("Put to {}".format(url))
        return url

    def read(self, name):
        try:
            response = self.client.get_object(self.bucket, name)
            if response.status == 200:
                return response.data
            logging.error("Could not read object {}, returned with status: {}".format(name, response.status))
        except NoSuchKey:
            raise KeyError()

    def delete(self, name):
        if not self.query(name):
            raise KeyError()
        # Does not raise NoSuchKey
        self.client.remove_object(self.bucket, name)
        return True

    def query(self, name):
        try:
            self.client.stat_object(self.bucket, name)
            return True
        except NoSuchKey:
            return False

    def stat(self, name):
        try:
            obj = self.client.stat_object(self.bucket, name)
            return obj.content_type, obj.size
        except NoSuchKey:
            raise KeyError()

    def list(self):
        resources = []
        for o in self.client.list_objects(self.bucket):
            resources.append(staging.ResourceInfo(o.object_name, o.size))
        return resources

    def wipe(self):
        resources = self.list()
        for err in self.client.remove_objects(self.bucket, [v.name for v in resources]):
            logging.debug("Removing object error: {}".format(err))

    def collect_metric_info(self):
        return self.storage_metric_collector.collect().serialize()

    def get_url(self, name):
        if self.url is None:
            return None
        url = "{}/{}/{}".format(self.url, self.bucket, name)
        return url

    def get_internal_url(self, name):
        url = "{}/{}/{}".format(self.internal_url, self.bucket, name)
        return url

    def get_url_prefix(self):
        return "{}/".format(self.bucket)

    def get_type(self):
        return "S3DataStaging"

    def bucket_policy(self):
        """
        Grants read access to individual objects - user has access to all objects, but would need to know the UUID.
        Denies read access to the bucket (cannot list objects) - important, so users cannot see all UUIDs!
        Denies read access to the bucket location (quite meaningless for MinIO)
        """
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AllowReadAccessToIndividualObjects",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{self.bucket}/*",
                },
                {
                    "Sid": "AllowListBucket",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:ListBucket",
                    "Resource": f"arn:aws:s3:::{self.bucket}",
                },
                {
                    "Sid": "AllowGetBucketLocation",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetBucketLocation",
                    "Resource": f"arn:aws:s3:::{self.bucket}",
                },
            ],
        }
        return json.dumps(policy)

    def iterator_buffer(self, iterable, buffer_size):
        buffer = b""
        for data in iterable:
            buffer += data
            if len(buffer) >= buffer_size:
                output, leftover = buffer[:buffer_size], buffer[buffer_size:]
                buffer = leftover
                yield output

        yield buffer
