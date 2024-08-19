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

import copy
import json
import logging
import time
from concurrent.futures import Future, ThreadPoolExecutor

import minio
from minio import Minio
from minio.definitions import UploadPart
from minio.error import BucketAlreadyExists, BucketAlreadyOwnedByYou, NoSuchKey

from ..metric_collector import S3StorageMetricCollector
from . import staging


class AvailableThreadPoolExecutor(ThreadPoolExecutor):

    def __init__(self, max_workers=None, thread_name_prefix="", initializer=None, initargs=()):
        super().__init__(max_workers, thread_name_prefix, initializer, initargs)
        self._running_worker_futures: set[Future] = set()

    @property
    def available_workers(self) -> int:
        return self._max_workers - len(self._running_worker_futures)

    def wait_for_available_worker(self, timeout=None) -> None:
        start_time = time.monotonic()
        while True:
            if self.available_workers > 0:
                return
            if timeout is not None and time.monotonic() - start_time > timeout:
                raise TimeoutError
            time.sleep(0.1)

    def submit(self, fn, /, *args, **kwargs):
        f = super().submit(fn, *args, **kwargs)
        self._running_worker_futures.add(f)
        f.add_done_callback(self._running_worker_futures.remove)
        return f


class S3Staging(staging.Staging):
    def __init__(self, config):
        self.host = config.get("host", "0.0.0.0")
        self.port = config.get("port", "8000")
        self.max_threads = config.get("max_threads", 20)
        self.buffer_size = config.get("buffer_size", 20 * 1024 * 1024)
        endpoint = "{}:{}".format(self.host, self.port)
        access_key = config.get("access_key", "")
        secret_key = config.get("secret_key", "")
        self.bucket = config.get("bucket", "default")
        secure = config.get("secure", False)
        self.url = config.get("url", None)
        internal_url = "{}:{}".format(self.host, self.port)
        secure = config.get("use_ssl", False)

        if access_key == "" or secret_key == "":
            self.client = Minio(
                internal_url,
                secure=secure,
            )

        else:
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

    def upload_part(self, part_number, buf, metadata, name, upload_id):
        logging.debug(f"Uploading part {part_number} ({len(buf)} bytes) of {name}")
        etag = self.client._do_put_object(
            self.bucket, name, buf, len(buf), part_number=part_number, metadata=metadata, upload_id=upload_id
        )
        return etag, len(buf)

    def create(self, name, data, content_type):
        url = self.get_url(name)
        logging.info("Putting to staging: {}".format(name))

        metadata = minio.helpers.amzprefix_user_metadata({})
        metadata["Content-Type"] = content_type

        upload_id = self.client._new_multipart_upload(self.bucket, name, metadata)

        parts = {}
        part_number = 1
        futures = []

        with AvailableThreadPoolExecutor(max_workers=self.max_threads) as executor:
            executor.wait_for_available_worker()
            for buf in self.iterator_buffer(data, self.buffer_size):
                if len(buf) == 0:
                    break
                future = executor.submit(self.upload_part, copy.copy(part_number), buf, metadata, name, upload_id)
                futures.append((future, part_number))
                part_number += 1

        try:
            for future, part_number in futures:
                etag, size = future.result()
                parts[part_number] = UploadPart(self.bucket, name, upload_id, part_number, etag, None, size)
        except Exception as e:
            logging.error(f"Error uploading parts: {str(e)}")
            self.client._remove_incomplete_upload(self.bucket, name, upload_id)
            raise

        # Completing upload
        try:
            logging.info(parts)
            try:
                self.client._complete_multipart_upload(self.bucket, name, upload_id, parts)
            except:
                time.sleep(5)
                self.client._complete_multipart_upload(self.bucket, name, upload_id, parts)

        except Exception as e:
            logging.error(f"Error completing multipart upload: {str(e)}")
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
