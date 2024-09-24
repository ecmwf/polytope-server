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
import time
import warnings
from collections import namedtuple
from concurrent.futures import Future, ThreadPoolExecutor

from minio import Minio
from minio.error import S3Error

from ..metric_collector import S3StorageMetricCollector
from . import staging

# Ensure that DeprecationWarnings are displayed
warnings.simplefilter("always", DeprecationWarning)

warnings.warn(
    f"The '{__name__}' module is deprecated and will be removed in a future version. "
    "Please migrate to the new module 's3_boto3' to avoid disruption.",
    DeprecationWarning,
    stacklevel=1,
)


# Defining a named tuple to represent a part with part_number and etag
Part = namedtuple("Part", ["part_number", "etag"])


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
        access_key = config.get("access_key", "")
        secret_key = config.get("secret_key", "")
        self.bucket = config.get("bucket", "default")
        secure = config.get("secure", False)
        self.url = config.get("url", None)
        self.internal_url = f"http://{self.host}:{self.port}"
        self.use_ssl = config.get("use_ssl", False)
        self.should_set_policy = config.get("should_set_policy", False)
        
        #remove the protocol from the internal_url, both http and https can be removed
        endpoint = self.internal_url.split("://")[-1]

        if access_key == "" or secret_key == "":
            self.client = Minio(
                endpoint,
                secure=secure,
            )

        else:
            self.client = Minio(
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure,
            )

        self.prefix = "https" if self.use_ssl else "http"

        try:
            self.client.make_bucket(self.bucket)
            if self.should_set_policy:
                self.client.set_bucket_policy(self.bucket, self.bucket_policy())
        except S3Error as err:
            if err.code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                pass
            else:
                raise

        self.storage_metric_collector = S3StorageMetricCollector(
            self.internal_url, self.client, self.bucket, self.get_type()
        )

        logging.info(
            "Opened data staging at {}:{}/{}, locatable from {}".format(self.host, self.port, self.bucket, self.url)
        )

    def create(self, name, data, content_type):
        name = name + ".grib"
        try:
            # Prepare headers for content type and content disposition
            headers = {
                "Content-Type": content_type,
                "Content-Disposition": "attachment",
            }

            # Initiate a multipart upload
            upload_id = self.client._create_multipart_upload(
                bucket_name=self.bucket,
                object_name=name,
                headers=headers,
            )

            parts = []
            part_number = 1
            futures = []

            with AvailableThreadPoolExecutor(max_workers=self.max_threads) as executor:
                executor.wait_for_available_worker()
                if not data:
                    logging.info(f"No data provided. Uploading a single empty part for {name}.")
                    # Upload an empty part
                    result = self.upload_part(name, part_number, b"", upload_id)
                    parts.append(result)
                else:
                    # Ensure 'data' is an iterable of bytes objects
                    if isinstance(data, bytes):
                        data_iter = [data]  # Wrap bytes object in a list to make it iterable
                    elif hasattr(data, "read"):
                        # If 'data' is a file-like object, read it in chunks
                        data_iter = iter(lambda: data.read(self.buffer_size), b"")
                    elif hasattr(data, "__iter__"):
                        data_iter = data  # Assume it's already an iterable of bytes
                    else:
                        raise TypeError("data must be bytes, a file-like object, or an iterable over bytes")

                    for part_data in self.iterator_buffer(data_iter, self.buffer_size):
                        if part_data:
                            futures.append(
                                executor.submit(
                                    self.upload_part,
                                    name,
                                    part_number,
                                    part_data,
                                    upload_id,
                                )
                            )
                            part_number += 1

                    for future in futures:
                        result = future.result()
                        parts.append(result)

            if not parts:
                logging.warning(f"No parts uploaded for {name}. Aborting upload.")
                self.client._abort_multipart_upload(self.bucket, name, upload_id)
                raise ValueError("No data retrieved")

            # Complete multipart upload
            self.client._complete_multipart_upload(
                bucket_name=self.bucket,
                object_name=name,
                upload_id=upload_id,
                parts=parts,
            )

            logging.info(f"Successfully uploaded {name} in {len(parts)} parts.")
            return self.get_url(name)

        except S3Error as e:
            logging.error(f"Failed to upload {name}: {e}")
            if "upload_id" in locals():
                self.client._abort_multipart_upload(self.bucket, name, upload_id)
            raise

    def upload_part(self, name, part_number, data, upload_id):
        logging.debug(f"Uploading part {part_number} of {name}, {len(data)} bytes")

        # 'data' is expected to be a bytes object
        if not isinstance(data, bytes):
            raise TypeError(f"'data' must be bytes, got {type(data)}")

        response = self.client._upload_part(
            bucket_name=self.bucket,
            object_name=name,
            data=data,
            headers=None,
            upload_id=upload_id,
            part_number=part_number,
        )
        etag = response.replace('"', "")  # Remove any quotes from the ETag

        return Part(part_number=part_number, etag=etag)

    def read(self, name):
        try:
            response = self.client.get_object(self.bucket, name)
            if response.status == 200:
                return response.data
            logging.error("Could not read object {}, returned with status: {}".format(name, response.status))
        except S3Error as err:
            if err.code == "NoSuchKey":
                raise KeyError()
            else:
                raise

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
        except S3Error as err:
            if err.code == "NoSuchKey":
                return

    def stat(self, name):
        try:
            obj = self.client.stat_object(self.bucket, name)
            return obj.content_type, obj.size
        except S3Error as err:
            if err.code == "NoSuchKey":
                raise KeyError()
            else:
                raise

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
        if self.url:
            if self.url.startswith("http"):
                # This covers both http and https
                return f"{self.url}/{self.bucket}/{name}"
            else:
                return f"{self.prefix}://{self.url}/{self.bucket}/{name}"
        return None

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
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": "s3:ListBucket",
                    "Resource": f"arn:aws:s3:::{self.bucket}",
                },
                {
                    "Sid": "AllowGetBucketLocation",
                    "Effect": "Deny",
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
