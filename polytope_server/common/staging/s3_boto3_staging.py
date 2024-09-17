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
import time
from concurrent.futures import Future, ThreadPoolExecutor

import boto3
import botocore
from botocore.exceptions import ClientError

from ..metric_collector import S3StorageMetricCollector
from . import staging


class AvailableThreadPoolExecutor(ThreadPoolExecutor):
    def __init__(
        self, max_workers=None, thread_name_prefix="", initializer=None, initargs=()
    ):
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


class S3Staging_boto3(staging.Staging):
    def __init__(self, config):
        self.bucket = config.get("bucket", "default")
        self.url = config.get("url", None)

        self.host = config.get("host", "0.0.0.0")
        self.port = config.get("port", "8333")
        self.use_ssl = config.get("use_ssl", False)
        self.max_threads = config.get("max_threads", 10)
        self.buffer_size = config.get("buffer_size", 10 * 1024 * 1024)
        self.should_set_policy = config.get("should_set_policy", False)

        access_key = config.get("access_key", "")
        secret_key = config.get("secret_key", "")

        for name in ["boto", "urllib3", "s3transfer", "boto3", "botocore", "nose"]:
            logging.getLogger(name).setLevel(logging.WARNING)

        self.prefix = "https" if self.use_ssl else "http"

        self._internal_url = f"http://{self.host}:{self.port}"

        # Setup Boto3 client
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=self._internal_url,
            config=botocore.config.Config(
                max_pool_connections=50,
                s3={"addressing_style ": "path"},
            ),
            # use_ssl=self.use_ssl,
        )

        # Attempt to create the bucket
        try:
            self.s3_client.create_bucket(Bucket=self.bucket)
        except self.s3_client.exceptions.BucketAlreadyExists:
            logging.info(f"Bucket {self.bucket} already exists.")
        except self.s3_client.exceptions.BucketAlreadyOwnedByYou:
            logging.info(f"Bucket {self.bucket} already exists and owned by you.")
        except ClientError as e:
            logging.error(f"Error creating bucket: {e}")
        # Set bucket policy
        if self.should_set_policy:
            self.set_bucket_policy()
        self.storage_metric_collector = S3StorageMetricCollector(
            self.host, self.s3_client, self.bucket, self.get_type()
        )

        logging.info(
            f"Opened data staging at {self.host}:{self.port} with bucket {self.bucket}"
        )

    def create(self, name, data, content_type):
        name = name + ".grib"
        # fix for seaweedfs auto-setting Content-Disposition to inline and earthkit expecting extension,
        # else using content-disposition header
        try:
            multipart_upload = self.s3_client.create_multipart_upload(
                Bucket=self.bucket,
                Key=name,
                ContentType=content_type,
                ContentDisposition="attachment",
            )
            upload_id = multipart_upload["UploadId"]

            parts = []
            part_number = 1
            futures = []

            with AvailableThreadPoolExecutor(max_workers=self.max_threads) as executor:
                executor.wait_for_available_worker()
                if not data:
                    logging.info(
                        f"No data provided. Uploading a single empty part for {name}."
                    )
                else:
                    for part_data in self.iterator_buffer(data, self.buffer_size):
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
                self.s3_client.abort_multipart_upload(
                    Bucket=self.bucket, Key=name, UploadId=upload_id
                )
                raise ValueError("No data retrieved")

            self.s3_client.complete_multipart_upload(
                Bucket=self.bucket,
                Key=name,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )

            logging.info(f"Successfully uploaded {name} in {len(parts)} parts.")
            return self.get_url(name)

        except ClientError as e:
            logging.error(f"Failed to upload {name}: {e}")
            if "upload_id" in locals():
                self.s3_client.abort_multipart_upload(
                    Bucket=self.bucket, Key=name, UploadId=upload_id
                )
            raise

    def upload_part(self, name, part_number, data, upload_id):
        logging.debug(f"Uploading part {part_number} of {name}, {len(data)} bytes")
        response = self.s3_client.upload_part(
            Bucket=self.bucket,
            Key=name,
            PartNumber=part_number,
            UploadId=upload_id,
            Body=data,
        )
        return {"PartNumber": part_number, "ETag": response["ETag"]}

    def set_bucket_policy(self):
        """
        Grants read access to individual objects - user has access to all objects, but would need to know the UUID.
        Denies read access to the bucket (cannot list objects) - important, so users cannot see all UUIDs!
        Denies read access to the bucket location
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
        self.s3_client.put_bucket_policy(Bucket=self.bucket, Policy=json.dumps(policy))

    def read(self, name):
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=name)
            return response["Body"].read()
        except ClientError as e:
            logging.error(f"Could not read object {name}: {e}")
            raise KeyError(name)

    def delete(self, name):
        try:
            self.s3_client.delete_object(Bucket=self.bucket, Key=name)
            return True
        except ClientError as e:
            logging.error(f"Could not delete object {name}: {e}")
            raise KeyError(name)

    def query(self, name):
        try:
            self.s3_client.head_object(Bucket=self.bucket, Key=name)
            return True
        except ClientError:
            return False

    def stat(self, name):
        try:
            response = self.s3_client.head_object(Bucket=self.bucket, Key=name)
            return response["ContentType"], response["ContentLength"]
        except ClientError as e:
            logging.error(f"Could not stat object {name}: {e}")
            raise KeyError(name)

    def get_url(self, name):
        if self.url:
            if self.url.startswith("http"):
                # This covers both http and https
                return f"{self.url}/{self.bucket}/{name}"
            else:
                return f"{self.prefix}://{self.url}/{self.bucket}/{name}"
        return None

    def get_internal_url(self, name):
        pass

    def get_type(self):
        return "S3DataStaging_boto3"

    def list(self):
        try:
            resources = []
            data = self.s3_client.list_objects_v2(
                Bucket=self.bucket, MaxKeys=999999999999999
            )

            if data.get("contents", {}).get("IsTruncated  ncated", False):
                logging.warning(
                    "Truncated list of objects. Some objects may not be listed."
                )

            if "Contents" not in data:  # No objects in the bucket
                return resources
            for o in data["Contents"]:
                resources.append(staging.ResourceInfo(o["Key"], o["Size"]))
            return resources
        except ClientError as e:
            logging.error(f"Failed to list objects: {e}")
            raise

    def wipe(self):
        objects_to_delete = self.list()
        delete_objects = [{"Key": obj} for obj in objects_to_delete]
        if delete_objects:
            try:
                logging.info(
                    f"Deleting {len(delete_objects)} : {delete_objects} objects from {self.bucket}"
                )
                self.s3_client.delete_objects(
                    Bucket=self.bucket, Delete={"Objects": delete_objects}
                )
            except ClientError as e:
                logging.error(f"Error deleting objects: {e}")
                raise

    def collect_metric_info(self):
        return self.storage_metric_collector.collect().serialize()

    def get_url_prefix(self):
        return "{}/".format(self.bucket)

    def iterator_buffer(self, iterable, buffer_size):
        buffer = b""
        for data in iterable:
            buffer += data
            if len(buffer) >= buffer_size:
                output, leftover = buffer[:buffer_size], buffer[buffer_size:]
                buffer = leftover
                yield output

        yield buffer
