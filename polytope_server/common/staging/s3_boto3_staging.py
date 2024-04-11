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
from pprint import pprint as pp

import boto3
from botocore.exceptions import ClientError

from ..metric_collector import S3StorageMetricCollector
from . import staging


class S3Staging_boto3(staging.Staging):
    def __init__(self, config):

        self.bucket = config.get("bucket", "default")
        self.url = config.get("url", None)

        self.host = config.get("host", "0.0.0.0")
        self.use_ssl = config.get("use_ssl", False)

        access_key = config.get("access_key", "")
        secret_key = config.get("secret_key", "")

        # Setup Boto3 client
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=self.host,
            use_ssl=self.use_ssl,
        )

        self.resource = boto3.resource(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=self.host,
            use_ssl=self.use_ssl,
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
        self.set_bucket_policy()
        self.storage_metric_collector = S3StorageMetricCollector(self.host, self.s3_client, self.bucket)

        logging.info(f"Opened data staging at {self.host} with bucket {self.bucket}")

    def create(self, name, data, content_type):
        try:
            multipart_upload = self.s3_client.create_multipart_upload(
                Bucket=self.bucket, Key=name, ContentType=content_type
            )
            upload_id = multipart_upload["UploadId"]

            parts = []
            part_number = 1
            for part_data in self.iterator_buffer(data, 200 * 1024 * 1024):
                response = self.s3_client.upload_part(
                    Bucket=self.bucket, Key=name, PartNumber=part_number, UploadId=upload_id, Body=part_data
                )
                parts.append({"PartNumber": part_number, "ETag": response["ETag"]})
                part_number += 1

            if not parts:  # Handling the case of empty data
                logging.info(f"Uploading single empty part of {name}")
                response = self.s3_client.upload_part(
                    Bucket=self.bucket, Key=name, PartNumber=part_number, UploadId=upload_id, Body=b""
                )
                parts.append({"PartNumber": part_number, "ETag": response["ETag"]})

            self.s3_client.complete_multipart_upload(
                Bucket=self.bucket, Key=name, UploadId=upload_id, MultipartUpload={"Parts": parts}
            )
            logging.info(f"Successfully uploaded {name} in {len(parts)} parts.")
            return self.get_url(name)
        except ClientError as e:
            logging.error(f"Failed to upload {name}: {e}")
            self.s3_client.abort_multipart_upload(Bucket=self.bucket, Key=name, UploadId=upload_id)
            raise

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
                    "Resource": "arn:aws:s3:::{self.bucket}}/*",
                },
                {
                    "Sid": "DenyListBucket",
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": "s3:ListBucket",
                    "Resource": "arn:aws:s3:::{self.bucket}}",
                },
                {
                    "Sid": "DenyGetBucketLocation",
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": "s3:GetBucketLocation",
                    "Resource": "arn:aws:s3:::{self.bucket}}",
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
            return f"{self.url}/{self.bucket}/{name}"
        return None

    def get_internal_url(self, name):
        pass

    def get_type(self):
        return "S3DataStaging_boto3"

    def list(self):
        try:
            resources = []
            data = self.s3_client.list_objects_v2(Bucket=self.bucket)
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
                logging.info(f"Deleting {len(delete_objects)} : {delete_objects} objects from {self.bucket}")
                self.s3_client.delete_objects(Bucket=self.bucket, Delete={"Objects": delete_objects})
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