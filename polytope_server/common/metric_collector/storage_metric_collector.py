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

import sys

from ..metric import MongoStorageInfo, S3StorageInfo, StorageInfo
from . import MetricCollector


class StorageMetricCollector(MetricCollector):
    def __init__(self, host, storage_type):
        self.host = host
        self.storage_type = storage_type

    def collect(self):
        return StorageInfo(
            storage_host=self.host,
            storage_type=self.storage_type,
            storage_space_used=self.storage_space_used(),
            storage_space_limit=self.storage_space_limit(),
            device_space_used=self.device_space_used(),
            device_space_limit=self.device_space_limit(),
            entries=self.total_entries(),
        )

    def storage_space_used(self):
        return "not implemented"

    def storage_space_limit(self):
        return "not implemented"

    def device_space_used(self):
        return "not implemented"

    def device_space_limit(self):
        return "not implemented"

    def total_entries(self):
        return "not implemented"


class DictStorageMetricCollector(StorageMetricCollector):
    def __init__(self, host, dictionary):
        super().__init__(host, "dict")
        self.dictionary = dictionary

    def storage_space_used(self):
        return sys.getsizeof(self.dictionary)

    def total_entries(self):
        return len(self.dictionary)


class MemcachedStorageMetricCollector(StorageMetricCollector):
    def __init__(self, host, client):
        super().__init__(host, "memcached")
        self.client = client

    def storage_space_used(self):
        return self.client.stats()["bytes"]


class RedisStorageMetricCollector(StorageMetricCollector):
    def __init__(self, host, client):
        super().__init__(host, "redis")
        self.client = client

    def storage_space_used(self):
        return self.client.info()["used_memory"]


class MongoStorageMetricCollector(StorageMetricCollector):
    def __init__(self, host, client, database, collection):
        super().__init__(host, "mongodb")
        self.client = client
        self.database = database
        self.collection = collection
        self.store = getattr(self.client, database)[collection]

    def collect(self):
        r = super().collect()
        m = MongoStorageInfo(
            from_dict=r.serialize(),
            collection_name=self.collection,
            db_space_used=self.db_space_used(),
            db_space_limit=self.db_space_limit(),
            db_name=self.db_name(),
        )
        return m

    def storage_space_used(self):
        space_used = 0
        for db in self.client.list_database_names():
            space_used += int(getattr(self.client, db).command({"dbStats": 1}).get("storageSize"))
        return space_used

    def total_entries(self):
        return self.store.count()

    def db_name(self):
        return self.database

    def db_space_used(self):
        return int(getattr(self.client, self.database).command({"dbStats": 1}).get("storageSize"))

    def db_space_limit(self):
        return "not implemented"


class S3StorageMetricCollector(StorageMetricCollector):
    def __init__(self, host, client, bucket):
        super().__init__(host, "s3")
        self.client = client
        self.bucket = bucket

    def collect(self):
        r = super().collect()
        m = S3StorageInfo(
            from_dict=r.serialize(),
            bucket_space_used=self.bucket_space_used(),
            bucket_space_limit=self.bucket_space_limit(),
            bucket_name=self.bucket_name(),
        )
        return m

    def total_entries(self):
        try:
            return len(list(self.client.list_objects(self.bucket)))
        except TypeError:
            # boto only accepts keyword arguments
            return len(list(self.client.list_objects_v2(Bucket=self.bucket)))

    def bucket_name(self):
        return self.bucket

    def bucket_space_used(self):
        size = 0
        try:
            for o in self.client.list_objects(self.bucket):
                size += o.size
            return size
        except TypeError:
            # boto only accepts keyword arguments
            for o in self.client.list_objects_v2(Bucket=self.bucket)["Contents"]:
                size += o["Size"]
            return size

    def bucket_space_limit(self):
        return "not implemented"
