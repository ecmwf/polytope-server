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

import datetime
import functools
import hashlib
import io
import logging
import pickle
import socket
from abc import ABC, abstractmethod
from typing import Dict, Union

import pymemcache
import pymongo
import redis

from ..metric import MetricType
from ..metric_collector import (
    DictStorageMetricCollector,
    GlobalVarCacheMetricCollector,
    MemcachedCacheMetricCollector,
    MemcachedStorageMetricCollector,
    MongoCacheMetricCollector,
    MongoStorageMetricCollector,
    RedisCacheMetricCollector,
    RedisStorageMetricCollector,
)
from ..request import Status


class Caching(ABC):
    @abstractmethod
    def __init__(self, cache_config):
        pass

    @abstractmethod
    def get_type(self) -> str:
        """Returns the type of the caching system (e.g. redis)"""

    @abstractmethod
    def get(self, key: str) -> object:
        """Gets a cached object by key
        Raises KeyError if key not found
        """

    @abstractmethod
    def set(self, key: str, object: object, lifetime: int):
        """Caches an object by key, for [lifetime] seconds"""

    @abstractmethod
    def wipe(self):
        """Wipes the cache"""

    @abstractmethod
    def collect_metric_info(
        self,
    ) -> Dict[str, Union[None, int, float, str, Status, MetricType]]:
        """Collect dictionary of metrics"""


# ------------------ globalvar cache -----------------


class GlobalVarCaching(Caching):
    def __init__(self, cache_config):
        super().__init__(cache_config)
        self.config = cache_config
        self.store = {}
        host = socket.gethostname()
        self.storage_metric_collector = DictStorageMetricCollector(host, self.store)
        self.cache_metric_collector = GlobalVarCacheMetricCollector()

    def get_type(self):
        return "globalvar"

    def get(self, key):
        obj = self.store.get(key, None)
        if obj is None:
            raise KeyError()
        return obj["data"]

    def set(self, key, object, lifetime):
        if lifetime == 0 or lifetime is None:
            self.store[key] = {"data": object}
        else:
            logging.warn("GlobalVarCaching ignores 'lifetime' arguments")
            expiry = datetime.datetime.now() + datetime.timedelta(seconds=lifetime)
            self.store[key] = {"data": object, "expiry": expiry}

    def wipe(self):
        self.store = {}

    def collect_metric_info(self):
        metric = self.cache_metric_collector.collect().serialize()
        metric["storage"] = self.storage_metric_collector.collect().serialize()
        return metric


# ------------------ Memcached -----------------


class MemcachedCaching(Caching):
    def __init__(self, cache_config):
        super().__init__(cache_config)
        host = cache_config.get("host", "localhost")
        port = cache_config.get("port", 11211)
        endpoint = "{}:{}".format(host, port)
        self.client = pymemcache.client.base.Client((host, port), connect_timeout=5, timeout=1)
        self.storage_metric_collector = MemcachedStorageMetricCollector(endpoint, self.client)
        self.cache_metric_collector = MemcachedCacheMetricCollector(self.client)

    def get_type(self):
        return "memcached"

    def get(self, key):
        obj = self.client.get(key)
        if obj is None:
            raise KeyError()
        return obj

    def set(self, key, object, lifetime):
        if lifetime == 0 or lifetime is None:
            self.client.set(key, object)
        else:
            self.client.set(key, object, lifetime)

    def wipe(self):
        self.client.flush_all()

    def collect_metric_info(self):
        metric = self.storage_metric_collector.collect().serialize()
        metric["storage"] = self.cache_metric_collector.collect().serialize()
        return metric


# ------------------ Redis -----------------


class RedisCaching(Caching):
    def __init__(self, cache_config):
        super().__init__(cache_config)
        host = cache_config.get("host", "localhost")
        port = cache_config.get("port", 6379)
        endpoint = "{}:{}".format(host, port)
        db = cache_config.get("db", 0)
        self.client = redis.Redis(host=host, port=port, db=db)
        self.storage_metric_collector = RedisStorageMetricCollector(endpoint, self.client)
        self.cache_metric_collector = RedisCacheMetricCollector(self.client)

    def get_type(self):
        return "redis"

    def get(self, key):
        obj = self.client.get(key)
        if obj is None:
            raise KeyError()
        return obj

    def set(self, key, object, lifetime):
        if lifetime == 0 or lifetime is None:
            self.client.set(key, object)
        else:
            self.client.set(key, object, ex=lifetime)

    def wipe(self):
        self.client.flushdb()

    def collect_metric_info(self):
        metric = self.cache_metric_collector.collect().serialize()
        metric["storage"] = self.storage_metric_collector.collect().serialize()
        return metric


# ------------------ MongoDB -----------------


class MongoDBCaching(Caching):
    def __init__(self, cache_config):
        super().__init__(cache_config)
        host = cache_config.get("host", "localhost")
        port = cache_config.get("port", 27017)
        endpoint = "{}:{}".format(host, port)
        collection = cache_config.get("collection", "cache")
        self.client = pymongo.MongoClient(host + ":" + str(port), journal=False, connect=False)
        self.database = self.client.cache
        self.collection = self.database[collection]
        self.collection.create_index("expire_at", expireAfterSeconds=0)
        self.collection.update_one({"_id": "hits"}, {"$setOnInsert": {"n": 0}}, upsert=True)
        self.collection.update_one({"_id": "misses"}, {"$setOnInsert": {"n": 0}}, upsert=True)
        self.storage_metric_collector = MongoStorageMetricCollector(endpoint, self.client, "cache", collection)
        self.cache_metric_collector = MongoCacheMetricCollector(self.client, "cache", collection)

    def get_type(self):
        return "mongodb"

    def get(self, key):
        obj = self.collection.find_one({"_id": key})
        if obj is None:
            self.collection.update_one({"_id": "misses"}, {"$inc": {"n": 1}})
            raise KeyError()
        self.collection.update_one({"_id": "hits"}, {"$inc": {"n": 1}})
        return obj["data"]

    def set(self, key, object, lifetime):

        if lifetime == 0 or lifetime is None:
            expiry = datetime.datetime.max
        else:
            expiry = datetime.datetime.now() + datetime.timedelta(seconds=lifetime)

        self.collection.update_one(
            {"_id": key},
            {"$set": {"_id": key, "data": object, "expire_at": expiry}},
            upsert=True,
        )

    def wipe(self):
        hits = self.collection.find_one({"_id": "hits"})["n"]
        misses = self.collection.find_one({"_id": "misses"})["n"]
        self.collection.drop()
        self.collection.update_one({"_id": "hits"}, {"$setOnInsert": {"n": hits}}, upsert=True)
        self.collection.update_one({"_id": "misses"}, {"$setOnInsert": {"n": misses}}, upsert=True)

    def collect_metric_info(self):
        metric = self.cache_metric_collector.collect().serialize()
        metric["storage"] = self.storage_metric_collector.collect().serialize()
        return metric


# ------------------ Decorator -----------------

type_to_class_map = {
    "memcached": "MemcachedCaching",
    "redis": "RedisCaching",
    "mongodb": "MongoDBCaching",
    "globalvar": "GlobalVarCaching",
}


class DBPickler(pickle.Pickler):
    def persistent_id(self, obj):
        if callable(getattr(obj, "cache_id", None)):
            return obj.cache_id()
        else:
            return None


class cache(object):
    """
    This class applies a decorator for caching a function. It uses the fully-qualified-name (FQN) of the function and
    a hash of all function arguments to create a hash key. The return value of the function is serialized and cached.
    The hash is done using pickle and if it encounters custom types it will traverse the entire __dict__ to create a
    unique identifier. To override this behaviour, provide a cache_id() function which returns an identifier. This
    identifier can be a hash or any other picklable object (like a dict). It even works on class methods, where the
    first argument (self) is used as part of the hash. If you want to cache between different instances of a class
    you should define self.cache_id() as described.
    """

    cache = None
    config = None

    @classmethod
    def init(cls, config):
        """
        Initialize caching used by all @cache() decorated functions
        """
        if cls.cache is not None and config == cls.config:
            return
        cls.config = config
        type = next(iter(config.keys()), "mongodb")
        cls.cache = globals()[type_to_class_map[type]](config.get(type, {}))

        logging.info("Caching initialized using {}".format(cls.cache.get_type()))

    @classmethod
    def wipe(cls):
        """
        Wipes the cache entirely.
        """
        cls.cache.wipe()

    @classmethod
    def cancel(cls):
        """
        When called within a @cache() decorated function, cancel will prevent this function
        from being cached when it returns. This is useful if a function fails and you don't
        want to cache the failure.
        """
        cls.cancelled = True

    @classmethod
    def collect_metric_info(cls):
        """Collect dictionary of metrics"""
        return cls.cache.collect_metric_info()

    def __init__(self, lifetime=0, ignore=None):  # lifetime in seconds
        """
        Init is called at import-time when the @cache() decorator is used.
        """
        self.lifetime = lifetime

    def __call__(self, f):
        """
        This function is called when accessing the decorated function.
        """

        @functools.wraps(f)
        def wrapper(*args, **kwargs):

            cache.cancelled = False

            if self.cache is None:
                logging.warning("Caching not set up")
                return f(*args, **kwargs)

            # Hash function args
            items = sorted(kwargs.items())
            hashable_args = (args, tuple(items))
            bytes = io.BytesIO()
            DBPickler(bytes, protocol=-1).dump(hashable_args)
            bytes.seek(0)
            hashed_args = hashlib.md5(bytes.read()).hexdigest()

            # Generate unique cache key
            cache_key = "{0}-{1}-{2}".format(f.__module__, f.__qualname__, hashed_args)

            # logging.debug(cache_key)

            # Return cached version
            try:
                cache_result = self.cache.get(cache_key)
                logging.debug("Cache hit with key {}".format(cache_key))
                result = pickle.loads(cache_result)
                return result
            except KeyError:
                logging.info("Cache miss with key {}".format(cache_key))

            result = f(*args, **kwargs)

            # Cache output

            if not cache.cancelled:
                try:
                    data = pickle.dumps(result)
                except Exception:
                    logging.warning("Could not cache object, the return type is not serializable.")
                    return result

                logging.debug("Caching function result with key {}".format(cache_key))

                self.cache.set(cache_key, data, self.lifetime)

            return result

        return wrapper
