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

from ..metric import CacheInfo
from . import MetricCollector


class CacheMetricCollector(MetricCollector):
    def collect(self):
        return CacheInfo(hits=self.hits(), misses=self.misses())

    def hits(self):
        return None

    def misses(self):
        return None


class GlobalVarCacheMetricCollector(CacheMetricCollector):
    pass


class MemcachedCacheMetricCollector(CacheMetricCollector):
    def __init__(self, client):
        self.client = client

    def hits(self):
        return "not implemented"

    def misses(self):
        return "not implemented"


class RedisCacheMetricCollector(CacheMetricCollector):
    def __init__(self, client):
        self.client = client

    def hits(self):
        return "not implemented"

    def misses(self):
        return "not implemented"


class MongoCacheMetricCollector(CacheMetricCollector):
    def __init__(self, client, database, collection):
        self.client = client
        self.store = getattr(self.client, database)[collection]

    def hits(self):
        return self.store.find_one({"_id": "hits"})["n"]

    def misses(self):
        return self.store.find_one({"_id": "misses"})["n"]
