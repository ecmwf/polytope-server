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
from typing import Any, Dict, List, Protocol, cast

from pymongo.collection import Collection

from polytope_server.common.metric_calculator.mongo import MongoMetricCalculator


class CollectionProtocol(Protocol):
    """Protocol for collection-like objects."""

    name: str

    def create_index(self, keys, name=None, **kwargs) -> None: ...


class DummyCollection:
    """Minimal stub for the main request collection."""

    def __init__(self) -> None:
        self.name = "dummy_requests"
        self.created_indexes: List[Dict[str, Any]] = []

    def create_index(self, keys, name=None, **kwargs) -> None:
        self.created_indexes.append({"keys": keys, "name": name, "kwargs": kwargs})


class DummyMetricCollection:
    """Stub for the metric_collection used by get_usage_metrics_aggregated."""

    def __init__(self) -> None:
        self.name = "dummy_metrics"
        self.distinct_calls: List[Dict[str, Any]] = []

    def distinct(self, field: str, filter: Dict[str, Any]) -> List[str]:
        self.distinct_calls.append({"field": field, "filter": filter})
        # We only care that different cutoff timestamps map to different values,
        # not about the filter content itself. So we branch on the presence of timestamp.
        has_timestamp = "timestamp" in filter

        if not has_timestamp and field == "request_id":
            return ["r1", "r2", "r3", "r4"]
        if not has_timestamp and field == "user_id":
            return ["u1", "u2", "u3"]

        # For per-timeframe metrics, just return different fixed sizes.
        if has_timestamp and field == "request_id":
            return ["req_a", "req_b"]
        if has_timestamp and field == "user_id":
            return ["user_x", "user_y"]

        return []


def test_get_usage_metrics_aggregated_basic() -> None:
    request_coll = DummyCollection()
    metric_coll = DummyMetricCollection()

    calc = MongoMetricCalculator(
        collection=cast(Collection, request_coll), metric_collection=cast(Collection, metric_coll)
    )
    cutoffs = {
        "last_1h": 1000.0,
        "last_24h": 2000.0,
    }

    res = calc.get_usage_metrics_aggregated(cutoffs)
    # Top-level totals from the "no timestamp" distinct calls
    assert res["total_requests"] == 4
    assert res["unique_users"] == 3

    # Per-timeframe metrics come from the "has timestamp" calls.
    # We returned 2 request_ids and 2 user_ids for any timestamp filter.
    tf = res["timeframe_metrics"]
    assert tf["last_1h"]["requests"] == 2
    assert tf["last_1h"]["unique_users"] == 2
    assert tf["last_24h"]["requests"] == 2
    assert tf["last_24h"]["unique_users"] == 2
