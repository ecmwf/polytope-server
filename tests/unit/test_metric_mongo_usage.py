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
from typing import cast

import mongomock
from pymongo.collection import Collection

from polytope_server.common.metric_calculator.mongo import MongoMetricCalculator


def test_get_usage_metrics_aggregated_basic() -> None:
    client = mongomock.MongoClient()
    db = client.testdb
    request_coll = db.requests
    metric_coll = db.metrics

    # Populate metric collection
    # We need: type="request_status_change", status="processed", timestamp, request_id, user_id

    # Cutoffs: last_1h -> 10000, last_24h -> 5000

    docs = [
        # Inside last_1h (>= 10000)
        {
            "type": "request_status_change",
            "status": "processed",
            "timestamp": 12000,
            "request_id": "r1",
            "user_id": "u1",
        },
        {
            "type": "request_status_change",
            "status": "processed",
            "timestamp": 12000,
            "request_id": "r2",
            "user_id": "u1",
        },
        {
            "type": "request_status_change",
            "status": "processed",
            "timestamp": 11000,
            "request_id": "r3",
            "user_id": "u2",
        },
        # Inside last_24h (>= 5000) but not last_1h
        {
            "type": "request_status_change",
            "status": "processed",
            "timestamp": 8000,
            "request_id": "r4",
            "user_id": "u1",
        },
        {
            "type": "request_status_change",
            "status": "processed",
            "timestamp": 6000,
            "request_id": "r5",
            "user_id": "u3",
        },
        # Older (< 5000)
        {
            "type": "request_status_change",
            "status": "processed",
            "timestamp": 1000,
            "request_id": "r6",
            "user_id": "u4",
        },
        # Ignored docs (wrong type or status)
        {
            "type": "other",
            "status": "processed",
            "timestamp": 12000,
            "request_id": "r_bad1",
            "user_id": "u1",
        },
        {
            "type": "request_status_change",
            "status": "failed",
            "timestamp": 12000,
            "request_id": "r_bad2",
            "user_id": "u1",
        },
        # duplicate processed for same request (should not happen, but test anyway)
        {
            "type": "request_status_change",
            "status": "processed",
            "timestamp": 12000,
            "request_id": "r1",
            "user_id": "u1",
        },
    ]
    metric_coll.insert_many(docs)

    calc = MongoMetricCalculator(
        collection=cast(Collection, request_coll), metric_collection=cast(Collection, metric_coll)
    )
    cutoffs = {
        "last_1h": 10000.0,
        "last_24h": 5000.0,
    }

    res = calc.get_usage_metrics_aggregated(cutoffs)

    # Total requests: 7 valid processed requests (including duplicate)
    assert res["total_requests"] == 7
    # Unique users: u1, u2, u3, u4 -> 4
    assert res["unique_users"] == 4

    tf = res["timeframe_metrics"]

    # last_1h (>= 10000): r1, r2, r3, r1(dup) -> 4 requests. u1, u2 -> 2 users.
    assert tf["last_1h"]["requests"] == 4
    assert tf["last_1h"]["unique_users"] == 2

    # last_24h (>= 5000): r1..r5 + r1(dup) -> 6 requests. u1, u2, u3 -> 3 users.
    assert tf["last_24h"]["requests"] == 6
    assert tf["last_24h"]["unique_users"] == 3
