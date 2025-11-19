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

from prometheus_client import CollectorRegistry, generate_latest

from polytope_server.telemetry.helpers import (
    prepare_aggregated_json_metrics,
    set_aggregated_prometheus_metrics,
)
from polytope_server.telemetry.telemetry_helpers import (
    build_product_labels,
    parse_window,
    seconds_to_duration_label,
)
from polytope_server.telemetry.telemetry_utils import (
    METRIC_PREFIX,
    TELEMETRY_PRODUCT_LABELS,
)


def test_parse_window_basic_units():
    # plain number -> seconds
    assert parse_window("5") == 5.0
    assert parse_window("5s") == 5.0

    # minutes / hours / days
    assert parse_window("2m") == 2 * 60
    assert parse_window("1h") == 3600
    assert parse_window("1d") == 86400

    # default fallback for invalid input
    assert parse_window("nonsense", default_seconds=123) == 123.0


def test_seconds_to_duration_label():
    assert seconds_to_duration_label(5) == "5s"
    assert seconds_to_duration_label(60) == "1m"
    assert seconds_to_duration_label(3600) == "1h"
    # something that doesn’t fit exactly – we still want a reasonable label
    label = seconds_to_duration_label(90)
    assert label in {"90s", "1m", "2m"}  # depending on your exact implementation


def test_build_product_labels_uses_telemetry_product_labels():
    """
    Ensure build_product_labels picks up values from:
      - collection
      - user.realm
      - coerced_request.<product_label> for each TELEMETRY_PRODUCT_LABELS entry
    """
    doc = {
        "collection": "destination-earth",
        "user": {
            "realm": "ecmwf",
        },
        "coerced_request": {k: f"value-{k}" for k in TELEMETRY_PRODUCT_LABELS},
    }

    labels = build_product_labels(doc)

    assert labels["collection"] == "destination-earth"
    assert labels["realm"] == "ecmwf"
    for k in TELEMETRY_PRODUCT_LABELS:
        assert labels[k] == f"value-{k}"


def test_prepare_aggregated_json_metrics_shape():
    metrics = {
        "total_requests": 10,
        "unique_users": 3,
        "timeframe_metrics": {
            "last_5m": {"requests": 4, "unique_users": 2},
            "last_1h": {"requests": 6, "unique_users": 3},
        },
    }
    time_frames = [
        {"name": "last_5m"},
        {"name": "last_1h"},
    ]

    res = prepare_aggregated_json_metrics(metrics, time_frames)

    assert res["total_requests"] == 10
    assert res["unique_users"] == 3
    assert res["time_frames"]["last_5m"]["requests"] == 4
    assert res["time_frames"]["last_5m"]["unique_users"] == 2
    assert res["time_frames"]["last_1h"]["requests"] == 6
    assert res["time_frames"]["last_1h"]["unique_users"] == 3


def test_set_aggregated_prometheus_metrics_produces_gauges():
    """
    Basic smoke test: make sure Prometheus metrics can be rendered and include
    our prefix + values.
    """
    metrics = {
        "total_requests": 42,
        "unique_users": 7,
        "timeframe_metrics": {
            "last_5m": {"requests": 10, "unique_users": 2},
        },
    }
    time_frames = [
        {
            "name": "last_5m",
            "request_metric_name": f"{METRIC_PREFIX}_requests_last_5m",
            "request_metric_description": "Requests last 5m",
            "user_metric_name": f"{METRIC_PREFIX}_users_last_5m",
            "user_metric_description": "Users last 5m",
        }
    ]

    registry = CollectorRegistry()
    set_aggregated_prometheus_metrics(registry, metrics, time_frames)

    output = generate_latest(registry).decode("utf-8")

    assert f"{METRIC_PREFIX}_total_requests" in output
    assert "42.0" in output or "42 " in output
    assert f"{METRIC_PREFIX}_unique_users" in output
    assert f"{METRIC_PREFIX}_requests_last_5m" in output
    assert f"{METRIC_PREFIX}_users_last_5m" in output
