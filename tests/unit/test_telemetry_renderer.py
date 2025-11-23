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

from typing import Any, Dict, List

from polytope_server.common.metric_calculator.base import MetricCalculator
from polytope_server.telemetry.renderer import render_counters, render_unique_users
from polytope_server.telemetry.telemetry_utils import METRIC_PREFIX


class DummyMetricCalculator(MetricCalculator):
    def ensure_indexes(self) -> None:
        pass

    def ensure_metric_indexes(self) -> None:
        pass

    def aggregate_requests_total_window(self, window_seconds: float) -> List[Dict[str, Any]]:
        # minimal single row
        return [
            {
                "labels": {
                    "collection": "destination-earth",
                    "realm": "ecmwf",
                    "class": "d1",
                    "type": "fc",
                    "expver": "0001",
                    "status": "processed",
                },
                "value": 3,
            }
        ]

    def aggregate_bytes_served_total_window(self, window_seconds: float) -> List[Dict[str, Any]]:
        return [
            {
                "labels": {
                    "collection": "destination-earth",
                    "realm": "ecmwf",
                    "class": "d1",
                    "type": "fc",
                    "expver": "0001",
                },
                "value": 1024,
            }
        ]

    def aggregate_request_duration_histogram(self, window_seconds: float):
        raise NotImplementedError

    def aggregate_processing_duration_histogram(self, window_seconds: float):
        raise NotImplementedError

    def get_usage_metrics_aggregated(self, cutoff_timestamps):
        raise NotImplementedError

    def aggregate_unique_users(self, windows_seconds):
        # map each window to a fixed value, e.g. 5
        return {w: 5 for w in windows_seconds}

    def list_requests(self, *args, **kwargs):
        raise NotImplementedError

    def list_requests_by_user(self, *args, **kwargs):
        raise NotImplementedError


def test_render_counters_output_shape():
    calc = DummyMetricCalculator()
    lines = render_counters(calc, winsecs=300)

    # Should contain HELP/TYPE and the metrics themselves
    joined = "\n".join(lines)
    assert f"# HELP {METRIC_PREFIX}_requests_total" in joined
    assert f"# TYPE {METRIC_PREFIX}_requests_total gauge" in joined
    assert f"{METRIC_PREFIX}_requests_total" in joined
    assert f"{METRIC_PREFIX}_bytes_served_total" in joined
    assert "1024" in joined  # bytes value


def test_render_unique_users_output():
    calc = DummyMetricCalculator()
    windows = [300, 3600]
    lines = render_unique_users(calc, windows)

    joined = "\n".join(lines)
    assert f"# HELP {METRIC_PREFIX}_unique_users" in joined
    assert 'window="5m"' in joined or 'window="300s"' in joined
    assert " 5" in joined  # the value we returned in DummyMetricCalculator


def test_render_counters_empty_results():
    """If aggregators return empty lists, we still want HELP/TYPE headers and no crash."""

    class EmptyMetricCalculator(DummyMetricCalculator):
        def aggregate_requests_total_window(self, window_seconds: float) -> List[Dict[str, Any]]:
            return []

        def aggregate_bytes_served_total_window(self, window_seconds: float) -> List[Dict[str, Any]]:
            return []

    calc = EmptyMetricCalculator()
    lines = render_counters(calc, winsecs=300)
    joined = "\n".join(lines)

    assert f"# HELP {METRIC_PREFIX}_requests_total" in joined
    assert f"# HELP {METRIC_PREFIX}_bytes_served_total" in joined
    # but no sample line with numeric value should be present
    assert f"{METRIC_PREFIX}_requests_total" in joined  # name exists
    # quick heuristic: there should be no "requests_total{" with value
    assert f"{METRIC_PREFIX}_requests_total{{" not in joined
