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

from polytope_server.common.metric_calculator.histogram import HistogramBuilder


def test_histogram_builder_basic():
    """
    Simple sanity check: two rows, one short and one long duration, over buckets.
    """
    rows = [
        {
            "status": "processed",
            "collection": "colA",
            "realm": "ecmwf",
            "cr": {"class": "d1", "type": "fc"},
            "datasource": "mars",
            "duration": 0.5,
        },
        {
            "status": "processed",
            "collection": "colA",
            "realm": "ecmwf",
            "cr": {"class": "d1", "type": "fc"},
            "datasource": "mars",
            "duration": 3.0,
        },
    ]

    buckets = [1.0, 2.0, 5.0]  # typical prom-style le buckets
    product_labels = ["class", "type"]

    builder = HistogramBuilder()
    hist = builder.build_histogram(
        rows=rows,
        buckets=buckets,
        duration_key="duration",
        include_status=True,
        product_labels=product_labels,
    )

    # We expect:
    #   - sum: 0.5 + 3.0 = 3.5
    #   - count: 2
    #   - bucket counts spread across <=1, <=2, <=5, +Inf
    sums = hist["sum"]
    counts = hist["count"]
    buckets_rows = hist["buckets"]

    # There should be exactly one label combination
    assert len(sums) == 1
    assert len(counts) == 1

    base_labels = {
        "status": "processed",
        "collection": "colA",
        "realm": "ecmwf",
        "class": "d1",
        "type": "fc",
        "datasource": "mars",
    }

    assert sums[0]["labels"] == base_labels
    assert counts[0]["labels"] == base_labels
    assert sums[0]["value"] == 3.5
    assert counts[0]["value"] == 2

    # Check buckets: we should get len(buckets)+1 entries for this label set
    # le=1.0 -> 1 sample    (0.5)
    # le=2.0 -> still 1     (0.5 < 2, 3.0 >= 2)
    # le=5.0 -> 2 samples   (both < 5)
    # le=Inf -> 2 samples   (Prom-style cumulative)
    # We don't check the exact ordering, but we check le and value pairs.
    by_le = {row["labels"]["le"]: row["value"] for row in buckets_rows}

    assert by_le["1.0"] == 1
    assert by_le["2.0"] == 1
    assert by_le["5.0"] == 2
    assert by_le["Inf"] == 2
