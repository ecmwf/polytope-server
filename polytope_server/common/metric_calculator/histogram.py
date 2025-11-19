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

from collections import defaultdict
from typing import Any, Dict, List, Tuple


class HistogramBuilder:
    """
    Shared histogram building logic for all implementations.
    Works on pre-aggregated data to build Prometheus-style histograms.
    """

    @staticmethod
    def build_histogram(
        rows: List[Dict[str, Any]],
        buckets: List[float],
        duration_key: str,
        include_status: bool,
        product_labels: List[str],
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Build histogram data structure from aggregated rows.

        Args:
            rows: Aggregated rows from database
            buckets: List of bucket boundaries (floats)
            duration_key: Key name for duration value in rows
            include_status: Whether to include status in label keys
            product_labels: List of product label keys

        Returns:
            Dict with 'buckets', 'sum', and 'count' histogram components
        """
        bnds = buckets + [float("inf")]

        def le_str(b: float) -> str:
            return "Inf" if b == float("inf") else str(b)

        # Track sums, counts, and raw durations per label combination
        sum_map: Dict[Tuple[Any, ...], float] = defaultdict(float)
        cnt_map: Dict[Tuple[Any, ...], int] = defaultdict(int)
        durations_map: Dict[Tuple[Any, ...], List[float]] = defaultdict(list)

        for r in rows:
            ds = r.get("datasource", "")
            # Build label ID tuple
            lid: Tuple[Any, ...]
            if include_status:
                lid = (
                    r["status"],
                    r["collection"],
                    ds,
                    r.get("realm", ""),
                    tuple(r.get("cr", {}).get(k, "") for k in product_labels),
                )
            else:
                lid = (
                    r["collection"],
                    ds,
                    r.get("realm", ""),
                    tuple(r.get("cr", {}).get(k, "") for k in product_labels),
                )

            dur = float(r.get(duration_key, 0.0))

            durations_map[lid].append(dur)
            sum_map[lid] += dur
            cnt_map[lid] += 1

        # Build output structure
        buckets_rows: List[Dict[str, Any]] = []
        sum_rows: List[Dict[str, Any]] = []
        count_rows: List[Dict[str, Any]] = []

        for lid in durations_map.keys():
            # Unpack label tuple
            base: Dict[str, Any]
            if include_status:
                status, collection, datasource, realm, prod = lid
                prodmap = dict(zip(product_labels, prod))
                base = {
                    "status": status,
                    "collection": collection,
                    "datasource": datasource,
                    "realm": realm,
                    **prodmap,
                }
            else:
                collection, datasource, realm, prod = lid
                prodmap = dict(zip(product_labels, prod))
                base = {"collection": collection, "datasource": datasource, "realm": realm, **prodmap}

            # Calculate cumulative bucket counts
            durations = durations_map[lid]
            for b in bnds:
                count = sum(1 for d in durations if d <= b)
                buckets_rows.append(
                    {
                        "labels": {"le": le_str(b), **base},
                        "value": count,
                    }
                )

            # Emit sum and count
            sum_rows.append({"labels": base, "value": sum_map[lid]})
            count_rows.append({"labels": base, "value": cnt_map[lid]})

        return {"buckets": buckets_rows, "sum": sum_rows, "count": count_rows}
