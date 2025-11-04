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

        def pick_bucket(v: float) -> float:
            for b in bnds:
                if v < b:
                    return b
            return float("inf")

        # Track buckets, sums, and counts per label combination
        # Use Union type to allow both tuple shapes
        bucket_out: Dict[Tuple[Any, ...], Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        sum_map: Dict[Tuple[Any, ...], float] = defaultdict(float)
        cnt_map: Dict[Tuple[Any, ...], int] = defaultdict(int)

        for r in rows:
            # Build label ID tuple
            lid: Tuple[Any, ...]
            if include_status:
                lid = (
                    r["status"],
                    r["collection"],
                    r.get("realm", ""),
                    tuple(r.get("cr", {}).get(k, "") for k in product_labels),
                )
            else:
                lid = (
                    r["collection"],
                    r.get("realm", ""),
                    tuple(r.get("cr", {}).get(k, "") for k in product_labels),
                )

            dur = float(r.get(duration_key, 0.0))
            b = pick_bucket(dur)

            bucket_out[lid][le_str(b)] += 1
            sum_map[lid] += dur
            cnt_map[lid] += 1

        # Build output structure
        buckets_rows: List[Dict[str, Any]] = []
        sum_rows: List[Dict[str, Any]] = []
        count_rows: List[Dict[str, Any]] = []

        for lid, le_counts in bucket_out.items():
            # Unpack label tuple
            base: Dict[str, Any]
            if include_status:
                status, collection, realm, prod = lid
                prodmap = dict(zip(product_labels, prod))
                base = {
                    "status": status,
                    "collection": collection,
                    "realm": realm,
                    **prodmap,
                }
            else:
                collection, realm, prod = lid
                prodmap = dict(zip(product_labels, prod))
                base = {"collection": collection, "realm": realm, **prodmap}

            # Emit bucket counts
            for b in bnds:
                key = le_str(b)
                buckets_rows.append(
                    {
                        "labels": {"le": key, **base},
                        "value": le_counts.get(key, 0),
                    }
                )

            # Emit sum and count
            sum_rows.append({"labels": base, "value": sum_map[lid]})
            count_rows.append({"labels": base, "value": cnt_map[lid]})

        return {"buckets": buckets_rows, "sum": sum_rows, "count": count_rows}
