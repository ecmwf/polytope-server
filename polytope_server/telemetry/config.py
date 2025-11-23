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
from typing import Any, Dict, List, Tuple

from ..common.config import ConfigParser


class Config:
    def __init__(self, allow_empty: bool | None = None):
        """
        Lightweight wrapper around ConfigParser for telemetry.

        - In normal runtime (no pytest), any config/schema error should abort
        - In tests (pytest), we allow an empty config so telemetry modules
          can be imported without requiring a real config file.
        """
        # Auto-detect test environment if not explicitly specified
        if allow_empty is None:
            allow_empty = "pytest" in sys.modules

        self._allow_empty = allow_empty

        try:
            self.config: Dict[str, Any] = ConfigParser().read()
        except SystemExit:
            # ConfigParser uses sys.exit(1) on schema/validation failure.
            if self._allow_empty:
                # In tests: just use an empty config.
                self.config = {}
            else:
                raise
        except Exception:
            # Any other error (e.g. no config files) – same policy.
            if self._allow_empty:
                self.config = {}
            else:
                raise

    def get(self, section: str, default=None):
        """Retrieve a section from the config or return a default value."""
        return (self.config or {}).get(section, default)

    def get_metrics_config(
        self,
    ) -> Tuple[str, Tuple[str, ...], Tuple[str, ...], List[float], List[float]]:
        """
        Read telemetry metrics configuration (labels, buckets) with safe fallbacks.

        Returns:
            (
                prefix,
                product_labels,
                canonical_label_order,
                request_buckets,
                processing_buckets,
            )
        """
        metrics_cfg = self.get("metrics", {}) or {}

        # Prefix for *all* telemetry metrics (requests_total, bytes_served_total, usage gauges, etc.)
        prefix = metrics_cfg.get("prefix", "polytope")

        # Product labels taken from coerced_request
        product_labels_cfg = metrics_cfg.get("product_labels", ["class", "type"])
        if not isinstance(product_labels_cfg, (list, tuple)):
            product_labels_cfg = ["class", "type"]
        product_labels = tuple(str(label) for label in product_labels_cfg)

        # Canonical label order for counters; if not provided, we derive it
        canonical_cfg = metrics_cfg.get("canonical_label_order")
        if isinstance(canonical_cfg, (list, tuple)):
            canonical_label_order = tuple(str(label) for label in canonical_cfg)
        else:
            canonical_label_order = ("status", "collection", "datasource", "realm", *product_labels)

        # Histogram buckets
        def _ensure_bucket_list(key: str, default: List[float]) -> List[float]:
            raw = metrics_cfg.get(key, default)
            if not isinstance(raw, (list, tuple)):
                return default
            out: List[float] = []
            for v in raw:
                try:
                    out.append(float(v))
                except Exception:
                    continue
            return out or default

        request_buckets = _ensure_bucket_list(
            "request_duration_buckets",
            [0.5, 1, 2, 5, 10, 20, 30, 60, 120, 300],
        )
        processing_buckets = _ensure_bucket_list(
            "processing_duration_buckets",
            [0.5, 1, 2, 5, 10, 20, 30, 60, 120, 300],
        )

        return (
            prefix,
            product_labels,
            canonical_label_order,
            request_buckets,
            processing_buckets,
        )


# Global config instance
config = Config()
