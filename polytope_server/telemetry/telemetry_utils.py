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
from typing import Any, List, Mapping, Optional, Tuple

from .config import config

(
    METRIC_PREFIX,
    TELEMETRY_PRODUCT_LABELS,
    CANONICAL_LABEL_ORDER,
    REQUEST_DURATION_BUCKETS,
    PROCESSING_DURATION_BUCKETS,
) = config.get_metrics_config()


def now_utc_ts() -> float:
    return datetime.datetime.now(datetime.timezone.utc).timestamp()


def parse_window(window: Optional[str], default_seconds: float = 300.0) -> float:
    if not window:
        return default_seconds
    try:
        unit = window[-1].lower() if window[-1].isalpha() else "s"
        num = window[:-1] if window[-1].isalpha() else window
        val = float(num)
        mult = {"s": 1, "m": 60, "h": 3600, "d": 86400}.get(unit, 1)
        return max(1.0, val * mult)
    except Exception:
        return default_seconds


def seconds_to_duration_label(seconds: int) -> str:
    if seconds % 86400 == 0:
        return f"{seconds // 86400}d"
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds % 60 == 0:
        return f"{seconds // 60}m"
    return f"{seconds}s"


def sanitize_label_value(value: Any) -> str:
    if value is None:
        value = ""
    sval = str(value)
    return sval.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def labels_to_exposition(labels: Mapping[str, Any]) -> str:
    # Canonical order with defaults; fine when all canonical keys are expected.
    parts: List[str] = []
    for key in CANONICAL_LABEL_ORDER:
        sval = sanitize_label_value(labels.get(key, ""))
        parts.append(f'{key}="{sval}"')
    return "{" + ",".join(parts) + "}"


def labels_to_exposition_freeform(labels: Mapping[str, Any], order: Optional[List[str]] = None) -> str:
    keys = order or list(labels.keys())
    return "{" + ",".join(f'{k}="{sanitize_label_value(labels.get(k, ""))}"' for k in keys) + "}"


def exposition_header(name: str, mtype: str, help_text: str) -> List[str]:
    return [f"# HELP {name} {help_text}", f"# TYPE {name} {mtype}"]


def histogram_metric_names(base: str) -> Tuple[str, str, str, str]:
    return base, f"{base}_bucket", f"{base}_sum", f"{base}_count"


def build_product_labels(doc: Mapping[str, Any]) -> Mapping[str, Any]:
    labels: dict[str, Any] = {
        "collection": doc.get("collection", ""),
        "realm": (((doc.get("user") or {}) or {}).get("realm")) if doc.get("user") else "",
        "datasource": doc.get("datasource", ""),
    }
    cr = doc.get("cr") or doc.get("coerced_request") or {}
    for k in TELEMETRY_PRODUCT_LABELS:
        labels[k] = cr.get(k, "")
    return labels
