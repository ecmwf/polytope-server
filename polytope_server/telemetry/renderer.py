from typing import List

from .telemetry_utils import (
    CANONICAL_LABEL_ORDER,
    TELEMETRY_PRODUCT_LABELS,
    exposition_header,
    histogram_metric_names,
    labels_to_exposition,
    labels_to_exposition_freeform,
    seconds_to_duration_label,
)


def render_counters(request_store, win_secs: float) -> List[str]:
    lines: List[str] = []
    # Requests
    req_rows = request_store.agg_requests_total_window(win_secs)
    lines += exposition_header("polytope_requests_total", "counter", "Requests observed in the sliding window")
    for row in req_rows:
        labels = row["labels"]
        for key in CANONICAL_LABEL_ORDER:
            labels.setdefault(key, "")
        lines.append(f'polytope_requests_total{labels_to_exposition(labels)} {int(row["value"])}')
    # Bytes
    bytes_rows = request_store.agg_bytes_served_total_window(win_secs)
    lines += exposition_header("polytope_bytes_served_total", "counter", "Bytes served in the sliding window")
    for row in bytes_rows:
        labels = dict(row["labels"])
        order = ["collection", "realm", *TELEMETRY_PRODUCT_LABELS]
        lines.append(f'polytope_bytes_served_total{labels_to_exposition_freeform(labels, order)} {int(row["value"])}')
    return lines


def render_req_duration_hist(request_store, win_secs: float) -> List[str]:
    lines: List[str] = []
    base, bucket_name, sum_name, count_name = histogram_metric_names("polytope_request_duration_seconds")
    lines += exposition_header(base, "histogram", "End-to-end request duration over the sliding window")
    req_hist = request_store.agg_request_duration_histogram(win_secs)
    for row in req_hist["buckets"]:
        labels = dict(row["labels"])
        order = ["status", "collection", "realm", *TELEMETRY_PRODUCT_LABELS, "le"]
        lines.append(f'{bucket_name}{labels_to_exposition_freeform(labels, order)} {int(row["value"])}')
    for row in req_hist["sum"]:
        labels = dict(row["labels"])
        order = ["status", "collection", "realm", *TELEMETRY_PRODUCT_LABELS]
        lines.append(f'{sum_name}{labels_to_exposition_freeform(labels, order)} {row["value"]}')
    for row in req_hist["count"]:
        labels = dict(row["labels"])
        order = ["status", "collection", "realm", *TELEMETRY_PRODUCT_LABELS]
        lines.append(f'{count_name}{labels_to_exposition_freeform(labels, order)} {int(row["value"])}')
    return lines


def render_proc_hist(request_store, win_secs: float) -> List[str]:
    lines: List[str] = []
    base, bucket_name, sum_name, count_name = histogram_metric_names("polytope_processing_seconds")
    lines += exposition_header(base, "histogram", "Processing time (processing->processed) over the sliding window")
    proc_hist = request_store.agg_processing_duration_histogram(win_secs)
    for row in proc_hist["buckets"]:
        labels = dict(row["labels"])
        order = ["collection", "realm", *TELEMETRY_PRODUCT_LABELS, "le"]
        lines.append(f'{bucket_name}{labels_to_exposition_freeform(labels, order)} {int(row["value"])}')
    for row in proc_hist["sum"]:
        labels = dict(row["labels"])
        order = ["collection", "realm", *TELEMETRY_PRODUCT_LABELS]
        lines.append(f'{sum_name}{labels_to_exposition_freeform(labels, order)} {row["value"]}')
    for row in proc_hist["count"]:
        labels = dict(row["labels"])
        order = ["collection", "realm", *TELEMETRY_PRODUCT_LABELS]
        lines.append(f'{count_name}{labels_to_exposition_freeform(labels, order)} {int(row["value"])}')
    return lines


def render_unique_users(request_store, windows_seconds: List[int]) -> List[str]:
    lines: List[str] = []
    uniques = request_store.agg_unique_users(windows_seconds)  # Dict[int,int]
    lines += exposition_header("polytope_unique_users", "gauge", "Distinct users over common windows")
    for secs in windows_seconds:
        val = int(uniques.get(secs, 0))
        label = seconds_to_duration_label(secs)  # "5m", "1h", "1d", "3d"
        lines.append(f'polytope_unique_users{{window="{label}"}} {val}')
    return lines
