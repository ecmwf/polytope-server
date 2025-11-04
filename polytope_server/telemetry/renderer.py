from typing import List

from ..common.metric_calculator.base import MetricCalculator
from .telemetry_utils import (
    CANONICAL_LABEL_ORDER,
    TELEMETRY_PRODUCT_LABELS,
    exposition_header,
    histogram_metric_names,
    labels_to_exposition,
    labels_to_exposition_freeform,
    seconds_to_duration_label,
)


def render_counters(metric_calculator: MetricCalculator, winsecs: float) -> List[str]:
    """
    Render counter metrics (requests total, bytes served).

    Args:
        metric_calculator: MetricCalculator instance to fetch metrics from
        winsecs: Time window in seconds

    Returns:
        List of Prometheus exposition format lines
    """
    lines: List[str] = []

    # Requests total
    reqrows = metric_calculator.aggregate_requests_total_window(winsecs)
    lines += exposition_header("polytope_requests_total", "counter", "Requests observed in the sliding window")
    for row in reqrows:
        labels = row["labels"]
        for key in CANONICAL_LABEL_ORDER:
            labels.setdefault(key, "")
        lines.append(f"polytope_requests_total{labels_to_exposition(labels)} {int(row['value'])}")

    # Bytes served
    bytesrows = metric_calculator.aggregate_bytes_served_total_window(winsecs)
    lines += exposition_header("polytope_bytes_served_total", "counter", "Bytes served in the sliding window")
    for row in bytesrows:
        labels = dict(row["labels"])
        order = ["collection", "realm"] + list(TELEMETRY_PRODUCT_LABELS)
        lines.append(f"polytope_bytes_served_total{labels_to_exposition_freeform(labels, order)} {int(row['value'])}")

    return lines


def render_req_duration_hist(metric_calculator: MetricCalculator, winsecs: float) -> List[str]:
    """
    Render request duration histogram metrics.

    Args:
        metric_calculator: MetricCalculator instance to fetch metrics from
        winsecs: Time window in seconds

    Returns:
        List of Prometheus exposition format lines
    """
    lines: List[str] = []
    base, bucketname, sumname, countname = histogram_metric_names("polytope_request_duration_seconds")
    lines += exposition_header(base, "histogram", "End-to-end request duration over the sliding window")

    reqhist = metric_calculator.aggregate_request_duration_histogram(winsecs)

    for row in reqhist["buckets"]:
        labels = dict(row["labels"])
        order = ["status", "collection", "realm"] + list(TELEMETRY_PRODUCT_LABELS) + ["le"]
        lines.append(f"{bucketname}{labels_to_exposition_freeform(labels, order)} {int(row['value'])}")

    for row in reqhist["sum"]:
        labels = dict(row["labels"])
        order = ["status", "collection", "realm"] + list(TELEMETRY_PRODUCT_LABELS)
        lines.append(f"{sumname}{labels_to_exposition_freeform(labels, order)} {row['value']}")

    for row in reqhist["count"]:
        labels = dict(row["labels"])
        order = ["status", "collection", "realm"] + list(TELEMETRY_PRODUCT_LABELS)
        lines.append(f"{countname}{labels_to_exposition_freeform(labels, order)} {int(row['value'])}")

    return lines


def render_proc_hist(metric_calculator: MetricCalculator, winsecs: float) -> List[str]:
    """
    Render processing duration histogram metrics.

    Args:
        metric_calculator: MetricCalculator instance to fetch metrics from
        winsecs: Time window in seconds

    Returns:
        List of Prometheus exposition format lines
    """
    lines: List[str] = []
    base, bucketname, sumname, countname = histogram_metric_names("polytope_processing_seconds")
    lines += exposition_header(
        base,
        "histogram",
        "Processing time (processing→processed) over the sliding window",
    )

    prochist = metric_calculator.aggregate_processing_duration_histogram(winsecs)

    for row in prochist["buckets"]:
        labels = dict(row["labels"])
        order = ["collection", "realm"] + list(TELEMETRY_PRODUCT_LABELS) + ["le"]
        lines.append(f"{bucketname}{labels_to_exposition_freeform(labels, order)} {int(row['value'])}")

    for row in prochist["sum"]:
        labels = dict(row["labels"])
        order = ["collection", "realm"] + list(TELEMETRY_PRODUCT_LABELS)
        lines.append(f"{sumname}{labels_to_exposition_freeform(labels, order)} {row['value']}")

    for row in prochist["count"]:
        labels = dict(row["labels"])
        order = ["collection", "realm"] + list(TELEMETRY_PRODUCT_LABELS)
        lines.append(f"{countname}{labels_to_exposition_freeform(labels, order)} {int(row['value'])}")

    return lines


def render_unique_users(metric_calculator: MetricCalculator, windows_seconds: List[int]) -> List[str]:
    """
    Render unique users gauge metrics.

    Args:
        metric_calculator: MetricCalculator instance to fetch metrics from
        windows_seconds: List of time windows in seconds

    Returns:
        List of Prometheus exposition format lines
    """
    lines: List[str] = []

    uniques = metric_calculator.aggregate_unique_users(windows_seconds)

    lines += exposition_header("polytope_unique_users", "gauge", "Distinct users over common windows")
    for secs in windows_seconds:
        val = int(uniques.get(secs, 0))
        label = seconds_to_duration_label(secs)
        lines.append(f'polytope_unique_users{{window="{label}"}} {val}')

    return lines
