# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

"""
Tests for the /chunks/v1 extract path (extract.py) and its dispatch in
run_polytope_worker.process().

pygribjump is replaced at the import boundary (sys.modules) by a fake that
returns deterministic per-field values; no gribjump/FDB/polytope-mars needed.
"""

import itertools
import json
import sys
import types
from pathlib import Path

import numpy as np
import pytest

zstandard = pytest.importorskip("zstandard")

sys.path.insert(0, str(Path(__file__).parent.parent))

import extract  # noqa: E402
import run_polytope_worker  # noqa: E402


# ---------------------------------------------------------------------------
# Fake pygribjump (mirrors the 0.12.0.x API used by extract.py)
# ---------------------------------------------------------------------------


DATES = ["20200101", "20200102"]
TIMES = ["0000", "0600", "1200"]


def field_base(field):
    """Deterministic, field-unique base value derived from the field dict."""
    return float(DATES.index(field["date"]) * 10 + TIMES.index(field["time"]) + 1)


def default_values(field, lo, hi):
    # value at grid index p of a field = base*1e6 + p  (exactly representable)
    return field_base(field) * 1e6 + np.arange(lo, hi, dtype=np.float64)


class FakeExtractionRequest:
    def __init__(self, req, ranges, gridHash=None):
        if not ranges:
            raise ValueError("Must provide at least one range")
        for k, v in req.items():
            assert isinstance(k, str) and isinstance(v, str), (k, v)
        self.req = dict(req)
        self.ranges = list(ranges)
        self.gridHash = gridHash


class FakeResult:
    def __init__(self, values):
        self.values = values


class FakeGribJump:
    def __init__(self, module):
        self.module = module

    def extract(self, requests, ctx=None):
        self.module.calls.append({"requests": requests, "ctx": ctx})
        if self.module.raise_exc is not None:
            raise self.module.raise_exc
        for n, r in enumerate(requests):
            if self.module.missing is not None and self.module.missing(r.req):
                # gribjump stops yielding / has no result for this field
                return
            yield FakeResult(
                [self.module.values_fn(r.req, lo, hi) for lo, hi in r.ranges]
            )


class GribJumpException(RuntimeError):
    pass


class FakePyGribJump:
    """Stands in for the ``pygribjump`` module (only the attributes extract.py uses)."""

    ExtractionRequest = FakeExtractionRequest
    GribJumpException = GribJumpException

    def __init__(self):
        self.calls = []
        self.raise_exc = None
        self.missing = None
        self.values_fn = default_values

    def GribJump(self):  # noqa: N802 - mirrors the pygribjump class name
        return FakeGribJump(self)


@pytest.fixture
def fake_gj(monkeypatch):
    mod = FakePyGribJump()
    monkeypatch.setitem(sys.modules, "pygribjump", mod)  # type: ignore[arg-type]
    extract._reset_gribjump()
    yield mod
    extract._reset_gribjump()


def base_request(**overrides):
    req = {
        "verb": "retrieve",
        "class": "d1",
        "dataset": "climate-dt",
        "activity": "scenariomip",
        "experiment": "ssp3-7.0",
        "generation": "1",
        "model": "ifs-nemo",
        "realization": "1",
        "resolution": "high",
        "expver": "0001",
        "stream": "clte",
        "type": "fc",
        "levtype": "sfc",
        "param": "167",
        # metkit_expansion emits slash-joined strings
        "date": "20200101/20200102",
        "time": "0000/0600/1200",
        "extract": {
            "ranges": [[0, 4]],
            "order": ["date", "time"],
            "grid_hash": "abcdef0123456789",
            "dtype": "float64",
        },
    }
    for k, v in overrides.items():
        if v is None:
            req.pop(k, None)
        else:
            req[k] = v
    return req


def decode(payload):
    raw = zstandard.ZstdDecompressor().decompress(payload)
    return np.frombuffer(raw, dtype="<f8")


# ---------------------------------------------------------------------------
# FIELD_ORDER
# ---------------------------------------------------------------------------


def test_field_order_two_axes_rightmost_fastest():
    spec, values = extract.parse_extract(base_request())
    fields = list(extract.enumerate_fields(values, spec["order"]))
    got = [(f["date"], f["time"]) for f in fields]
    assert got == [
        ("20200101", "0000"),
        ("20200101", "0600"),
        ("20200101", "1200"),
        ("20200102", "0000"),
        ("20200102", "0600"),
        ("20200102", "1200"),
    ]
    # identical to the normative contract expression
    assert got == list(itertools.product(values["date"], values["time"]))
    # values kept as received (no sorting), verb/extract stripped, scalars copied
    for f in fields:
        assert "verb" not in f and "extract" not in f
        assert f["param"] == "167" and f["class"] == "d1"


def test_field_order_respects_order_list_not_request_key_order():
    req = base_request(
        time="1200/0000",
        extract={
            "ranges": [[0, 1]],
            "order": ["time", "date"],
            "grid_hash": None,
            "dtype": "float64",
        },
    )
    spec, values = extract.parse_extract(req)
    got = [(f["time"], f["date"]) for f in extract.enumerate_fields(values, spec["order"])]
    assert got == [
        ("1200", "20200101"),
        ("1200", "20200102"),
        ("0000", "20200101"),
        ("0000", "20200102"),
    ]


def test_list_and_scalar_values_accepted():
    req = base_request(date=["20200101", "20200102"], time="0000", realization=1)
    req["extract"]["order"] = ["date"]
    spec, values = extract.parse_extract(req)
    assert values["date"] == ["20200101", "20200102"]
    assert values["time"] == ["0000"]
    assert values["realization"] == ["1"]


# ---------------------------------------------------------------------------
# Assembly / byte layout
# ---------------------------------------------------------------------------


def test_assembly_byte_layout_single_range(fake_gj):
    req = base_request()
    payload, content_type, timings = extract.run_extract(req)
    assert content_type == "application/octet-stream"

    got = decode(payload)
    expected = []
    for d in ["20200101", "20200102"]:
        for t in ["0000", "0600", "1200"]:
            expected.append(default_values({"date": d, "time": t}, 0, 4))
    expected = np.concatenate(expected)
    assert got.dtype == np.dtype("<f8")
    assert got.shape == (6 * 4,)
    np.testing.assert_array_equal(got, expected)
    # exact bytes too
    assert zstandard.ZstdDecompressor().decompress(payload) == expected.astype("<f8").tobytes()
    assert timings["fields"] == 6 and timings["raw_bytes"] == 6 * 4 * 8

    # one gribjump call with one single-field request per field, in order
    (call,) = fake_gj.calls
    reqs = call["requests"]
    assert [(r.req["date"], r.req["time"]) for r in reqs] == list(
        itertools.product(["20200101", "20200102"], ["0000", "0600", "1200"])
    )
    for r in reqs:
        assert r.ranges == [(0, 4)]
        assert r.gridHash == "abcdef0123456789"
        assert "verb" not in r.req and "extract" not in r.req
        assert all("/" not in v for v in r.req.values())


def test_multi_range_concatenation_within_field(fake_gj):
    req = base_request(date="20200101/20200102", time="0000")
    req["extract"] = {
        "ranges": [[10, 13], [2, 4], [100, 101]],
        "order": ["date"],
        "grid_hash": None,
        "dtype": "float64",
    }
    payload, _, _ = extract.run_extract(req)
    got = decode(payload)
    expected = []
    for d in ["20200101", "20200102"]:
        f = {"date": d, "time": "0000"}
        # payload order, not sorted
        expected += [
            default_values(f, 10, 13),
            default_values(f, 2, 4),
            default_values(f, 100, 101),
        ]
    np.testing.assert_array_equal(got, np.concatenate(expected))
    assert got.size == 2 * (3 + 2 + 1)
    assert fake_gj.calls[0]["requests"][0].gridHash is None
    assert fake_gj.calls[0]["requests"][0].ranges == [(10, 13), (2, 4), (100, 101)]


def test_nan_values_pass_through(fake_gj):
    fake_gj.values_fn = lambda f, lo, hi: np.full(hi - lo, np.nan)
    req = base_request(date="20200101", time="0000")
    req["extract"]["order"] = []
    payload, _, _ = extract.run_extract(req)
    got = decode(payload)
    assert got.shape == (4,) and np.isnan(got).all()


# ---------------------------------------------------------------------------
# Validation / failure
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "mutate, match",
    [
        (lambda r: r["extract"].update(dtype="float32"), "dtype"),
        (lambda r: r["extract"].pop("dtype"), "dtype"),
        (lambda r: r["extract"].update(ranges=[]), "ranges"),
        (lambda r: r["extract"].update(ranges=[[5, 5]]), "lo < hi"),
        (lambda r: r["extract"].update(ranges=[[6, 5]]), "lo < hi"),
        (lambda r: r["extract"].update(ranges=[[-1, 5]]), "lo < hi"),
        (lambda r: r["extract"].update(ranges=[[0, 1.5]]), "integer"),
        (lambda r: r["extract"].update(ranges=[[0, 1, 2]]), "pair"),
        (lambda r: r["extract"].update(order=["date", "time", "step"]), "not present"),
        (lambda r: r["extract"].update(order=["date"]), r"\['time'\].*not listed"),
        (lambda r: r.update(param="167/168"), r"\['param'\].*not listed"),
        (lambda r: r["extract"].update(order=["date", "date"]), "duplicate"),
        (lambda r: r.update(feature={"type": "polygon"}), "object"),
        (lambda r: r["extract"].update(grid_hash=5), "grid_hash"),
        (lambda r: r["extract"].update(bogus=1), "unknown"),
    ],
)
def test_validation_errors(fake_gj, mutate, match):
    req = base_request()
    mutate(req)
    with pytest.raises(extract.ExtractError, match=match):
        extract.run_extract(req)
    assert fake_gj.calls == []


def test_missing_field_fails_job(fake_gj):
    fake_gj.missing = lambda f: f["date"] == "20200102" and f["time"] == "0600"
    with pytest.raises(extract.ExtractError, match=r"returned 4 of 6 fields.*date=20200102.*time=0600"):
        extract.run_extract(base_request())


def test_value_count_mismatch_fails_job(fake_gj):
    fake_gj.values_fn = lambda f, lo, hi: np.zeros(hi - lo - 1)
    with pytest.raises(extract.ExtractError, match="returned 3 values, expected 4"):
        extract.run_extract(base_request())


def test_range_count_mismatch_fails_job(fake_gj, monkeypatch):
    orig = FakeGribJump.extract

    def short(self, requests, ctx=None):
        for r in orig(self, requests, ctx):
            yield FakeResult(r.values[:-1])

    real = FakeGribJump(fake_gj)
    monkeypatch.setattr(
        fake_gj,
        "GribJump",
        lambda: types.SimpleNamespace(extract=lambda reqs, ctx=None: short(real, reqs, ctx)),
    )
    req = base_request()
    req["extract"]["ranges"] = [[0, 1], [2, 3]]
    with pytest.raises(extract.ExtractError, match="returned 1 ranges, expected 2"):
        extract.run_extract(req)


def test_gribjump_exception_fails_job(fake_gj):
    fake_gj.raise_exc = GribJumpException("grid hash mismatch for field")
    with pytest.raises(extract.ExtractError, match="gribjump extraction failed: grid hash mismatch"):
        extract.run_extract(base_request())


# ---------------------------------------------------------------------------
# Dispatch in run_polytope_worker.process()
# ---------------------------------------------------------------------------


class RecordingDatasource:
    def __init__(self):
        self.retrieved = []

    def retrieve(self, request):
        self.retrieved.append(request.coerced_request)
        return {"retrieve_ms": 1.0}

    def result(self, request):
        return [b'{"type": "CoverageCollection"}']

    def destroy(self, request):
        pass

    def mime_type(self):
        return "application/prs.coverage+json"


@pytest.fixture
def recording_ds(monkeypatch):
    ds = RecordingDatasource()
    monkeypatch.setattr(run_polytope_worker, "_get_datasource", lambda path: ds)
    if hasattr(run_polytope_worker._log_buffer, "records"):
        delattr(run_polytope_worker._log_buffer, "records")
    return ds


def _payload(request):
    return json.dumps(
        {
            "request": request,
            "user": {"realm": "ecmwf", "username": "tester"},
            "metadata": {},
            "config_path": "/tmp/unused.yaml",
        }
    )


def test_dispatch_extract_path(fake_gj, recording_ds):
    body, status_json = run_polytope_worker.process(_payload(base_request()))
    status = json.loads(status_json)
    assert status["ok"] is True, status
    assert status["error"] is None
    assert status["content_type"] == "application/octet-stream"
    assert recording_ds.retrieved == []  # PolytopeMars path not used
    assert decode(body).size == 6 * 4
    assert fake_gj.calls[0]["ctx"] == {"user": "ecmwf:tester"}
    assert "total_ms" in status["timings"]


def test_dispatch_legacy_path_untouched(fake_gj, recording_ds):
    req = base_request(extract=None)
    req["feature"] = {"type": "timeseries"}
    body, status_json = run_polytope_worker.process(_payload(req))
    status = json.loads(status_json)
    assert status["ok"] is True, status
    assert status["content_type"] == "application/prs.coverage+json"
    assert body == b'{"type": "CoverageCollection"}'
    assert recording_ds.retrieved == [req]
    assert fake_gj.calls == []


def test_dispatch_non_object_extract_key_is_legacy(fake_gj, recording_ds):
    req = base_request(extract="yes")
    _, status_json = run_polytope_worker.process(_payload(req))
    assert json.loads(status_json)["content_type"] == "application/prs.coverage+json"
    assert len(recording_ds.retrieved) == 1
    assert fake_gj.calls == []


def test_dispatch_extract_error_reports_job_failure(fake_gj, recording_ds):
    req = base_request()
    req["extract"]["dtype"] = "float32"
    body, status_json = run_polytope_worker.process(_payload(req))
    status = json.loads(status_json)
    assert status["ok"] is False
    assert body == b""
    assert "extract.dtype" in status["error"]["message"]
    assert recording_ds.retrieved == []
    assert "content_type" not in status
