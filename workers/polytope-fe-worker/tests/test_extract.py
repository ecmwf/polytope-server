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
import logging
import re
import sys
import threading
import time
import types
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pytest

zstandard = pytest.importorskip("zstandard")

sys.path.insert(0, str(Path(__file__).parent.parent))

import extract  # noqa: E402
import location_cache  # type: ignore[import-not-found]  # noqa: E402
import run_polytope_worker  # noqa: E402


# ---------------------------------------------------------------------------
# Fake pygribjump (mirrors the 0.12.0.x API used by extract.py)
# ---------------------------------------------------------------------------


DATES = ["20200101", "20200102"]
TIMES = ["0000", "0600", "1200"]
DATE_BASES = {"19900101": -10.0, "20200101": 0.0, "20200102": 10.0}
TIME_BASES = {"0000": 1.0, "0600": 2.0, "1200": 3.0}


def field_base(field):
    """Deterministic, field-unique base value derived from the field dict."""
    return DATE_BASES[field["date"]] + TIME_BASES[field["time"]]


def default_values(field, lo, hi):
    # value at grid index p of a field = base*1e6 + p  (exactly representable)
    return field_base(field) * 1e6 + np.arange(lo, hi, dtype=np.float64)


class FakeExtractionRequest:
    def __init__(self, req, ranges, gridHash=None):
        if not ranges:
            raise ValueError("Must provide at least one range")
        for k, v in req.items():
            assert isinstance(k, str) and isinstance(v, str), f"{k!r}={v!r}"
        self.req = dict(req)
        self.ranges = list(ranges)
        self.gridHash = gridHash


class FakePathExtractionRequest:
    def __init__(self, path, scheme, offset, host, port, ranges, gridHash=None):
        self.path = path
        self.scheme = scheme
        self.offset = offset
        self.host = host
        self.port = port
        self.ranges = list(ranges)
        self.gridHash = gridHash


class FakeResult:
    def __init__(self, values):
        self.values = values


class FakeGribJump:
    def __init__(self, module):
        self.module = module
        self.handle_id = len(module.handles)
        module.handles.append(self)

    def extract(self, requests, ctx=None):
        self.module.calls.append(
            {
                "requests": requests,
                "ctx": ctx,
                "handle": self,
                "thread": threading.get_ident(),
            }
        )
        if self.module.delay:
            time.sleep(self.module.delay)
        if isinstance(self.module.raise_exc, list):
            exc = self.module.raise_exc.pop(0) if self.module.raise_exc else None
        else:
            exc = self.module.raise_exc
        if exc is not None:
            raise exc
        for r in requests:
            if self.module.missing is not None and self.module.missing(r.req):
                return
            yield FakeResult(
                [self.module.values_fn(r.req, lo, hi) for lo, hi in r.ranges]
            )

    def extract_from_paths(self, requests, ctx=None):
        self.module.path_calls.append(
            {
                "requests": requests,
                "ctx": ctx,
                "handle": self,
                "thread": threading.get_ident(),
            }
        )
        if self.module.delay:
            time.sleep(self.module.delay)
        for request in requests:
            failures = self.module.path_failures.get(request.path, 0)
            if failures:
                self.module.path_failures[request.path] = failures - 1
                raise GribJumpException(f"stale location {request.path}")
            field = self.module.path_fields[request.path]
            yield FakeResult(
                [self.module.values_fn(field, lo, hi) for lo, hi in request.ranges]
            )


class GribJumpException(RuntimeError):
    pass


class FakePyGribJump:
    """Stands in for the pygribjump 0.12 module."""

    ExtractionRequest = FakeExtractionRequest
    PathExtractionRequest = FakePathExtractionRequest
    GribJumpException = GribJumpException

    def __init__(self):
        self.calls = []
        self.path_calls = []
        self.path_fields = {}
        self.path_failures = {}
        self.raise_exc = None
        self.missing = None
        self.values_fn = default_values
        self.delay = 0.0
        self.handles = []

    def GribJump(self):  # noqa: N802 - mirrors the pygribjump class name
        return FakeGribJump(self)


class FakeURI:
    def __init__(
        self,
        path: str,
        scheme: str = "fdb",
        host: str | None = "store.example",
        port: int | None = 9000,
        query: str = "",
    ):
        self._path = path
        self._scheme = scheme
        self._host = host
        self._port = port
        self._query = query

    def path(self):
        return self._path

    def scheme(self):
        return self._scheme

    def hostname(self):
        return self._host

    def port(self):
        return self._port

    def query(self):
        return self._query


class FakeListElement:
    def __init__(self, field, location, number_of_data_points):
        self._field = dict(field)
        self._location = location
        self._number_of_data_points = number_of_data_points
        self.uri = FakeURI(
            location.path, location.scheme, location.host, location.port
        )

    def has_location(self):
        return True

    def offset(self):
        return self._location.offset

    def length(self):
        return self._location.length

    def number_of_data_points(self):
        return self._number_of_data_points

    def combined_key(self):
        field = dict(self._field)
        date = field.get("date", "")
        field.update(year=date[:4], month=date[4:6].lstrip("0"))
        return field


class FakePyFDB:
    def __init__(self, fake_gj):
        self.fake_gj = fake_gj
        self.calls = []
        self.sequences = {}
        self.omitted = set()
        self.reverse = False
        self.count_values = 12582912

    def FDB(self):  # noqa: N802 - mirrors pyfdb
        return self

    def set_sequence(self, field, locations):
        self.sequences[location_cache.canonical_field_key(field)] = list(locations)

    def omit(self, field):
        self.omitted.add(location_cache.canonical_field_key(field))

    def list(self, selection):
        selection = {
            key: list(values) if isinstance(values, list) else values
            for key, values in selection.items()
        }
        self.calls.append(selection)
        keys = list(selection)
        values = [
            raw if isinstance(raw, list) else str(raw).split("/")
            for raw in selection.values()
        ]
        combinations = list(itertools.product(*values))
        if self.reverse:
            combinations.reverse()
        for combination in combinations:
            field = dict(zip(keys, combination))
            key = location_cache.canonical_field_key(field)
            if key in self.omitted:
                continue
            sequence = self.sequences.get(key)
            if sequence:
                location = sequence.pop(0) if len(sequence) > 1 else sequence[0]
            else:
                location = make_location(field)
            self.fake_gj.path_fields[location.path] = field
            yield FakeListElement(field, location, self.count_values)


def make_location(field, suffix=""):
    return location_cache.FieldLocation(
        path=f"/archive/{field['date']}-{field['time']}{suffix}.grib",
        scheme="fdb",
        offset=1234,
        length=5678,
        host="store.example",
        port=9000,
    )


@pytest.fixture
def fake_gj(monkeypatch):
    mod = FakePyGribJump()
    monkeypatch.setitem(sys.modules, "pygribjump", mod)  # type: ignore[arg-type]
    monkeypatch.setenv("POLYTOPE_CHUNKS_LOCCACHE_SIZE", "0")
    # Most functional tests exercise the exact pre-executor path. Executor-specific
    # tests opt in explicitly below.
    monkeypatch.setenv("POLYTOPE_CHUNKS_GJ_THREADS", "0")
    extract._reset_gribjump()
    extract._reset_location_state()
    extract._reset_hash_learning()
    yield mod
    extract._reset_gribjump()
    extract._reset_location_state()
    extract._reset_hash_learning()


@pytest.fixture
def fake_fdb(fake_gj, monkeypatch):
    mod = FakePyFDB(fake_gj)
    monkeypatch.setitem(sys.modules, "pyfdb", mod)  # type: ignore[arg-type]
    return mod


def enable_location_cache(monkeypatch, size=4096, ttl=3600):
    monkeypatch.setenv("POLYTOPE_CHUNKS_LOCCACHE_SIZE", str(size))
    monkeypatch.setenv("POLYTOPE_CHUNKS_LOCCACHE_TTL_SECS", str(ttl))
    extract._reset_location_state()
    extract._location_servermap = location_cache.LocationServerMap(
        [{"fdb": "store.example:9000"}]
    )


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
            "shuffle": False,
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


def inverse_shuffle(raw, itemsize):
    """Inverse the contract's whole-payload byte shuffle."""
    return np.frombuffer(raw, dtype=np.uint8).reshape(itemsize, -1).T.tobytes()


def parse_json(raw):
    try:
        return json.loads(raw)
    except (TypeError, json.JSONDecodeError) as exc:
        pytest.fail(f"invalid JSON in test response: {exc}")


def as_int(raw):
    try:
        return int(raw)
    except (TypeError, ValueError) as exc:
        pytest.fail(f"expected integer text, got {raw!r}: {exc}")


def as_float(raw):
    try:
        return float(raw)
    except (TypeError, ValueError) as exc:
        pytest.fail(f"expected float text, got {raw!r}: {exc}")


# ---------------------------------------------------------------------------
# FIELD_ORDER
# ---------------------------------------------------------------------------


def test_field_order_two_axes_rightmost_fastest():
    spec, values = extract.parse_extract(base_request())
    fields = list(extract.enumerate_fields(values, spec["order"]))
    got = [(f["date"], f["time"]) for f in fields]
    expected_order = [
        ("20200101", "0000"),
        ("20200101", "0600"),
        ("20200101", "1200"),
        ("20200102", "0000"),
        ("20200102", "0600"),
        ("20200102", "1200"),
    ]
    assert got == expected_order
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
    expected_order = [
        ("1200", "20200101"),
        ("1200", "20200102"),
        ("0000", "20200101"),
        ("0000", "20200102"),
    ]
    assert got == expected_order


def test_list_and_scalar_values_accepted():
    req = base_request(date=["20200101", "20200102"], time="0000", realization=1)
    req["extract"]["order"] = ["date"]
    spec, values = extract.parse_extract(req)
    assert values["date"] == ["20200101", "20200102"]
    assert values["time"] == ["0000"]
    assert values["realization"] == ["1"]


def test_wire_options_default_to_float32_and_shuffle():
    req = base_request()
    req["extract"].pop("dtype")
    req["extract"].pop("shuffle")
    spec, _ = extract.parse_extract(req)
    assert spec["dtype"] == "float32"
    assert spec["shuffle"]


# ---------------------------------------------------------------------------
# Assembly / byte layout
# ---------------------------------------------------------------------------


def test_float64_unshuffled_payload_is_byte_identical_legacy(fake_gj):
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
    expected_shape = (6 * 4,)
    assert got.shape == expected_shape
    np.testing.assert_array_equal(got, expected)
    # exact bytes too
    assert zstandard.ZstdDecompressor().decompress(payload) == expected.astype("<f8").tobytes()
    assert timings["fields"] == 6 and timings["raw_bytes"] == 6 * 4 * 8

    # one gribjump call with one single-field request per field, in order
    (call,) = fake_gj.calls
    reqs = call["requests"]
    expected_order = list(
        itertools.product(["20200101", "20200102"], ["0000", "0600", "1200"])
    )
    actual_order = [(r.req["date"], r.req["time"]) for r in reqs]
    assert actual_order == expected_order
    for r in reqs:
        expected_ranges = [(0, 4)]
        assert r.ranges == expected_ranges
        assert r.gridHash == "abcdef0123456789"
        assert "verb" not in r.req and "extract" not in r.req
        assert all("/" not in v for v in r.req.values())


@pytest.mark.parametrize("dtype", ["<f4", "<f8"])
def test_byte_shuffle_inverse_property(dtype):
    values = np.array([0.0, 1.0, -2.5, 3.25, np.nan, np.inf], dtype=dtype)
    shuffled = extract.byte_shuffle(values)
    assert inverse_shuffle(shuffled, values.dtype.itemsize) == values.tobytes()


def test_float32_shuffled_payload_exact_bytes(fake_gj):
    known = np.array([0.0, 1.0, -2.5, 3.25], dtype="<f8")
    fake_gj.values_fn = lambda _field, lo, hi: known[lo:hi]
    req = base_request(date="20200101", time="0000")
    req["extract"].update(order=[], dtype="float32", shuffle=True)

    payload, content_type, timings = extract.run_extract(req)

    assert content_type == "application/octet-stream"
    compressed_input = zstandard.ZstdDecompressor().decompress(payload)
    expected = known.astype("<f4")
    assert compressed_input == extract.byte_shuffle(expected)
    unshuffled = inverse_shuffle(compressed_input, expected.dtype.itemsize)
    np.testing.assert_array_equal(np.frombuffer(unshuffled, dtype="<f4"), expected)
    assert timings["raw_bytes"] == expected.nbytes


def test_multi_range_concatenation_within_field(fake_gj):
    req = base_request(date="20200101/20200102", time="0000")
    req["extract"] = {
        "ranges": [[10, 13], [2, 4], [100, 101]],
        "order": ["date"],
        "grid_hash": None,
        "dtype": "float64",
        "shuffle": False,
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
    expected_ranges = [(10, 13), (2, 4), (100, 101)]
    assert fake_gj.calls[0]["requests"][0].ranges == expected_ranges


def test_nan_values_pass_through(fake_gj):
    fake_gj.values_fn = lambda f, lo, hi: np.full(hi - lo, np.nan)
    req = base_request(date="20200101", time="0000")
    req["extract"]["order"] = []
    payload, _, _ = extract.run_extract(req)
    got = decode(payload)
    expected_shape = (4,)
    assert got.shape == expected_shape and np.isnan(got).all()


# ---------------------------------------------------------------------------
# FDB location cache / location-based extraction
# ---------------------------------------------------------------------------


def single_field_request():
    req = base_request(date="20200101", time="0000")
    req["extract"]["order"] = []
    return req


def single_field(req):
    spec, values = extract.parse_extract(req)
    return next(extract.enumerate_fields(values, spec["order"]))


def _in_two_threads(call):
    barrier = threading.Barrier(2)

    def run(_):
        barrier.wait()
        first = call()
        return first, call()

    with ThreadPoolExecutor(max_workers=2) as executor:
        return list(executor.map(run, range(2)))


def test_gribjump_handles_are_thread_local():
    created = []

    def create_handle():
        handle = object()
        created.append(handle)
        return handle

    module = types.SimpleNamespace(GribJump=create_handle)
    results = _in_two_threads(lambda: extract._get_gribjump(module))

    assert all(first is second for first, second in results)
    assert results[0][0] is not results[1][0]
    assert len(created) == 2


def test_pyfdb_handles_are_thread_local():
    created = []

    def create_handle():
        handle = object()
        created.append(handle)
        return handle

    module = types.SimpleNamespace(FDB=create_handle)
    results = _in_two_threads(lambda: extract._get_fdb(module))

    assert all(first is second for first, second in results)
    assert results[0][0] is not results[1][0]
    assert len(created) == 2


def test_location_cache_is_shared_across_threads(monkeypatch):
    monkeypatch.setenv("POLYTOPE_CHUNKS_LOCCACHE_SIZE", "16")
    extract._reset_location_state()
    try:
        results = _in_two_threads(extract._get_location_cache)
        caches = [first for first, second in results if first is second]
        assert len(caches) == 2
        assert caches[0] is caches[1]
    finally:
        extract._reset_location_state()


def test_path_only_fdb_uri_uses_internal_scheme():
    class LocalListElement:
        uri = FakeURI(
            "/data/prod_6/fdb/archive.mn5-prod-store6.novalocal.123.data",
            scheme="",
            host=None,
            port=None,
            query="internalScheme=file",
        )

        @staticmethod
        def has_location():
            return True

        @staticmethod
        def offset():
            return 1234

        @staticmethod
        def length():
            return 5678

    location = extract._location_from_element(LocalListElement())
    assert location == location_cache.FieldLocation(
        path="/data/prod_6/fdb/archive.mn5-prod-store6.novalocal.123.data",
        scheme="file",
        offset=1234,
        length=5678,
        host="mn5-prod-store6.novalocal",
        port=0,
    )


def test_location_cache_hit_skips_pyfdb(fake_gj, fake_fdb, monkeypatch):
    enable_location_cache(monkeypatch)
    req = single_field_request()
    field = single_field(req)
    location = make_location(field)
    fake_gj.path_fields[location.path] = field
    extract._get_location_cache().put(field, location)

    payload, _, timings = extract.run_extract(req)

    np.testing.assert_array_equal(decode(payload), default_values(field, 0, 4))
    assert fake_fdb.calls == []
    assert timings["cache_hits"] == 1 and timings["cache_misses"] == 0
    path_request = fake_gj.path_calls[0]["requests"][0]
    expected_endpoint = ("fdb", "store.example", 9000)
    actual_endpoint = (path_request.scheme, path_request.host, path_request.port)
    assert actual_endpoint == expected_endpoint


def test_location_cache_miss_populates_then_hits(fake_gj, fake_fdb, monkeypatch):
    enable_location_cache(monkeypatch)
    req = single_field_request()

    first, _, first_timings = extract.run_extract(req)
    second, _, second_timings = extract.run_extract(req)

    assert first == second
    assert len(fake_fdb.calls) == 1
    assert first_timings["cache_misses"] == 1
    assert second_timings["cache_hits"] == 1
    assert first_timings["lookup_mode"] == "single"
    assert second_timings["lookup_mode"] == "none"
    assert len(fake_gj.path_calls) == 2


def test_all_cache_misses_use_one_batched_lookup_and_preserve_order(
    fake_gj, fake_fdb, monkeypatch, caplog
):
    enable_location_cache(monkeypatch)
    caplog.set_level(logging.INFO)
    fake_fdb.reverse = True
    req = base_request(date="20200101/20200102", time="0000/0600")
    spec, values = extract.parse_extract(req)
    fields = list(extract.enumerate_fields(values, spec["order"]))

    payload, _, timings = extract.run_extract(req, job_id="batch-profile")

    expected = np.concatenate([default_values(field, 0, 4) for field in fields])
    np.testing.assert_array_equal(decode(payload), expected)
    assert len(fake_fdb.calls) == 1
    assert fake_fdb.calls[0] == values
    assert timings["lookup_mode"] == "batch"
    cache = extract._get_location_cache()
    assert [cache.get(field) for field in fields] == [
        make_location(field) for field in fields
    ]
    (line,) = _profile_lines([record.getMessage() for record in caplog.records])
    match = _PROFILE_RE.match(line)
    assert match and match["lookup_mode"] == "batch"


def test_concurrent_batch_lookups_are_serialized(fake_gj, fake_fdb):
    req = base_request(date="20200101/20200102", time="0000/0600")
    spec, values = extract.parse_extract(req)
    fields = list(extract.enumerate_fields(values, spec["order"]))
    original_list = fake_fdb.list
    barrier = threading.Barrier(2)
    state_lock = threading.Lock()
    state = {"active": 0, "max_active": 0}

    def slow_list(selection):
        with state_lock:
            state["active"] += 1
            state["max_active"] = max(state["max_active"], state["active"])
        try:
            time.sleep(0.05)
            elements = list(original_list(selection))
        finally:
            with state_lock:
                state["active"] -= 1
        return iter(elements)

    fake_fdb.list = slow_list

    def lookup():
        barrier.wait()
        return extract._lookup_field_locations(fields, values, fake_fdb)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _index: lookup(), range(2)))

    assert state["max_active"] == 1
    assert all(len(result) == len(fields) for result in results)


def test_lookup_subbatch_boundaries(fake_gj, fake_fdb, monkeypatch):
    monkeypatch.setattr(extract, "LOOKUP_SUBBATCH", 2)
    req = base_request()
    spec, values = extract.parse_extract(req)
    fields = list(extract.enumerate_fields(values, spec["order"]))

    locations = extract._lookup_field_locations(fields, values, fake_fdb)

    assert len(fake_fdb.calls) == 3
    assert len(locations) == len(fields)
    assert all(
        locations[location_cache.canonical_field_key(field)] == make_location(field)
        for field in fields
    )


def test_lookup_releases_lock_between_subbatches(fake_gj, fake_fdb, monkeypatch):
    monkeypatch.setattr(extract, "LOOKUP_SUBBATCH", 2)
    req = base_request()
    spec, values = extract.parse_extract(req)
    fields = list(extract.enumerate_fields(values, spec["order"]))
    small = {**fields[0], "date": "20300101"}
    original_list = fake_fdb.list
    yielded = threading.Event()
    small_done = threading.Event()
    order = []

    def recording_list(selection):
        dates = selection.get("date")
        order.append("small" if "20300101" in dates else "big")
        return original_list(selection)

    def yield_to_waiter(seconds):
        assert seconds == 0
        yielded.set()
        assert small_done.wait(timeout=2)

    fake_fdb.list = recording_list
    monkeypatch.setattr(extract.time, "sleep", yield_to_waiter)
    thread = threading.Thread(
        target=extract._lookup_field_locations, args=(fields, values, fake_fdb)
    )
    thread.start()
    assert yielded.wait(timeout=2)
    extract._lookup_field_location(small, fake_fdb)
    small_done.set()
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert order[:2] == ["big", "small"]


def test_failed_lookup_subbatch_falls_back_only_its_fields(
    fake_gj, fake_fdb, monkeypatch
):
    enable_location_cache(monkeypatch)
    monkeypatch.setattr(extract, "LOOKUP_SUBBATCH", 2)
    original_list = fake_fdb.list
    calls = 0

    def flaky_list(selection):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("sub-list failed")
        return original_list(selection)

    fake_fdb.list = flaky_list
    req = base_request()
    spec, values = extract.parse_extract(req)
    fields = list(extract.enumerate_fields(values, spec["order"]))
    payload, _, timings = extract.run_extract(req)

    expected = np.concatenate([default_values(field, 0, 4) for field in fields])
    np.testing.assert_array_equal(decode(payload), expected)
    assert calls == 3
    assert timings["fallbacks"] == 2
    assert [request.req for request in fake_gj.calls[0]["requests"]] == fields[2:4]


def test_gj_subbatches_preserve_global_field_order(
    fake_gj, fake_fdb, monkeypatch
):
    enable_location_cache(monkeypatch)
    monkeypatch.setattr(extract, "GJ_SUBBATCH", 2)
    fake_fdb.reverse = True
    req = base_request()
    spec, values = extract.parse_extract(req)
    fields = list(extract.enumerate_fields(values, spec["order"]))

    payload, _, timings = extract.run_extract(req)

    expected = np.concatenate([default_values(field, 0, 4) for field in fields])
    np.testing.assert_array_equal(decode(payload), expected)
    assert [len(call["requests"]) for call in fake_gj.path_calls] == [2, 2, 2]
    assert timings["gj_subbatches"] == 3
    assert timings["lookup_subbatches"] == 1


def test_batch_missing_field_uses_request_fallback(
    fake_gj, fake_fdb, monkeypatch
):
    enable_location_cache(monkeypatch)
    req = base_request(date="20200101/20200102", time="0000/0600")
    spec, values = extract.parse_extract(req)
    fields = list(extract.enumerate_fields(values, spec["order"]))
    missing = fields[1]
    fake_fdb.omit(missing)

    payload, _, timings = extract.run_extract(req)

    expected = np.concatenate([default_values(field, 0, 4) for field in fields])
    np.testing.assert_array_equal(decode(payload), expected)
    assert len(fake_fdb.calls) == 1
    assert len(fake_gj.calls) == 1
    assert [request.req for request in fake_gj.calls[0]["requests"]] == [missing]
    assert timings["fallbacks"] == 1
    assert timings["lookup_mode"] == "batch"


def test_location_failure_invalidates_refreshes_and_retries_once(
    fake_gj, fake_fdb, monkeypatch
):
    enable_location_cache(monkeypatch)
    req = single_field_request()
    field = single_field(req)
    stale = make_location(field, "-stale")
    fresh = make_location(field, "-fresh")
    fake_fdb.set_sequence(field, [stale, fresh])
    fake_gj.path_failures[stale.path] = 1

    payload, _, timings = extract.run_extract(req)

    np.testing.assert_array_equal(decode(payload), default_values(field, 0, 4))
    assert len(fake_fdb.calls) == 2
    assert [c["requests"][0].path for c in fake_gj.path_calls] == [
        stale.path,
        fresh.path,
    ]
    assert timings["cache_misses"] == 1
    assert extract._get_location_cache().stats()["invalidations"] == 1


def test_location_failure_after_refresh_falls_back(fake_gj, fake_fdb, monkeypatch):
    enable_location_cache(monkeypatch)
    req = single_field_request()
    field = single_field(req)
    stale = make_location(field, "-stale")
    fresh = make_location(field, "-fresh")
    fake_fdb.set_sequence(field, [stale, fresh])
    fake_gj.path_failures.update({stale.path: 1, fresh.path: 1})

    payload, _, timings = extract.run_extract(req)

    np.testing.assert_array_equal(decode(payload), default_values(field, 0, 4))
    assert len(fake_fdb.calls) == 2
    assert len(fake_gj.path_calls) == 2
    assert len(fake_gj.calls) == 1
    assert timings["fallbacks"] == 1


def test_unmappable_location_uses_identical_request_fallback(
    fake_gj, fake_fdb, monkeypatch, caplog
):
    req = single_field_request()
    request_payload, _, _ = extract.run_extract(req)
    caplog.set_level(logging.INFO)

    enable_location_cache(monkeypatch)
    field = single_field(req)
    unmappable = location_cache.FieldLocation(
        path="/archive/internal.grib",
        scheme="fdb",
        offset=1234,
        length=5678,
        host="unknown-store.novalocal",
        port=0,
    )
    extract._get_location_cache().put(field, unmappable)
    path_calls_before = len(fake_gj.path_calls)

    fallback_payload, _, timings = extract.run_extract(req, job_id="fallback-profile")

    assert fallback_payload == request_payload
    assert len(fake_gj.path_calls) == path_calls_before
    assert timings["fallbacks"] == 1
    assert timings["cache_hits"] == 1
    (line,) = _profile_lines([record.getMessage() for record in caplog.records])
    match = _PROFILE_RE.match(line)
    assert match and match["job"] == "fallback-profile"
    assert as_int(match["fallback"]) == 1


def test_novalocal_port_zero_translates_before_path_request(
    fake_gj, fake_fdb, monkeypatch
):
    enable_location_cache(monkeypatch)
    extract._location_servermap = location_cache.LocationServerMap(
        [
            {
                "fdb": (
                    "mn5-prod-store6-ope.mn5.apps.dte.destination-earth.eu:10000"
                )
            }
        ]
    )
    req = single_field_request()
    field = single_field(req)
    internal = location_cache.FieldLocation(
        path="/archive/internal.grib",
        scheme="fdb",
        offset=1234,
        length=5678,
        host="mn5-prod-store6.novalocal",
        port=0,
    )
    fake_gj.path_fields[internal.path] = field
    extract._get_location_cache().put(field, internal)

    payload, _, timings = extract.run_extract(req)

    np.testing.assert_array_equal(decode(payload), default_values(field, 0, 4))
    path_request = fake_gj.path_calls[0]["requests"][0]
    assert path_request.host == "mn5-prod-store6-ope.mn5.apps.dte.destination-earth.eu"
    assert path_request.port == 10000
    assert timings["fallbacks"] == 0


def test_size_zero_uses_byte_identical_request_path(fake_gj, fake_fdb):
    req = single_field_request()
    field = single_field(req)

    payload, _, timings = extract.run_extract(req)

    expected = default_values(field, 0, 4).astype("<f8").tobytes()
    assert zstandard.ZstdDecompressor().decompress(payload) == expected
    assert len(fake_gj.calls) == 1 and fake_gj.path_calls == []
    assert fake_fdb.calls == []
    assert timings["cache_hits"] == 0 and timings["cache_misses"] == 0


def test_field_order_preserved_with_mixed_hits_and_misses(
    fake_gj, fake_fdb, monkeypatch
):
    enable_location_cache(monkeypatch)
    req = base_request(date="20200101/20200102", time="0000/0600")
    spec, values = extract.parse_extract(req)
    fields = list(extract.enumerate_fields(values, spec["order"]))
    cache = extract._get_location_cache()
    cached = {}
    for field in (fields[0], fields[2]):
        location = make_location(field, "-cached")
        cached[location_cache.canonical_field_key(field)] = location
        fake_gj.path_fields[location.path] = field
        cache.put(field, location)

    payload, _, timings = extract.run_extract(req)

    expected = np.concatenate([default_values(field, 0, 4) for field in fields])
    np.testing.assert_array_equal(decode(payload), expected)
    extracted_paths = [request.path for request in fake_gj.path_calls[0]["requests"]]
    assert extracted_paths == [
        cached.get(location_cache.canonical_field_key(field), make_location(field)).path
        for field in fields
    ]
    assert timings["cache_hits"] == 2 and timings["cache_misses"] == 2
    assert timings["lookup_mode"] == "batch"
    assert len(fake_fdb.calls) == 1
    assert fake_fdb.calls[0] == values
    assert cache.get(fields[0]) == cached[location_cache.canonical_field_key(fields[0])]
    assert cache.get(fields[2]) == cached[location_cache.canonical_field_key(fields[2])]
    assert len(fake_gj.path_calls) == 1


# ---------------------------------------------------------------------------
# Validation / failure
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "mutate, match",
    [
        (lambda r: r["extract"].update(dtype="float16"), "dtype"),
        (lambda r: r["extract"].update(dtype=32), "dtype"),
        (lambda r: r["extract"].update(shuffle="true"), "shuffle"),
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


def grid_mismatch(found="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"):
    return GribJumpException(
        "Bad value: Grid hash mismatch for extraction item 0. "
        "Request specified: cbda19e48d4d7e5e22641154878b9b22, "
        f"JumpInfo contains: {found}"
    )


def test_grid_hash_mismatch_learns_retries_and_caches(fake_gj, fake_fdb, caplog):
    found = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    req = single_field_request()
    req["extract"]["grid_hash"] = "cbda19e48d4d7e5e22641154878b9b22"
    fake_gj.raise_exc = [grid_mismatch(found), None]
    caplog.set_level(logging.WARNING)

    first, _, _ = extract.run_extract(
        req, pyfdb=fake_fdb, job_id="01hashlearnrequest0000000001"
    )
    second, _, _ = extract.run_extract(req, pyfdb=fake_fdb)

    assert first == second
    assert [call["requests"][0].gridHash for call in fake_gj.calls] == [
        "cbda19e48d4d7e5e22641154878b9b22",
        found,
        found,
    ]
    assert len(fake_fdb.calls) == 1
    assert any("chunks-grid-hash learned" in record.getMessage() for record in caplog.records)


def test_grid_hash_mismatch_wrong_count_fails_without_retry(fake_gj, fake_fdb):
    req = single_field_request()
    req["extract"]["grid_hash"] = "cbda19e48d4d7e5e22641154878b9b22"
    fake_fdb.count_values = 3145728
    fake_gj.raise_exc = grid_mismatch()

    with pytest.raises(
        extract.ExtractError,
        match=r"class=d1.*expected cbda19e.*found a{32}.*server-side grid-registry gap",
    ):
        extract.run_extract(req, pyfdb=fake_fdb, job_id="wrong-count-id")

    assert len(fake_gj.calls) == 1
    assert len(fake_fdb.calls) == 1


def test_grid_hash_mismatch_learning_kill_switch(fake_gj, fake_fdb, monkeypatch):
    req = single_field_request()
    req["extract"]["grid_hash"] = "cbda19e48d4d7e5e22641154878b9b22"
    fake_gj.raise_exc = grid_mismatch()
    monkeypatch.setenv("POLYTOPE_CHUNKS_HASH_LEARN", "0")

    with pytest.raises(extract.ExtractError, match=r"request ID: kill-switch-id"):
        extract.run_extract(req, pyfdb=fake_fdb, job_id="kill-switch-id")

    assert len(fake_gj.calls) == 1
    assert fake_fdb.calls == []


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
    status = parse_json(status_json)
    assert status["ok"], status
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
    status = parse_json(status_json)
    assert status["ok"], status
    assert status["content_type"] == "application/prs.coverage+json"
    assert body == b'{"type": "CoverageCollection"}'
    assert recording_ds.retrieved == [req]
    assert fake_gj.calls == []


def test_dispatch_non_object_extract_key_is_legacy(fake_gj, recording_ds):
    req = base_request(extract="yes")
    _, status_json = run_polytope_worker.process(_payload(req))
    assert parse_json(status_json)["content_type"] == "application/prs.coverage+json"
    assert len(recording_ds.retrieved) == 1
    assert fake_gj.calls == []


def test_dispatch_extract_error_reports_job_failure(fake_gj, recording_ds):
    req = base_request()
    req["extract"]["dtype"] = "float16"
    body, status_json = run_polytope_worker.process(_payload(req))
    status = parse_json(status_json)
    assert not status["ok"]
    assert body == b""
    assert "extract.dtype" in status["error"]["message"]
    assert recording_ds.retrieved == []
    assert "content_type" not in status


# ---------------------------------------------------------------------------
# Per-phase profiling (chunks-profile log line + timings)
# ---------------------------------------------------------------------------

_PROFILE_RE = re.compile(
    r"^chunks-profile job=(?P<job>\S+) status=(?P<status>\w+) phase=(?P<phase>\w+) "
    r"fields=(?P<fields>\d+) ranges=(?P<ranges>\d+) points=(?P<points>\d+) "
    r"dtype=(?P<dtype>f32|f64) shuffle=(?P<shuffle>[01]) "
    r"cache=(?P<hits>\d+)/(?P<misses>\d+) fallback=(?P<fallback>\d+) "
    r"lookup_mode=(?P<lookup_mode>batch|single|none) "
    r"subbatches=(?P<lookup_subbatches>\d+)/(?P<gj_subbatches>\d+) "
    r"t_lookup=(?P<t_lookup>[\d.]+)ms "
    r"t_parse=(?P<t_parse>[\d.]+)ms t_enum=(?P<t_enum>[\d.]+)ms "
    r"t_extract=(?P<t_extract>[\d.]+)ms t_assemble=(?P<t_assemble>[\d.]+)ms "
    r"t_shuffle=(?P<t_shuffle>[\d.]+)ms t_zstd=(?P<t_zstd>[\d.]+)ms "
    r"t_total=(?P<t_total>[\d.]+)ms raw_bytes=(?P<raw_bytes>\d+) "
    r"bytes=(?P<bytes>\d+) zstd_level=(?P<level>\d+)$"
)


def _profile_lines(messages):
    return [m for m in messages if m.startswith("chunks-profile ")]


def test_profile_line_and_phase_timings(fake_gj, caplog):
    caplog.set_level(logging.INFO)
    req = base_request(
        extract={
            **base_request()["extract"],
            "ranges": [[0, 4], [10, 13]],
            "dtype": "float32",
            "shuffle": True,
        }
    )
    payload, _, timings = extract.run_extract(req, job_id="job-xyz")

    (line,) = _profile_lines([r.getMessage() for r in caplog.records])
    m = _PROFILE_RE.match(line)
    assert m, line
    assert m["job"] == "job-xyz"
    assert m["status"] == "ok" and m["phase"] == "done"
    assert as_int(m["fields"]) == 6 and as_int(m["ranges"]) == 2
    assert as_int(m["points"]) == 6 * 7
    assert m["dtype"] == "f32" and as_int(m["shuffle"]) == 1
    assert as_float(m["t_shuffle"]) >= 0.0
    assert as_int(m["raw_bytes"]) == 6 * 7 * 4
    assert as_int(m["bytes"]) == len(payload)
    assert as_int(m["level"]) == extract.ZSTD_LEVEL

    for k in (
        "parse_ms",
        "enum_ms",
        "extract_ms",
        "assemble_ms",
        "shuffle_ms",
        "compress_ms",
        "retrieve_ms",
    ):
        assert isinstance(timings[k], float) and timings[k] >= 0.0, k
    assert timings["points"] == 42 and timings["fields"] == 6
    assert timings["raw_bytes"] == 42 * 4 and timings["payload_bytes"] == len(payload)
    assert as_int(m["hits"]) == 0 and as_int(m["misses"]) == 0
    assert as_int(m["fallback"]) == 0
    assert as_float(m["t_lookup"]) == 0.0
    assert timings["lookup_ms"] == 0.0
    assert m["lookup_mode"] == "none"
    assert timings["lookup_mode"] == "none"


def test_profile_line_reports_cache_counts_and_lookup_time(
    fake_gj, fake_fdb, monkeypatch, caplog
):
    enable_location_cache(monkeypatch)
    caplog.set_level(logging.INFO)
    extract.run_extract(single_field_request(), job_id="cache-profile")
    (line,) = _profile_lines([r.getMessage() for r in caplog.records])
    match = _PROFILE_RE.match(line)
    assert match, line
    assert as_int(match["hits"]) == 0 and as_int(match["misses"]) == 1
    assert as_int(match["fallback"]) == 0
    assert as_float(match["t_lookup"]) >= 0.0
    assert match["lookup_mode"] == "single"


def test_profile_line_on_failure_reports_phase(fake_gj, caplog, monkeypatch):
    caplog.set_level(logging.INFO)

    # Real pygribjump does the remote extraction eagerly inside extract()
    # (the fake is a lazy generator), so raise from the call itself.
    def eager_raise(self, requests, ctx=None):
        raise GribJumpException("boom")

    monkeypatch.setattr(FakeGribJump, "extract", eager_raise)
    with pytest.raises(extract.ExtractError):
        extract.run_extract(base_request(), job_id="job-err")
    (line,) = _profile_lines([r.getMessage() for r in caplog.records])
    m = _PROFILE_RE.match(line)
    assert m, line
    assert m["job"] == "job-err" and m["status"] == "error" and m["phase"] == "extract"
    assert as_int(m["fields"]) == 6 and as_int(m["bytes"]) == 0


def test_profile_line_on_validation_failure(fake_gj, caplog):
    caplog.set_level(logging.INFO)
    req = base_request()
    req["extract"]["dtype"] = "float16"
    with pytest.raises(extract.ExtractError):
        extract.run_extract(req)
    (line,) = _profile_lines([r.getMessage() for r in caplog.records])
    m = _PROFILE_RE.match(line)
    assert m and m["job"] == "-" and m["status"] == "error" and m["phase"] == "parse"
    assert fake_gj.calls == []


def test_compress_accepts_numpy_buffer_without_copy():
    arr = np.arange(1000, dtype="<f8")
    assert zstandard.ZstdDecompressor().decompress(extract.compress(arr)) == arr.tobytes()


def test_gribjump_handle_is_process_scoped(fake_gj, monkeypatch):
    created = []
    orig = fake_gj.GribJump

    def counting():
        created.append(1)
        return orig()

    monkeypatch.setattr(fake_gj, "GribJump", counting)
    for _ in range(3):
        extract.run_extract(base_request())
    assert len(created) == 1
    assert len(fake_gj.calls) == 3


def test_dispatch_passes_job_id_and_emits_one_profile_log(fake_gj, recording_ds):
    payload = parse_json(_payload(base_request()))
    payload["job_id"] = "01abc"
    body, status_json = run_polytope_worker.process(json.dumps(payload))
    status = parse_json(status_json)
    assert status["ok"], status
    assert fake_gj.calls[0]["ctx"] == {"user": "ecmwf:tester", "job_id": "01abc"}
    lines = _profile_lines([rec["message"] for rec in status["logs"]])
    assert len(lines) == 1 and lines[0].startswith("chunks-profile job=01abc status=ok ")
    assert status["timings"]["payload_bytes"] == len(body)
    for k in (
        "parse_ms",
        "enum_ms",
        "extract_ms",
        "assemble_ms",
        "shuffle_ms",
        "compress_ms",
        "total_ms",
    ):
        assert k in status["timings"], k


def enable_gj_executor(fake_gj, monkeypatch, threads):
    fake_fdb = FakePyFDB(fake_gj)
    monkeypatch.setitem(sys.modules, "pyfdb", fake_fdb)  # type: ignore[arg-type]
    monkeypatch.setenv("POLYTOPE_CHUNKS_GJ_THREADS", str(threads))
    extract._location_servermap = location_cache.LocationServerMap(
        [{"fdb": "store.example:9000"}]
    )
    extract._reset_gribjump()
    extract.warm_up(fake_gj)


def test_disabled_executor_retains_lazy_thread_local_path(fake_gj, monkeypatch):
    created = []
    orig = fake_gj.GribJump

    def counting():
        created.append(1)
        return orig()

    monkeypatch.setattr(fake_gj, "GribJump", counting)
    extract.warm_up(fake_gj)
    extract.warm_up(fake_gj)
    assert created == []

    extract.run_extract(base_request())
    extract.run_extract(base_request())
    assert len(created) == 1
    assert extract._gj_executor is None


def test_executor_warms_each_handle_once_at_startup(fake_gj, monkeypatch):
    enable_gj_executor(fake_gj, monkeypatch, 3)

    assert len(fake_gj.handles) == 3
    assert len(fake_gj.path_calls) == 3
    assert {call["handle"] for call in fake_gj.path_calls} == set(fake_gj.handles)
    warm_ranges = [call["requests"][0].ranges for call in fake_gj.path_calls]
    assert [ranges[0][0] for ranges in warm_ranges] == [0] * 3
    assert [ranges[0][1] for ranges in warm_ranges] == [1] * 3

    extract.warm_up(fake_gj)
    assert len(fake_gj.handles) == 3
    assert len(fake_gj.path_calls) == 3


def test_executor_reuses_same_handle_across_jobs(fake_gj, monkeypatch):
    enable_gj_executor(fake_gj, monkeypatch, 1)
    fake_gj.calls.clear()

    first, _, _ = extract.run_extract(base_request())
    second, _, _ = extract.run_extract(base_request())

    assert first == second
    assert len(fake_gj.calls) == 2
    assert {call["handle"] for call in fake_gj.calls} == {fake_gj.handles[0]}


def test_concurrent_jobs_share_pool_and_preserve_placement(fake_gj, monkeypatch):
    enable_gj_executor(fake_gj, monkeypatch, 2)
    fake_gj.calls.clear()
    fake_gj.delay = 0.02
    requests = [
        base_request(date="20200101/20200102", time="0000/0600"),
        base_request(date="20200102/20200101", time="0600/0000"),
        base_request(date="20200101/20200102", time="0600/0000"),
        base_request(date="20200102/20200101", time="0000/0600"),
    ]

    with ThreadPoolExecutor(max_workers=4) as pool:
        payloads = list(pool.map(lambda req: extract.run_extract(req)[0], requests))

    for request, payload in zip(requests, payloads):
        spec, values = extract.parse_extract(request)
        fields = list(extract.enumerate_fields(values, spec["order"]))
        expected = np.concatenate([default_values(field, 0, 4) for field in fields])
        np.testing.assert_array_equal(decode(payload), expected)
    assert len(fake_gj.calls) == 4
    assert {call["handle"] for call in fake_gj.calls} == set(fake_gj.handles)


def test_failed_executor_call_does_not_poison_pool(fake_gj, monkeypatch):
    enable_gj_executor(fake_gj, monkeypatch, 1)
    fake_gj.calls.clear()
    fake_gj.raise_exc = [GribJumpException("boom"), None]

    with pytest.raises(extract.ExtractError, match="gribjump extraction failed: boom"):
        extract.run_extract(base_request())
    payload, _, _ = extract.run_extract(base_request())

    assert decode(payload).size == 24
    assert len(fake_gj.handles) == 1
    assert {call["handle"] for call in fake_gj.calls} == {fake_gj.handles[0]}


def test_get_datasource_warms_extract_path_once(fake_gj, monkeypatch, tmp_path):
    calls = []
    fake_polytope = types.ModuleType("polytope")

    class FakeDS:
        def __init__(self, config):
            calls.append(("ds", config))

    fake_polytope.PolytopeDataSource = FakeDS  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "polytope", fake_polytope)
    monkeypatch.setattr(extract, "warm_up", lambda: calls.append(("warm",)))
    monkeypatch.setattr(run_polytope_worker, "_datasource", None)
    monkeypatch.setattr(run_polytope_worker, "_config_path", None)
    cfg = tmp_path / "c.json"
    cfg.write_text(json.dumps({"polytope": {"type": "polytope"}}))

    ds1 = run_polytope_worker._get_datasource(str(cfg))
    ds2 = run_polytope_worker._get_datasource(str(cfg))
    assert ds1 is ds2
    expected_calls = [("ds", {"type": "polytope"}), ("warm",)]
    assert calls == expected_calls


def test_warm_up_failure_does_not_block_startup(monkeypatch, caplog):
    def boom():
        raise OSError("libgribjump not found")

    monkeypatch.setattr(extract, "warm_up", boom)
    caplog.set_level(logging.WARNING)
    run_polytope_worker._warm_extract_path()  # must not raise
    assert any("warm-up failed" in r.getMessage() for r in caplog.records)
