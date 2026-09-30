# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

"""/chunks/v1 extract path for the fe-worker (wire contract v2.2).

A job whose (metkit-expanded) request carries a top-level ``extract`` object is
served here instead of by PolytopeMars: enumerate the fields of the request in
FIELD_ORDER, extract the requested half-open grid-point ranges from every field
with pygribjump, concatenate them into one little-endian float64 assembly buffer,
cast to the requested wire dtype, optionally byte-shuffle, and return one zstd
frame.

No polytope-mars, no CoverageJSON. Any missing field, value-count mismatch,
grid-hash mismatch or gribjump error fails the whole job (D5) -- this module
raises and never returns partial results.

gribjump/FDB configuration: the process-scoped ``PolytopeDataSource``
materialises the worker's own ``gribjump_config`` (and optional
``fdb_config``) to /tmp and exports ``GRIBJUMP_CONFIG_FILE`` /
``FDB5_CONFIG_FILE``. The cache reads that same gribjump file once to map
pyfdb's internal store aliases onto the configured FDB servermap endpoints.
By default, location-cache misses are resolved by the parent across the extraction
process pool, then native extraction, assembly, shuffle and compression run inside one
of several long-lived spawn subprocesses. Each child owns exactly one FDB and one
warmed GribJump handle and serves tasks sequentially. Setting
``POLYTOPE_CHUNKS_PROC_POOL=0`` retains the process-locked FDB/threaded-GJ fallback.

Profiling: every job emits exactly one ``chunks-profile`` INFO log line (see
``_log_profile``) with per-phase wall times, so worker-side cost can be read
straight out of ``kubectl logs`` (inside the host's ``python worker logs``
record).
"""

from contextlib import suppress
from concurrent.futures import ThreadPoolExecutor, as_completed
import atexit
import copy
import gc
import faulthandler
import itertools
import logging
import multiprocessing
import os
import queue
import re
import sys
import threading
import time
import struct
from urllib.parse import parse_qs

import numpy as np

from location_cache import (  # type: ignore[import-not-found]
    FieldLocation,
    LocationCache,
    LocationServerMap,
    canonical_field_key,
)

CONTENT_TYPE = "application/octet-stream"
MULTI_CONTENT_TYPE = "application/x-polytope-multichunk"

EXTRACT_KEY = "extract"

# Request keys that are never MARS field-identifying keys and must not be sent
# to gribjump in the per-field request string.
_NON_FIELD_KEYS = {"verb", EXTRACT_KEY}

_SUPPORTED_DTYPES = {"float32", "float64"}


def _env_subbatch(name, default, *, allow_zero=False):
    raw = os.environ.get(name, "").strip()
    try:
        value = int(raw) if raw else default
    except ValueError:
        return default
    return value if value > 0 or (allow_zero and value == 0) else default


def _zstd_level() -> int:
    raw = os.environ.get("POLYTOPE_CHUNKS_ZSTD_LEVEL", "").strip()
    try:
        return int(raw) if raw else 3
    except ValueError:
        return 3


# Read once per process; override via env for profiling experiments.
ZSTD_LEVEL = _zstd_level()
LOOKUP_SUBBATCH = _env_subbatch("POLYTOPE_CHUNKS_LOOKUP_SUBBATCH", 256)
GJ_SUBBATCH = _env_subbatch("POLYTOPE_CHUNKS_GJ_SUBBATCH", 1024, allow_zero=True)
PROC_POOL_SIZE = _env_subbatch("POLYTOPE_CHUNKS_PROC_POOL", 4, allow_zero=True)
LOOKUP_PARALLEL = _env_subbatch(
    "POLYTOPE_CHUNKS_LOOKUP_PARALLEL", PROC_POOL_SIZE or 1
)


def _proc_timeout():
    raw = os.environ.get("POLYTOPE_CHUNKS_PROC_TIMEOUT", "").strip()
    try:
        value = float(raw) if raw else 600.0
    except ValueError:
        return 600.0
    return value if value > 0 else 600.0


class ExtractError(ValueError):
    """Invalid extract job (bad payload or data-level failure)."""

    def __init__(self, message):
        super().__init__(message)
        self.message = message


def is_extract_request(request) -> bool:
    """True iff the job request carries a top-level ``extract`` object."""
    return isinstance(request, dict) and isinstance(request.get(EXTRACT_KEY), dict)


# ---------------------------------------------------------------------------
# Parsing / validation
# ---------------------------------------------------------------------------


def _values(key, raw):
    """Normalise a flat MARS value to a list of strings, preserving order.

    The broker's ``transform::metkit_expansion`` emits every key as a
    slash-joined string (``"20200101/20200102"``); lists and scalars are also
    accepted (scalars/single strings are 1-lists).
    """
    if isinstance(raw, list):
        items = raw
    elif isinstance(raw, str):
        items = raw.split("/")
    elif isinstance(raw, bool) or raw is None or isinstance(raw, dict):
        raise ExtractError(f"request key '{key}' has unsupported value {raw!r}")
    elif isinstance(raw, (int, float)):
        items = [raw]
    else:
        raise ExtractError(f"request key '{key}' has unsupported value {raw!r}")

    out = []
    for item in items:
        if isinstance(item, (dict, list)) or item is None or isinstance(item, bool):
            raise ExtractError(f"request key '{key}' has unsupported value {item!r}")
        text = str(item).strip()
        if not text:
            raise ExtractError(f"request key '{key}' contains an empty value")
        out.append(text)
    if not out:
        raise ExtractError(f"request key '{key}' has no values")
    return out


def _parse_int(value, what):
    if isinstance(value, bool) or not isinstance(value, int):
        raise ExtractError(f"{what} must be an integer, got {value!r}")
    return value


def parse_extract(request):
    """Validate the job and return ``(spec, field_values)``.

    ``spec`` contains ranges, field order, grid hash, wire dtype and shuffle;
    ``field_values`` maps every MARS key to its ordered string values.
    """
    if not is_extract_request(request):
        raise ExtractError("request has no 'extract' object")
    ext = request[EXTRACT_KEY]

    unknown = set(ext) - {"ranges", "order", "grid_hash", "dtype", "shuffle"}
    if unknown:
        raise ExtractError(f"extract: unknown field(s) {sorted(unknown)}")

    dtype = ext.get("dtype", "float32")
    if not isinstance(dtype, str) or dtype not in _SUPPORTED_DTYPES:
        raise ExtractError(
            f"extract.dtype must be one of {sorted(_SUPPORTED_DTYPES)}, got {dtype!r}"
        )
    shuffle = ext.get("shuffle", True)
    if not isinstance(shuffle, bool):
        raise ExtractError(f"extract.shuffle must be a boolean, got {shuffle!r}")

    ranges_raw = ext.get("ranges")
    if not isinstance(ranges_raw, list) or not ranges_raw:
        raise ExtractError("extract.ranges must be a non-empty list of [lo, hi] pairs")
    ranges = []
    for i, r in enumerate(ranges_raw):
        if not isinstance(r, (list, tuple)) or len(r) != 2:
            raise ExtractError(f"extract.ranges[{i}] must be a [lo, hi] pair, got {r!r}")
        lo = _parse_int(r[0], f"extract.ranges[{i}][0]")
        hi = _parse_int(r[1], f"extract.ranges[{i}][1]")
        if lo < 0 or hi <= lo:
            raise ExtractError(
                f"extract.ranges[{i}] = [{lo}, {hi}] is invalid: need 0 <= lo < hi (half-open)"
            )
        ranges.append((lo, hi))

    grid_hash = ext.get("grid_hash", None)
    if grid_hash is not None and (not isinstance(grid_hash, str) or not grid_hash):
        raise ExtractError(f"extract.grid_hash must be a non-empty string or null, got {grid_hash!r}")

    order = ext.get("order")
    if not isinstance(order, list) or not all(isinstance(k, str) and k for k in order):
        raise ExtractError("extract.order must be a list of request key names")
    if len(set(order)) != len(order):
        raise ExtractError(f"extract.order contains duplicate keys: {order}")

    field_values = {}
    for key, raw in request.items():
        if key in _NON_FIELD_KEYS:
            continue
        if isinstance(raw, dict):
            # v0: no feature (or any other object-valued key) on extract jobs.
            raise ExtractError(f"request key '{key}' is an object; not supported on extract jobs")
        field_values[key] = _values(key, raw)

    missing = [k for k in order if k not in field_values]
    if missing:
        raise ExtractError(f"extract.order key(s) {missing} not present in request")

    multi = sorted(k for k, v in field_values.items() if k not in order and len(v) > 1)
    if multi:
        raise ExtractError(
            f"request key(s) {multi} are multi-valued but not listed in extract.order"
        )

    for k in order:
        if len(set(field_values[k])) != len(field_values[k]):
            raise ExtractError(f"request key '{k}' contains duplicate values")

    spec = {
        "ranges": ranges,
        "order": list(order),
        "grid_hash": grid_hash,
        "dtype": dtype,
        "shuffle": shuffle,
    }
    return spec, field_values


def enumerate_fields(field_values, order):
    """Yield one single-field request dict per field, in FIELD_ORDER (contract §4).

    ``itertools.product(*[request[k] for k in order])`` -- rightmost varies
    fastest. Non-order keys are single-valued and copied into every field.
    Key insertion order follows the job request.
    """
    fixed = {k: v[0] for k, v in field_values.items() if k not in order}
    for combo in itertools.product(*[field_values[k] for k in order]):
        point = dict(zip(order, combo))
        yield {k: (point[k] if k in point else fixed[k]) for k in field_values}


# ---------------------------------------------------------------------------
# gribjump / FDB location cache
# ---------------------------------------------------------------------------

_thread_handles = threading.local()
_gj_executor = None
_gj_executor_threads = 0
_gj_executor_lock = threading.Lock()
_process_pool = None
_process_pool_lock = threading.Lock()
_location_cache = None
_location_servermap = None
_location_state_lock = threading.Lock()
# pyfdb 5.22 is not safe to enter concurrently in one process. This lock is
# retained exclusively for POLYTOPE_CHUNKS_PROC_POOL=0 fallback mode; each default
# extraction subprocess owns one FDB handle and serves requests sequentially.
_fdb_list_lock = threading.Lock()

# Grid-hash variants already encoded in Polytope's gh68 change_hash logic, plus
# the live-confirmed climate-dt generation-2 H512 variant. None is applied until
# all non-wildcard request conditions match. Learned entries are exact identities.
_HASH_IDENTITY_KEYS = (
    "class",
    "dataset",
    "experiment",
    "generation",
    "model",
    "realization",
    "resolution",
)
_SEEDED_HASH_OVERRIDES = {
    (None, "climate-dt", None, "1", "icon", None, "high"):
        "9533855ee8e38314e19aaa0434c310da",
    ("d1", "climate-dt", "cont", "2", "ifs-nemo", "3", "high"):
        "47efaa0853e70948a41d5225e7653194",
}
_hash_overrides = copy.copy(_SEEDED_HASH_OVERRIDES)
_learned_hash_identities = set()
_hash_overrides_lock = threading.Lock()
_GRID_HASH_MISMATCH_RE = re.compile(
    r"Grid hash mismatch.*?Request specified:\s*([0-9a-f]{32}).*?"
    r"JumpInfo contains:\s*([0-9a-f]{32})",
    re.IGNORECASE | re.DOTALL,
 )
# Registry count_values for grids accepted by this worker. The hash identifies
# the registry entry even though the v0 frontend-to-worker payload omits the count.
_REGISTRY_COUNTS_BY_HASH = {
    "cbda19e48d4d7e5e22641154878b9b22": 12582912,
    "47efaa0853e70948a41d5225e7653194": 3145728,
    "f3dfeb7a5bbbdd13a20d10fdb3797c71": 196608,
}


def _get_gribjump(pygribjump):
    """Return the calling thread's lazily-created GribJump handle."""
    if not hasattr(_thread_handles, "gribjump"):
        _thread_handles.gribjump = pygribjump.GribJump()
    return _thread_handles.gribjump


_GJ_WARM_FIELD = {
    "class": "d1",
    "dataset": "climate-dt",
    "activity": "baseline",
    "experiment": "hist",
    "generation": "2",
    "model": "ifs-nemo",
    "realization": "1",
    "resolution": "high",
    "expver": "0001",
    "stream": "clte",
    "type": "fc",
    "levtype": "sfc",
    "param": "167",
    "time": "0000",
}
# These live fields cover all seven mn5 climate-dt store endpoints. Warming only
# 19900101 opened the store7 session; a new handle then paid the other six
# connection/session setup costs in its first large job.
_GJ_WARM_DATES = (
    "19900101",
    "19900103",
    "19900105",
    "19900106",
    "19900109",
    "19900112",
    "19900115",
)
_GJ_WARM_GRID_HASH = "cbda19e48d4d7e5e22641154878b9b22"


def _gj_thread_count():
    return _env_subbatch("POLYTOPE_CHUNKS_GJ_THREADS", 4, allow_zero=True)


def _resolve_gj_warm_locations(pyfdb):
    """Resolve one live field per GribJump endpoint without retaining it."""
    fields = [{**_GJ_WARM_FIELD, "date": date} for date in _GJ_WARM_DATES]
    locations_by_key = _lookup_field_locations(
        fields, _batch_request_for_fields(fields), pyfdb
    )
    servermap = _get_location_servermap()
    locations = {}
    for field in fields:
        location = locations_by_key.get(canonical_field_key(field))
        if location is None:
            continue
        translated = servermap.translate(location)
        if translated is not None:
            endpoint = (translated.scheme, translated.host, translated.port)
            locations.setdefault(endpoint, translated)
    if not locations:
        raise ExtractError("GribJump warm-up fields have no routable locations")
    return list(locations.values())


def _warm_gribjump_handle(gj, pygribjump, locations):
    """Open every path client session with one discarded point, never cached."""
    requests = [
        pygribjump.PathExtractionRequest(
            location.path,
            location.scheme,
            location.offset,
            location.host,
            location.port,
            [(0, 1)],
            gridHash=_GJ_WARM_GRID_HASH,
        )
        for location in locations
    ]
    list(gj.extract_from_paths(requests))


def _init_gj_executor_thread(pygribjump, warm_locations):
    gj = _get_gribjump(pygribjump)
    if not warm_locations:
        return
    try:
        _warm_gribjump_handle(gj, pygribjump, warm_locations)
    except Exception as exc:  # best effort; keep the constructed handle usable
        logging.warning(
            "chunks GribJump executor thread warm-up failed (will retry on job): %s",
            exc,
        )


def _executor_barrier(barrier):
    barrier.wait()


def _get_gj_executor(pygribjump):
    """Create and fully start the bounded process-wide GribJump executor once."""
    global _gj_executor, _gj_executor_threads
    threads = _gj_thread_count()
    if threads == 0:
        return None
    if _gj_executor is None:
        with _gj_executor_lock:
            if _gj_executor is None:
                try:
                    import pyfdb  # type: ignore[import-not-found]

                    warm_locations = _resolve_gj_warm_locations(pyfdb)
                except Exception as exc:
                    logging.warning(
                        "chunks GribJump warm-up field lookup failed "
                        "(handles will still be persistent): %s",
                        exc,
                    )
                    warm_locations = []
                executor = ThreadPoolExecutor(
                    max_workers=threads,
                    thread_name_prefix="polytope-gj",
                    initializer=_init_gj_executor_thread,
                    initargs=(pygribjump, warm_locations),
                )
                # ThreadPoolExecutor starts workers lazily. A barrier makes all handles
                # exist and open every endpoint session before startup ends.
                barrier = threading.Barrier(threads)
                futures = [
                    executor.submit(_executor_barrier, barrier) for _ in range(threads)
                ]
                try:
                    for future in futures:
                        future.result()
                except Exception:
                    executor.shutdown(wait=True, cancel_futures=True)
                    raise
                _gj_executor = executor
                _gj_executor_threads = threads
    return _gj_executor


def _invoke_gj(operation, pygribjump):
    return operation(_get_gribjump(pygribjump))


def _run_gribjump(operation, pygribjump):
    """Run one native GJ call on a persistent warmed handle, or the old fallback."""
    executor = _get_gj_executor(pygribjump)
    if executor is None:
        return operation(_get_gribjump(pygribjump))
    return executor.submit(_invoke_gj, operation, pygribjump).result()


def _get_fdb(pyfdb):
    """Return the current child facade handle or a thread-local fallback handle."""
    if hasattr(pyfdb, "reset"):
        return pyfdb.FDB()
    if not hasattr(_thread_handles, "fdb"):
        _thread_handles.fdb = pyfdb.FDB()
    return _thread_handles.fdb


def _get_location_cache():
    """Return the process-global, internally locked location cache."""
    global _location_cache
    if _location_cache is None:
        with _location_state_lock:
            if _location_cache is None:
                _location_cache = LocationCache()
    return _location_cache


def _get_location_servermap():
    global _location_servermap
    if _location_servermap is None:
        with _location_state_lock:
            if _location_servermap is None:
                _location_servermap = LocationServerMap.from_config()
    return _location_servermap


def _reset_gribjump():  # for tests
    global _gj_executor, _gj_executor_threads, _process_pool
    with _gj_executor_lock:
        executor = _gj_executor
        _gj_executor = None
        _gj_executor_threads = 0
    if executor is not None:
        executor.shutdown(wait=True, cancel_futures=True)
    with _process_pool_lock:
        process_pool = _process_pool
        _process_pool = None
    if process_pool is not None:
        process_pool.close()
    if hasattr(_thread_handles, "gribjump"):
        del _thread_handles.gribjump


def _reset_location_state():  # for tests
    global _location_cache, _location_servermap
    if hasattr(_thread_handles, "fdb"):
        del _thread_handles.fdb
    with _location_state_lock:
        _location_cache = None
        _location_servermap = None


def _reset_hash_learning():  # for tests
    global _hash_overrides, _learned_hash_identities
    with _hash_overrides_lock:
        _hash_overrides = copy.copy(_SEEDED_HASH_OVERRIDES)
        _learned_hash_identities = set()


def rust_extract_enabled():
    """Native extraction is the default; set exactly ``0`` for Python fallback."""
    return os.environ.get("POLYTOPE_CHUNKS_RUST_EXTRACT", "1") != "0"


def rust_warm_plan():
    """Resolve one live path per configured endpoint for the Rust host handle."""
    import pyfdb  # type: ignore[import-not-found]

    locations = _resolve_gj_warm_locations(pyfdb)
    return {
        "paths": [
            {
                "path": location.path,
                "offset": location.offset,
                "host": location.host,
                "port": location.port,
                "scheme": location.scheme,
            }
            for location in locations
        ],
        "grid_hash": _GJ_WARM_GRID_HASH,
    }


def warm_up(pygribjump=None):
    """Warm imports, cache state, and the active extraction data plane."""
    import zstandard  # noqa: F401  # type: ignore[import-not-found]

    _get_location_cache()
    if rust_extract_enabled() and pygribjump is None:
        return
    if PROC_POOL_SIZE > 0 and pygribjump is None:
        _get_process_pool()
        return
    if pygribjump is None:
        import pygribjump  # type: ignore[import-not-found]
    _get_gj_executor(pygribjump)


def _describe(field):
    return ",".join(f"{k}={v}" for k, v in field.items())


def build_requests(field_requests, spec, pygribjump):
    """One request-based ``ExtractionRequest`` per field, in FIELD_ORDER."""
    ranges = spec["ranges"]
    grid_hash = spec["grid_hash"]
    return [
        pygribjump.ExtractionRequest(field, list(ranges), gridHash=grid_hash)
        for field in field_requests
    ]


def _location_int(value, name):
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ExtractError(f"FDB location has invalid {name} {value!r}") from exc


def _location_from_element(element):
    if not element.has_location():
        raise ExtractError("FDB location lookup returned an entry without a location")
    uri = element.uri
    offset = element.offset()
    length = element.length()
    if uri is None or offset is None or length is None:
        raise ExtractError("FDB location lookup returned an incomplete location")
    path = uri.path()
    scheme = uri.scheme()
    if not scheme:
        # Local FDB list results are path-only URIs whose concrete storage
        # scheme is carried in the query, e.g. /data/file?internalScheme=file.
        values = parse_qs(str(uri.query() or "")).get("internalScheme", [])
        scheme = values[0] if values else ""
    if not path or not scheme:
        raise ExtractError(f"FDB location lookup returned invalid URI {uri!r}")
    # Preserve the FDB URI host. Local mn5 FDB listings expose a path-only URI
    # but encode the internal store alias in the archive filename.
    host = uri.hostname() or ""
    if not host:
        match = re.search(r"\.([A-Za-z0-9-]+\.novalocal)\.", path)
        host = match.group(1) if match else ""
    port = uri.port() or 0
    return FieldLocation(
        path=path,
        scheme=scheme,
        offset=_location_int(offset, "offset"),
        length=_location_int(length, "length"),
        host=host,
        port=_location_int(port, "port"),
    )


def _lookup_field_element_unlocked(field, pyfdb):
    """Resolve one FDB list element while the caller holds _fdb_list_lock."""
    try:
        iterator = iter(_get_fdb(pyfdb).list(field))
        first = next(iterator)
    except StopIteration as exc:
        raise ExtractError(f"field {_describe(field)}: FDB location lookup returned no fields") from exc
    except Exception as exc:
        raise ExtractError(f"field {_describe(field)}: FDB location lookup failed: {exc}") from exc
    try:
        next(iterator)
    except StopIteration:
        return first
    except Exception as exc:
        raise ExtractError(f"field {_describe(field)}: FDB location lookup failed: {exc}") from exc
    raise ExtractError(f"field {_describe(field)}: FDB location lookup returned multiple fields")


def _lookup_field_location(field, pyfdb):
    """Resolve exactly one FDB field to its path extraction location."""
    with _fdb_list_lock:
        first = _lookup_field_element_unlocked(field, pyfdb)
        try:
            return _location_from_element(first)
        except ExtractError as exc:
            raise ExtractError(f"field {_describe(field)}: {exc}") from exc
        except Exception as exc:
            raise ExtractError(
                f"field {_describe(field)}: FDB location lookup returned an invalid location: {exc}"
            ) from exc


def _batch_request_for_fields(fields):
    """Build the narrowest FDB selection representing a field sub-batch."""
    values = {}
    for field in fields:
        for key, value in field.items():
            if key in _NON_FIELD_KEYS:
                continue
            bucket = values.setdefault(key, [])
            if value not in bucket:
                bucket.append(value)
    return values


def _lookup_field_locations(fields, batch_request, pyfdb):
    """Resolve fields in serialized sub-batches, yielding the lock between each."""
    if len(fields) <= LOOKUP_SUBBATCH:
        batches = [(fields, batch_request)]
    else:
        batches = [
            (subset, _batch_request_for_fields(subset))
            for start in range(0, len(fields), LOOKUP_SUBBATCH)
            for subset in [fields[start : start + LOOKUP_SUBBATCH]]
        ]

    locations = {}
    for index, (subset, request) in enumerate(batches):
        with _fdb_list_lock:
            locations.update(_lookup_field_locations_unlocked(subset, request, pyfdb))
        if index + 1 < len(batches):
            # Let an already-waiting short job acquire the process lock before this
            # large lookup queues its next transaction.
            os.sched_yield()
    return locations


def _locations_from_elements(fields, iterator):
    """Join FDB list/inspect elements to requested fields by canonical metadata."""
    wanted = {canonical_field_key(field): field for field in fields}
    identity_names = set().union(*(field.keys() for field in fields)) - _NON_FIELD_KEYS
    locations = {}
    seen = set()
    while True:
        try:
            element = next(iterator)
        except StopIteration:
            break
        except Exception as exc:
            logging.warning("Batched FDB location lookup failed: %s", exc)
            break

        try:
            metadata = element.combined_key()
            # FDB may add derived schema keys (for example year/month from date).
            # Project onto the job field's identity before canonical comparison.
            identity = {name: metadata[name] for name in identity_names if name in metadata}
            key = canonical_field_key(identity)
        except Exception as exc:
            logging.warning("Ignoring FDB lookup element with invalid metadata: %s", exc)
            continue
        field = wanted.get(key)
        if field is None:
            continue
        if key in seen:
            locations.pop(key, None)
            logging.warning(
                "field %s: FDB location lookup returned multiple fields",
                _describe(field),
            )
            continue
        seen.add(key)

        try:
            locations[key] = _location_from_element(element)
        except Exception as exc:
            logging.warning("field %s: %s", _describe(field), exc)
    return locations


def _lookup_field_locations_unlocked(fields, batch_request, pyfdb):
    """Resolve fields through tolerant ``fdb.list(request)`` without a lock."""
    try:
        iterator = iter(_get_fdb(pyfdb).list(batch_request))
    except Exception as exc:
        logging.warning("Batched FDB location lookup failed: %s", exc)
        return {}
    return _locations_from_elements(fields, iterator)


def _reset_process_fdb(pyfdb):
    """Drop a poisoned child-local remote handle and create a fresh connection."""
    reset = getattr(pyfdb, "reset", None)
    if reset is None:
        raise ExtractError("process FDB handle cannot be reset after remote failure")
    reset()


def _list_process_locations(fields, batch_request, pyfdb):
    """Run tolerant list and defensively replace its handle on protocol failure."""
    elements = None
    error = None
    try:
        elements = list(_get_fdb(pyfdb).list(batch_request))
    except Exception as exc:
        error = str(exc)
    if elements is None:
        logging.warning(
            "Batched FDB list failed; resetting child FDB handle: %s", error
        )
        # Reset outside the exception scope so its traceback cannot retain the old
        # C++ handle while the replacement connection is constructed.
        _reset_process_fdb(pyfdb)
        return {}
    return _locations_from_elements(fields, iter(elements))


def _inspect_process_locations(fields, batch_request, pyfdb):
    """Prefer strict inspect, retrying the whole sub-batch through tolerant list."""
    elements = None
    error = None
    try:
        # Materialise before parsing: inspect can fail lazily when any field in the
        # selection cannot be resolved strictly. A partial inspect result is unsafe.
        elements = list(_get_fdb(pyfdb).inspect(batch_request))
    except Exception as exc:
        error = str(exc)
    if elements is None:
        logging.warning(
            "Batched FDB inspect failed; resetting child FDB handle before list: %s",
            error,
        )
        # See _list_process_locations: destroy after leaving the exception scope.
        _reset_process_fdb(pyfdb)
        return _list_process_locations(fields, batch_request, pyfdb), 1
    return _locations_from_elements(fields, iter(elements)), 0


def _field_number_of_data_points(field, pyfdb):
    """Read one field header through a process-serialized pyfdb transaction."""
    with _fdb_list_lock:
        return _field_number_of_data_points_unlocked(field, pyfdb)


def _field_number_of_data_points_unlocked(field, pyfdb):
    """Read numberOfDataPoints while the caller holds _fdb_list_lock."""
    element = _lookup_field_element_unlocked(field, pyfdb)
    # Lightweight test doubles can expose the decoded header directly.
    if hasattr(element, "number_of_data_points"):
        return _location_int(element.number_of_data_points(), "numberOfDataPoints")
    try:
        import eccodes  # type: ignore[import-not-found]

        length = _location_int(element.length(), "length")
        message = bytearray(length)
        view = memoryview(message)
        offset = 0
        with element.data_handle as handle:
            if handle is None:
                raise ExtractError("FDB list element has no data handle")
            while offset < length:
                read = handle.readinto(view[offset:])
                if read <= 0:
                    raise ExtractError(
                        f"short GRIB read while checking grid: wanted {length}, got {offset}"
                    )
                offset += read
        gid = eccodes.codes_new_from_message(bytes(message))
        try:
            return _location_int(
                eccodes.codes_get(gid, "numberOfDataPoints"), "numberOfDataPoints"
            )
        finally:
            eccodes.codes_release(gid)
    except ExtractError:
        raise
    except Exception as exc:
        raise ExtractError(
            f"field {_describe(field)}: could not read numberOfDataPoints: {exc}"
        ) from exc


def _hash_identity(field):
    return tuple(field.get(key) for key in _HASH_IDENTITY_KEYS)


def _matching_hash_override(identity):
    with _hash_overrides_lock:
        exact = _hash_overrides.get(identity)
        if exact is not None:
            source = "learned" if identity in _learned_hash_identities else "seed"
            return exact, source
        for pattern, grid_hash in _hash_overrides.items():
            if all(want is None or want == got for want, got in zip(pattern, identity)):
                return grid_hash, "seed"
    return None, None


def _apply_hash_override(field, spec):
    identity = _hash_identity(field)
    grid_hash, source = _matching_hash_override(identity)
    if grid_hash is not None and grid_hash != spec["grid_hash"]:
        logging.info(
            "chunks-grid-hash registry-hit source=%s identity=%s registry=%s selected=%s",
            source,
            ",".join(
                f"{key}={value or '-'}" for key, value in zip(_HASH_IDENTITY_KEYS, identity)
            ),
            spec["grid_hash"],
            grid_hash,
        )
        spec["grid_hash"] = grid_hash


def _hash_learning_enabled():
    return os.environ.get("POLYTOPE_CHUNKS_HASH_LEARN", "1").strip() != "0"


def _grid_mismatch_error(field, expected, found, job_id):
    identity = ", ".join(
        f"{key}={field.get(key, '-')}" for key in _HASH_IDENTITY_KEYS
    )
    request_id = job_id or "unknown"
    return ExtractError(
        f"Grid hash mismatch for {identity}: expected {expected}, found {found}; "
        f"server-side grid-registry gap — report with your request ID: {request_id}"
    )


def _retry_learned_hash(operation, field, job_id, exception_type):
    try:
        return operation()
    except Exception as retry_exc:
        return _handle_learned_hash_retry_error(
            retry_exc, field, job_id, exception_type
        )


def _handle_learned_hash_retry_error(retry_exc, field, job_id, exception_type):
    retry_match = _GRID_HASH_MISMATCH_RE.search(str(retry_exc))
    if retry_match and retry_exc.__class__ is exception_type:
        retry_expected, retry_found = retry_match.groups()
        logging.error(
            "gribjump grid hash mismatch after learned-hash retry: %s", retry_exc
        )
        raise _grid_mismatch_error(
            field, retry_expected, retry_found, job_id
        ) from retry_exc
    raise retry_exc


def _learn_hash_and_retry(exc, operation, field, spec, pygribjump, pyfdb, job_id):
    exception_type = getattr(pygribjump, "GribJumpException", None)
    match = _GRID_HASH_MISMATCH_RE.search(str(exc))
    if not match or exc.__class__ is not exception_type:
        raise exc
    expected, found = match.groups()
    logging.error("gribjump grid hash mismatch (raw, request_id=%s): %s", job_id or "-", exc)
    if not _hash_learning_enabled():
        raise _grid_mismatch_error(field, expected, found, job_id) from exc

    registry_hash = spec.get("registry_grid_hash") or expected
    registry_count = _REGISTRY_COUNTS_BY_HASH.get(registry_hash)
    if registry_count is None:
        logging.error(
            "cannot safety-learn grid hash for uncounted registry hash %s", registry_hash
        )
        raise _grid_mismatch_error(field, expected, found, job_id) from exc
    if pyfdb is None:
        import pyfdb as imported_pyfdb  # type: ignore[import-not-found]

        pyfdb = imported_pyfdb
    actual_count = _field_number_of_data_points(field, pyfdb)
    if actual_count != registry_count:
        logging.error(
            "refusing grid hash learn: registry count_values=%d header numberOfDataPoints=%d",
            registry_count,
            actual_count,
        )
        raise _grid_mismatch_error(field, expected, found, job_id) from exc

    identity = _hash_identity(field)
    with _hash_overrides_lock:
        prior = _hash_overrides.get(identity)
        if prior is not None and prior != found:
            logging.error(
                "refusing conflicting grid hash learn for %s: cached=%s found=%s",
                identity,
                prior,
                found,
            )
            raise _grid_mismatch_error(field, expected, found, job_id) from exc
        _hash_overrides[identity] = found
        _learned_hash_identities.add(identity)
    logging.warning(
        "chunks-grid-hash learned identity=%s expected=%s found=%s count_values=%d request_id=%s",
        ",".join(
            f"{key}={value or '-'}" for key, value in zip(_HASH_IDENTITY_KEYS, identity)
        ),
        expected,
        found,
        registry_count,
        job_id or "-",
    )
    spec["grid_hash"] = found
    return _retry_learned_hash(operation, field, job_id, exception_type)


def _extract_with_hash_learning(operation, field, spec, pygribjump, pyfdb, job_id):
    """Run a gribjump operation, safety-learn one mismatch, and retry once."""
    try:
        return operation()
    except Exception as exc:
        return _learn_hash_and_retry(
            exc, operation, field, spec, pygribjump, pyfdb, job_id
        )


def _extract_trace_enabled():
    return os.environ.get("POLYTOPE_CHUNKS_EXTRACT_TRACE", "").strip() == "1"


def _extract_echo_enabled():
    return os.environ.get("POLYTOPE_CHUNKS_EXTRACT_ECHO", "").strip() == "1"


def _extract_locations(
    gj,
    pygribjump,
    locations,
    spec,
    ctx,
    *,
    trace_job=None,
    trace_sub=0,
    trace_retry=0,
):
    trace = _extract_trace_enabled()
    phase = "build"
    status = "error"
    build_started = time.monotonic()
    build_finished = call_started = call_finished = consume_finished = None
    watchdog_armed = False
    try:
        requests = [
            pygribjump.PathExtractionRequest(
                location.path,
                location.scheme,
                location.offset,
                location.host,
                location.port,
                list(spec["ranges"]),
                gridHash=spec["grid_hash"],
            )
            for location in locations
        ]
        build_finished = time.monotonic()
        if trace:
            print(
                f"extract-watchdog arm job={trace_job or '-'} sub={trace_sub} "
                f"retry={trace_retry}",
                file=sys.stderr,
                flush=True,
            )
            with suppress(RuntimeError):
                faulthandler.dump_traceback_later(2, repeat=True)
                watchdog_armed = True

        phase = "call"
        call_started = time.monotonic()
        iterator = (
            gj.extract_from_paths(requests, ctx=ctx)
            if ctx is not None
            else gj.extract_from_paths(requests)
        )
        call_finished = time.monotonic()
        phase = "consume"
        # Materialise inside the retry boundary: the C iterator can also raise.
        results = list(iterator)
        consume_finished = time.monotonic()
        status = "ok"
        return results
    finally:
        finished = time.monotonic()
        if watchdog_armed:
            faulthandler.cancel_dump_traceback_later()
        if trace:
            if build_finished is None:
                build_finished = finished
            if call_started is None:
                call_started = build_finished
            if call_finished is None:
                call_finished = finished if phase == "call" else call_started
            if consume_finished is None:
                consume_finished = finished if phase == "consume" else call_finished
            print(
                f"extract-trace job={trace_job or '-'} sub={trace_sub} "
                f"build={build_finished - build_started:.6f} "
                f"call={call_finished - call_started:.6f} "
                f"consume={consume_finished - call_finished:.6f} "
                f"retry={trace_retry} status={status}",
                file=sys.stderr,
                flush=True,
            )
            print(
                f"extract-watchdog disarm job={trace_job or '-'} sub={trace_sub} "
                f"retry={trace_retry}",
                file=sys.stderr,
                flush=True,
            )

def assemble(results, field_requests, spec):
    """Copy gribjump results into one contiguous little-endian float64 array.

    ``results`` is the iterator returned by ``GribJump.extract``. With the real
    pygribjump the remote extraction happens inside ``extract()`` itself; the
    iteration here only walks the already-materialised results and copies them.
    """
    ranges = spec["ranges"]
    expected = [hi - lo for lo, hi in ranges]
    per_field = sum(expected)
    nfields = len(field_requests)
    out = np.empty(nfields * per_field, dtype="<f8")

    n = 0
    for i, result in enumerate(results):
        if i >= nfields:
            raise ExtractError(
                f"gribjump returned more results than requested fields ({nfields})"
            )
        # result.values: one array per range, in request range order; these
        # are views into memory owned by `result`, so copy immediately.
        values = result.values
        if len(values) != len(ranges):
            raise ExtractError(
                f"field {_describe(field_requests[i])}: gribjump returned "
                f"{len(values)} ranges, expected {len(ranges)}"
            )
        base = i * per_field
        off = 0
        for j, (arr, count) in enumerate(zip(values, expected)):
            arr = np.asarray(arr)
            if arr.shape != (count,):
                raise ExtractError(
                    f"field {_describe(field_requests[i])}: range {list(ranges[j])} "
                    f"returned {arr.size} values, expected {count}"
                )
            out[base + off: base + off + count] = arr
            off += count
        n += 1

    if n != nfields:
        missing = field_requests[n] if n < nfields else None
        raise ExtractError(
            f"gribjump returned {n} of {nfields} fields; missing field "
            f"{_describe(missing) if missing else '?'}"
        )
    return out


def byte_shuffle(values) -> bytes:
    """Regroup bytes by byte position across every element (Blosc semantics)."""
    if not values.flags.c_contiguous:
        values = np.ascontiguousarray(values)
    return values.view(np.uint8).reshape(-1, values.dtype.itemsize).T.tobytes()


def extract_raw(field_requests, spec, pygribjump=None, ctx=None):
    """Run gribjump and return the uncompressed little-endian float64 payload."""
    if pygribjump is None:
        import pygribjump  # type: ignore[import-not-found]
    requests = build_requests(field_requests, spec, pygribjump)
    try:
        def operation(gj):
            results = (
                gj.extract(requests, ctx=ctx) if ctx is not None else gj.extract(requests)
            )
            return assemble(results, field_requests, spec).tobytes()

        return _run_gribjump(operation, pygribjump)
    except ExtractError:
        raise
    except Exception as exc:  # GribJumpException and anything else -> job failure
        raise ExtractError(f"gribjump extraction failed: {exc}") from exc


def compress(raw) -> bytes:
    """One zstd frame over ``raw`` (bytes or any C-contiguous buffer)."""
    import zstandard  # type: ignore[import-not-found]

    return zstandard.ZstdCompressor(level=ZSTD_LEVEL, write_content_size=True).compress(raw)


def _ms(a, b):
    return round((b - a) * 1000, 1)


def _log_profile(prof):
    """Emit the single per-job ``chunks-profile`` line (key=value, grep-able)."""
    logging.info(
        "chunks-profile job=%s status=%s phase=%s proc=%d fields=%d ranges=%d points=%d "
        "dtype=%s shuffle=%d cache=%d/%d fallback=%d lookup_mode=%s "
        "lookup_fallbacks=%d subbatches=%d/%d t_lookup=%.1fms "
        "t_parse=%.1fms t_enum=%.1fms t_extract=%.1fms t_assemble=%.1fms "
        "t_shuffle=%.1fms t_zstd=%.1fms t_total=%.1fms "
        "raw_bytes=%d bytes=%d zstd_level=%d",
        prof["job"],
        prof["status"],
        prof["phase"],
        prof.get("proc", 0),
        prof["fields"],
        prof["ranges"],
        prof["points"],
        prof["dtype"],
        prof["shuffle"],
        prof["cache_hits"],
        prof["cache_misses"],
        prof["fallbacks"],
        prof["lookup_mode"],
        prof["lookup_fallbacks"],
        prof["lookup_subbatches"],
        prof["gj_subbatches"],
        prof["lookup_ms"],
        prof["parse_ms"],
        prof["enum_ms"],
        prof["extract_ms"],
        prof["assemble_ms"],
        prof["shuffle_ms"],
        prof["compress_ms"],
        prof["total_ms"],
        prof["raw_bytes"],
        prof["payload_bytes"],
        ZSTD_LEVEL,
    )


def _run_extract_fallback(request, pygribjump=None, pyfdb=None, user=None, job_id=None):
    """Serve an extract job. Returns ``(payload_bytes, content_type, timings)``."""
    if pygribjump is None:
        import pygribjump  # type: ignore[import-not-found]

    prof = {
        "job": job_id or "-",
        "status": "error",
        "phase": "parse",
        "proc": 0,
        "fields": 0,
        "ranges": 0,
        "points": 0,
        "dtype": "f32",
        "shuffle": 1,
        "cache_hits": 0,
        "cache_misses": 0,
        "fallbacks": 0,
        "lookup_fallbacks": 0,
        "lookup_subbatches": 0,
        "gj_subbatches": 0,
        "lookup_ms": 0.0,
        "lookup_mode": "none",
        "parse_ms": 0.0,
        "enum_ms": 0.0,
        "extract_ms": 0.0,
        "assemble_ms": 0.0,
        "shuffle_ms": 0.0,
        "compress_ms": 0.0,
        "total_ms": 0.0,
        "raw_bytes": 0,
        "payload_bytes": 0,
    }
    t0 = time.monotonic()
    try:
        spec, field_values = parse_extract(request)
        spec["registry_grid_hash"] = spec["grid_hash"]
        t_parse = time.monotonic()
        prof["parse_ms"] = _ms(t0, t_parse)
        prof["dtype"] = "f32" if spec["dtype"] == "float32" else "f64"
        prof["shuffle"] = int(spec["shuffle"])

        prof["phase"] = "enum"
        fields = list(enumerate_fields(field_values, spec["order"]))
        prof["fields"] = len(fields)
        prof["ranges"] = len(spec["ranges"])
        prof["points"] = len(fields) * sum(hi - lo for lo, hi in spec["ranges"])
        ctx = None
        if user is not None:
            ctx = {"user": f"{getattr(user, 'realm', '')}:{getattr(user, 'username', '')}"}
            if job_id:
                ctx["job_id"] = job_id
        _apply_hash_override(fields[0], spec)
        cache = _get_location_cache()
        t_enum = time.monotonic()
        prof["enum_ms"] = _ms(t_parse, t_enum)

        def lookup(field):
            nonlocal pyfdb
            prof["phase"] = "lookup"
            if prof["lookup_mode"] == "none":
                prof["lookup_mode"] = "single"
            prof["lookup_subbatches"] += 1
            started = time.monotonic()
            try:
                if pyfdb is None:
                    import pyfdb as imported_pyfdb  # type: ignore[import-not-found]

                    pyfdb = imported_pyfdb
                return _lookup_field_location(field, pyfdb)
            finally:
                prof["lookup_ms"] += _ms(started, time.monotonic())

        def lookup_batch(missing_fields):
            nonlocal pyfdb
            prof["phase"] = "lookup"
            prof["lookup_mode"] = "batch"
            prof["lookup_subbatches"] += max(
                1, (len(missing_fields) + LOOKUP_SUBBATCH - 1) // LOOKUP_SUBBATCH
            )
            started = time.monotonic()
            try:
                if pyfdb is None:
                    import pyfdb as imported_pyfdb  # type: ignore[import-not-found]

                    pyfdb = imported_pyfdb
                batch_request = {key: list(values) for key, values in field_values.items()}
                return _lookup_field_locations(missing_fields, batch_request, pyfdb)
            finally:
                prof["lookup_ms"] += _ms(started, time.monotonic())

        try:
            prof["phase"] = "extract"
            if not cache.enabled:
                def extract_requests(gj):
                    def operation():
                        requests = build_requests(fields, spec, pygribjump)
                        return list(
                            gj.extract(requests, ctx=ctx)
                            if ctx is not None
                            else gj.extract(requests)
                        )

                    return _extract_with_hash_learning(
                        operation, fields[0], spec, pygribjump, pyfdb, job_id
                    )

                results = _run_gribjump(extract_requests, pygribjump)
                t_extract = time.monotonic()
                prof["extract_ms"] = _ms(t_enum, t_extract)
            else:
                locations: list[FieldLocation | None] = [None] * len(fields)
                fallback_indices = set()
                missing = []
                for index, field in enumerate(fields):
                    location = cache.get(field)
                    if location is None:
                        prof["cache_misses"] += 1
                        missing.append((index, field))
                    else:
                        prof["cache_hits"] += 1
                        locations[index] = location

                if len(missing) == 1:
                    index, field = missing[0]
                    try:
                        location = lookup(field)
                    except ExtractError:
                        fallback_indices.add(index)
                    else:
                        cache.put(field, location)
                        locations[index] = location
                elif missing:
                    batch_locations = lookup_batch([field for _, field in missing])
                    inserts = []
                    for index, field in missing:
                        location = batch_locations.get(canonical_field_key(field))
                        if location is None:
                            fallback_indices.add(index)
                        else:
                            locations[index] = location
                            inserts.append((field, location))
                    cache.put_many(inserts)

                servermap = _get_location_servermap()
                results_by_index = [None] * len(fields)
                path_items = []
                for index, (field, location) in enumerate(zip(fields, locations)):
                    if location is None:
                        continue
                    translated = servermap.translate(location)
                    if translated is None:
                        fallback_indices.add(index)
                    else:
                        path_items.append((index, field, translated))

                prof["phase"] = "extract"
                extract_seconds = 0.0

                def extract_paths(items):
                    nonlocal extract_seconds
                    started = time.monotonic()
                    try:
                        size = GJ_SUBBATCH or len(items)
                        for start in range(0, len(items), size):
                            batch = items[start : start + size]
                            prof["gj_subbatches"] += 1
                            def extract_batch(gj):
                                return _extract_with_hash_learning(
                                    lambda: _extract_locations(
                                        gj,
                                        pygribjump,
                                        [item[2] for item in batch],
                                        spec,
                                        ctx,
                                    ),
                                    batch[0][1],
                                    spec,
                                    pygribjump,
                                    pyfdb,
                                    job_id,
                                )

                            extracted = _run_gribjump(extract_batch, pygribjump)
                            for item, result in zip(batch, extracted):
                                results_by_index[item[0]] = result
                    finally:
                        extract_seconds += time.monotonic() - started

                if path_items:
                    try:
                        extract_paths(path_items)
                    except ExtractError:
                        raise
                    except Exception:
                        # Refresh every path candidate because the path API reports
                        # only a batch failure, then retry the routable subset once.
                        fresh_items = []
                        for index, field, _location in path_items:
                            cache.invalidate(field)
                            try:
                                fresh_location = lookup(field)
                            except ExtractError:
                                fallback_indices.add(index)
                                continue
                            cache.put(field, fresh_location)
                            translated = servermap.translate(fresh_location)
                            if translated is None:
                                fallback_indices.add(index)
                            else:
                                fresh_items.append((index, field, translated))
                        prof["phase"] = "extract"
                        if fresh_items:
                            try:
                                extract_paths(fresh_items)
                            except ExtractError:
                                raise
                            except Exception as retry_exc:
                                logging.warning(
                                    "gribjump location extraction failed after refresh; "
                                    "falling back for %d field(s): %s",
                                    len(fresh_items),
                                    retry_exc,
                                )
                                fallback_indices.update(item[0] for item in fresh_items)

                if fallback_indices:
                    ordered_indices = sorted(fallback_indices)
                    logging.warning(
                        "Cached FDB location route unavailable; falling back to "
                        "request extraction for %d field(s)",
                        len(ordered_indices),
                    )
                    fallback_fields = [fields[index] for index in ordered_indices]
                    started = time.monotonic()
                    try:
                        def extract_fallback(gj):
                            def operation():
                                fallback_requests = build_requests(
                                    fallback_fields, spec, pygribjump
                                )
                                return list(
                                    gj.extract(fallback_requests, ctx=ctx)
                                    if ctx is not None
                                    else gj.extract(fallback_requests)
                                )

                            return _extract_with_hash_learning(
                                operation,
                                fallback_fields[0],
                                spec,
                                pygribjump,
                                pyfdb,
                                job_id,
                            )

                        fallback_results = _run_gribjump(
                            extract_fallback, pygribjump
                        )
                    finally:
                        extract_seconds += time.monotonic() - started
                    for index, result in zip(ordered_indices, fallback_results):
                        results_by_index[index] = result
                    prof["fallbacks"] = len(ordered_indices)

                results = results_by_index
                t_extract = time.monotonic()
                prof["extract_ms"] = round(extract_seconds * 1000, 1)

            prof["phase"] = "assemble"
            out = assemble(results, fields, spec)
            t_assemble = time.monotonic()
            prof["assemble_ms"] = _ms(t_extract, t_assemble)
        except ExtractError:
            raise
        except Exception as exc:  # GribJumpException and anything else -> job failure
            raise ExtractError(f"gribjump extraction failed: {exc}") from exc

        prof["phase"] = "shuffle"
        dtype = "<f4" if spec["dtype"] == "float32" else "<f8"
        wire_values = out.astype(dtype, copy=False)
        prof["raw_bytes"] = wire_values.nbytes
        wire = byte_shuffle(wire_values) if spec["shuffle"] else wire_values
        t_shuffle = time.monotonic()
        prof["shuffle_ms"] = _ms(t_assemble, t_shuffle)

        prof["phase"] = "zstd"
        payload = compress(wire)
        t_zstd = time.monotonic()
        prof["compress_ms"] = _ms(t_shuffle, t_zstd)
        prof["payload_bytes"] = len(payload)
        prof["status"] = "ok"
        prof["phase"] = "done"
    finally:
        prof["total_ms"] = _ms(t0, time.monotonic())
        _log_profile(prof)

    timings = {
        "parse_ms": prof["parse_ms"],
        "enum_ms": prof["enum_ms"],
        "lookup_ms": prof["lookup_ms"],
        "lookup_mode": prof["lookup_mode"],
        "lookup_fallbacks": prof["lookup_fallbacks"],
        "extract_ms": prof["extract_ms"],
        "assemble_ms": prof["assemble_ms"],
        "shuffle_ms": prof["shuffle_ms"],
        "compress_ms": prof["compress_ms"],
        "retrieve_ms": prof["total_ms"],
        "fields": prof["fields"],
        "points": prof["points"],
        "cache_hits": prof["cache_hits"],
        "cache_misses": prof["cache_misses"],
        "fallbacks": prof["fallbacks"],
        "lookup_subbatches": prof["lookup_subbatches"],
        "gj_subbatches": prof["gj_subbatches"],
        "raw_bytes": prof["raw_bytes"],
        "payload_bytes": prof["payload_bytes"],
    }
    return payload, CONTENT_TYPE, timings


class _FDBHandleModule:
    """pyfdb facade owning one resettable, child-local remote FDB handle."""

    def __init__(self, handle, factory=None):
        self.handle = handle
        self.factory = factory

    def FDB(self):  # noqa: N802 - mirrors pyfdb
        return self.handle

    def reset(self):
        if self.factory is None:
            raise ExtractError("process FDB handle has no reset factory")
        self.handle = None
        gc.collect()
        self.handle = self.factory()


def _lookup_process_locations_with_stats(fields, batch_request, pyfdb):
    """Resolve process-worker misses via inspect-first sub-batches without a lock."""
    locations = {}
    fallback_subbatches = 0
    for start in range(0, len(fields), LOOKUP_SUBBATCH):
        subset = fields[start : start + LOOKUP_SUBBATCH]
        request = (
            batch_request
            if len(fields) <= LOOKUP_SUBBATCH
            else _batch_request_for_fields(subset)
        )
        batch_locations, fallback_count = _inspect_process_locations(
            subset, request, pyfdb
        )
        locations.update(batch_locations)
        fallback_subbatches += fallback_count
    return locations, fallback_subbatches


def _lookup_process_locations(fields, batch_request, pyfdb):
    locations, _fallback_subbatches = _lookup_process_locations_with_stats(
        fields, batch_request, pyfdb
    )
    return locations


def _lookup_process_location(field, pyfdb):
    """Resolve one field on a subprocess-owned FDB handle without a lock."""
    first = _lookup_field_element_unlocked(field, pyfdb)
    try:
        return _location_from_element(first)
    except ExtractError as exc:
        raise ExtractError(f"field {_describe(field)}: {exc}") from exc
    except Exception as exc:
        raise ExtractError(
            f"field {_describe(field)}: FDB location lookup returned an invalid location: {exc}"
        ) from exc


def _resolve_process_warm_locations(pyfdb, servermap):
    fields = [{**_GJ_WARM_FIELD, "date": date} for date in _GJ_WARM_DATES]
    locations_by_key = _lookup_process_locations(
        fields, _batch_request_for_fields(fields), pyfdb
    )
    locations = {}
    for field in fields:
        location = locations_by_key.get(canonical_field_key(field))
        translated = servermap.translate(location) if location is not None else None
        if translated is not None:
            endpoint = (translated.scheme, translated.host, translated.port)
            locations.setdefault(endpoint, translated)
    if not locations:
        raise ExtractError("GribJump warm-up fields have no routable locations")
    return list(locations.values())


def _execute_process_job(job, pygribjump, pyfdb, gj, servermap):
    """Run one complete heavy extraction job on one subprocess-owned handle pair.

    This function is intentionally importable and accepts fake modules/handles so its
    payload can be compared directly with fallback mode in unit tests.
    """
    fields = job["fields"]
    field_values = job["field_values"]
    spec = job["spec"]
    locations = list(job["locations"])
    cache_enabled = job["cache_enabled"]
    ctx = job.get("ctx")
    job_id = job.get("job_id")
    profile = {
        "lookup_ms": 0.0,
        "lookup_mode": "none",
        "lookup_subbatches": 0,
        "lookup_fallbacks": 0,
        "gj_subbatches": 0,
        "extract_ms": 0.0,
        "assemble_ms": 0.0,
        "shuffle_ms": 0.0,
        "compress_ms": 0.0,
        "fallbacks": 0,
        "raw_bytes": 0,
        "payload_bytes": 0,
    }
    updates = []
    invalidations = []
    echo_runner = lambda: None

    try:
        if not cache_enabled:
            started = time.monotonic()

            def operation():
                requests = build_requests(fields, spec, pygribjump)
                iterator = (
                    gj.extract(requests, ctx=ctx)
                    if ctx is not None
                    else gj.extract(requests)
                )
                return list(iterator)

            results = _extract_with_hash_learning(
                operation, fields[0], spec, pygribjump, pyfdb, job_id
            )
            profile["extract_ms"] = round(
                (time.monotonic() - started) * 1000, 1
            )

            def echo_requests():
                requests = build_requests(fields, spec, pygribjump)
                return list(gj.extract(requests))

            echo_runner = echo_requests
        else:
            missing = [
                (index, field)
                for index, (field, location) in enumerate(zip(fields, locations))
                if location is None
            ]
            fallback_indices = set()
            fallback_fields = []
            if missing and not job.get("locations_resolved", False):
                started = time.monotonic()
                if len(missing) == 1:
                    profile["lookup_mode"] = "single"
                    profile["lookup_subbatches"] = 1
                    index, field = missing[0]
                    try:
                        location = _lookup_process_location(field, pyfdb)
                    except ExtractError:
                        fallback_indices.add(index)
                    else:
                        locations[index] = location
                        updates.append((field, location))
                else:
                    profile["lookup_mode"] = "batch"
                    profile["lookup_subbatches"] = max(
                        1, (len(missing) + LOOKUP_SUBBATCH - 1) // LOOKUP_SUBBATCH
                    )
                    batch_locations, fallback_count = _lookup_process_locations_with_stats(
                        [field for _, field in missing],
                        {key: list(values) for key, values in field_values.items()},
                        pyfdb,
                    )
                    profile["lookup_fallbacks"] += fallback_count
                    for index, field in missing:
                        location = batch_locations.get(canonical_field_key(field))
                        if location is None:
                            fallback_indices.add(index)
                        else:
                            locations[index] = location
                            updates.append((field, location))
                profile["lookup_ms"] += round(
                    (time.monotonic() - started) * 1000, 1
                )
            elif missing:
                # Parent-side pool lookup has already had its one attempt. Any field
                # still unresolved follows the existing request-extraction fallback.
                fallback_indices.update(index for index, _field in missing)

            results_by_index = [None] * len(fields)
            path_items = []
            for index, (field, location) in enumerate(zip(fields, locations)):
                if location is None:
                    continue
                translated = servermap.translate(location)
                if translated is None:
                    fallback_indices.add(index)
                else:
                    path_items.append((index, field, translated))

            extract_seconds = 0.0

            def extract_paths(items, retry=0):
                nonlocal extract_seconds
                started = time.monotonic()
                try:
                    size = GJ_SUBBATCH or len(items)
                    for start in range(0, len(items), size):
                        batch = items[start : start + size]
                        sub = start // size
                        profile["gj_subbatches"] += 1
                        trace_retry = retry

                        def operation():
                            nonlocal trace_retry
                            current_retry = trace_retry
                            try:
                                return _extract_locations(
                                    gj,
                                    pygribjump,
                                    [item[2] for item in batch],
                                    spec,
                                    ctx,
                                    trace_job=job_id,
                                    trace_sub=sub,
                                    trace_retry=current_retry,
                                )
                            finally:
                                trace_retry += 1

                        extracted = _extract_with_hash_learning(
                            operation,
                            batch[0][1],
                            spec,
                            pygribjump,
                            pyfdb,
                            job_id,
                        )
                        for item, result in zip(batch, extracted):
                            results_by_index[item[0]] = result
                finally:
                    extract_seconds += time.monotonic() - started

            if path_items:
                try:
                    extract_paths(path_items)
                except ExtractError:
                    raise
                except Exception:
                    fresh_items = []
                    lookup_started = time.monotonic()
                    for index, field, _location in path_items:
                        invalidations.append(field)
                        try:
                            fresh_location = _lookup_process_location(field, pyfdb)
                        except ExtractError:
                            fallback_indices.add(index)
                            continue
                        locations[index] = fresh_location
                        updates.append((field, fresh_location))
                        translated = servermap.translate(fresh_location)
                        if translated is None:
                            fallback_indices.add(index)
                        else:
                            fresh_items.append((index, field, translated))
                    profile["lookup_ms"] += round(
                        (time.monotonic() - lookup_started) * 1000, 1
                    )
                    profile["lookup_subbatches"] += len(path_items)
                    if fresh_items:
                        try:
                            extract_paths(fresh_items, retry=1)
                        except ExtractError:
                            raise
                        except Exception as retry_exc:
                            logging.warning(
                                "gribjump location extraction failed after refresh; "
                                "falling back for %d field(s): %s",
                                len(fresh_items),
                                retry_exc,
                            )
                            fallback_indices.update(
                                item[0] for item in fresh_items
                            )

            if fallback_indices:
                ordered_indices = sorted(fallback_indices)
                fallback_fields = [fields[index] for index in ordered_indices]
                started = time.monotonic()

                def fallback_operation():
                    requests = build_requests(
                        fallback_fields, spec, pygribjump
                    )
                    iterator = (
                        gj.extract(requests, ctx=ctx)
                        if ctx is not None
                        else gj.extract(requests)
                    )
                    return list(iterator)

                try:
                    fallback_results = _extract_with_hash_learning(
                        fallback_operation,
                        fallback_fields[0],
                        spec,
                        pygribjump,
                        pyfdb,
                        job_id,
                    )
                finally:
                    extract_seconds += time.monotonic() - started
                for index, result in zip(ordered_indices, fallback_results):
                    results_by_index[index] = result
                profile["fallbacks"] = len(ordered_indices)

            results = results_by_index
            profile["extract_ms"] = round(extract_seconds * 1000, 1)

            echo_locations = []
            for index, location in enumerate(locations):
                if location is None or index in fallback_indices:
                    continue
                translated = servermap.translate(location)
                if translated is not None and results_by_index[index] is not None:
                    echo_locations.append(translated)
            echo_fallback_fields = list(fallback_fields)

            def echo_locations_once():
                size = GJ_SUBBATCH or len(echo_locations) or 1
                for start in range(0, len(echo_locations), size):
                    _extract_locations(
                        gj,
                        pygribjump,
                        echo_locations[start : start + size],
                        spec,
                        None,
                    )
                if echo_fallback_fields:
                    requests = build_requests(
                        echo_fallback_fields, spec, pygribjump
                    )
                    list(gj.extract(requests))

            echo_runner = echo_locations_once

        if _extract_echo_enabled():
            echo_started = time.monotonic()
            echo_runner()
            echo_ms = round((time.monotonic() - echo_started) * 1000, 1)
            logging.info(
                "extract-echo job=%s first_ms=%.1f echo_ms=%.1f",
                job_id or "-",
                profile["extract_ms"],
                echo_ms,
            )

        started = time.monotonic()
        out = assemble(results, fields, spec)
        profile["assemble_ms"] = round(
            (time.monotonic() - started) * 1000, 1
        )

        started = time.monotonic()
        dtype = "<f4" if spec["dtype"] == "float32" else "<f8"
        wire_values = out.astype(dtype, copy=False)
        profile["raw_bytes"] = wire_values.nbytes
        wire = byte_shuffle(wire_values) if spec["shuffle"] else wire_values
        profile["shuffle_ms"] = round(
            (time.monotonic() - started) * 1000, 1
        )

        started = time.monotonic()
        payload = compress(wire)
        profile["compress_ms"] = round(
            (time.monotonic() - started) * 1000, 1
        )
        profile["payload_bytes"] = len(payload)
        return {
            "payload": payload,
            "profile": profile,
            "updates": updates,
            "invalidations": invalidations,
        }
    except ExtractError:
        raise
    except Exception as exc:
        raise ExtractError(f"gribjump extraction failed: {exc}") from exc


def _process_worker_main(connection, slot):
    """Spawn target: own an FDB handle; Python fallback also owns GribJump."""
    startup_started = time.monotonic()
    try:
        os.environ["GRIBJUMP_CONFIG_FILE"] = "/tmp/gribjump.yaml"
        import pyfdb  # type: ignore[import-not-found]

        fdb_module = _FDBHandleModule(pyfdb.FDB(), pyfdb.FDB)
        servermap = LocationServerMap.from_config("/tmp/gribjump.yaml")
        pygribjump = None
        gj = None
        warm_locations = []
        if not rust_extract_enabled():
            import pygribjump as imported_pygribjump  # type: ignore[import-not-found]

            pygribjump = imported_pygribjump
            gj = pygribjump.GribJump()
            warm_locations = _resolve_process_warm_locations(fdb_module, servermap)
            _warm_gribjump_handle(gj, pygribjump, warm_locations)
        warm_seconds = time.monotonic() - startup_started
        warm_endpoints = [
            f"{location.scheme}://{location.host}:{location.port}"
            for location in warm_locations
        ]
        if _extract_trace_enabled():
            print(
                f"extract-trace startup slot={slot} pid={os.getpid()} "
                f"endpoints={','.join(warm_endpoints)} warm={warm_seconds:.6f}",
                file=sys.stderr,
                flush=True,
            )
        connection.send(
            (
                "ready",
                {
                    "slot": slot,
                    "pid": os.getpid(),
                    "env": os.environ.get("GRIBJUMP_CONFIG_FILE"),
                    "warm_endpoints": len(warm_locations),
                    "warm_endpoint_names": warm_endpoints,
                    "warm_ms": round(warm_seconds * 1000, 1),
                },
            )
        )
    except BaseException as exc:
        try:
            connection.send(("startup_error", repr(exc)))
        finally:
            connection.close()
        return

    while True:
        try:
            command = connection.recv()
        except EOFError:
            break
        if command is None:
            break
        try:
            if command.get("task") == "lookup":
                locations, fallback_count = _lookup_process_locations_with_stats(
                    command["fields"], command["batch_request"], fdb_module
                )
                response = {
                    "locations": locations,
                    "fallbacks": fallback_count,
                }
            elif pygribjump is not None and gj is not None:
                response = _execute_process_job(
                    command, pygribjump, fdb_module, gj, servermap
                )
            else:
                raise ExtractError("Rust-mode subprocess received an extraction job")
            connection.send(("ok", response))
        except BaseException as exc:
            connection.send(
                (
                    "error",
                    {
                        "type": type(exc).__name__,
                        "message": getattr(exc, "message", str(exc)),
                    },
                )
            )
    connection.close()


def _pool_test_worker(connection, slot):
    """Small spawn-safe protocol worker used by the multiprocessing tests."""
    connection.send(
        (
            "ready",
            {
                "slot": slot,
                "pid": os.getpid(),
                "env": os.environ.get("GRIBJUMP_CONFIG_FILE"),
                "warm_endpoints": 0,
            },
        )
    )
    while True:
        try:
            command = connection.recv()
        except EOFError:
            break
        if command is None:
            break
        mode = command.get("mode", "echo")
        if mode == "sleep":
            threading.Event().wait(command.get("seconds", 1.0))
        elif mode == "exit":
            os._exit(17)
        if command.get("task") == "lookup":
            if command.get("fail"):
                connection.send(
                    ("error", {"type": "RuntimeError", "message": "lookup failed"})
                )
                continue
            connection.send(
                (
                    "ok",
                    {
                        "slot": slot,
                        "pid": os.getpid(),
                        "locations": command.get("locations", {}),
                        "fallbacks": command.get("fallbacks", 0),
                    },
                )
            )
            continue
        connection.send(
            (
                "ok",
                {
                    "slot": slot,
                    "pid": os.getpid(),
                    "value": command.get("value"),
                },
            )
        )
    connection.close()


class ProcessExtractionPool:
    """Fixed pool of long-lived, single-threaded spawn subprocesses."""

    def __init__(
        self,
        size,
        timeout=None,
        worker_target=_process_worker_main,
        python_executable="/opt/venv/bin/python",
        lookup_parallel=None,
    ):
        if size <= 0:
            raise ValueError("process extraction pool size must be positive")
        self.size = size
        self.timeout = _proc_timeout() if timeout is None else timeout
        self.startup_timeout = max(self.timeout, 30.0)
        self.worker_target = worker_target
        self.closed = False
        self.lookup_parallel = min(
            size, LOOKUP_PARALLEL if lookup_parallel is None else max(1, lookup_parallel)
        )
        self._available = queue.Queue(maxsize=size)
        self._workers = {}
        multiprocessing.set_executable(python_executable)
        self._context = multiprocessing.get_context("spawn")
        pending = [self._launch(slot) for slot in range(1, size + 1)]
        try:
            for worker in pending:
                self._await_ready(worker)
                self._workers[worker["slot"]] = worker
                self._available.put(worker)
        except Exception:
            for worker in pending:
                self._terminate(worker)
            raise
        self._lookup_executor = ThreadPoolExecutor(
            max_workers=self.lookup_parallel, thread_name_prefix="fdb-lookup"
        )

    def _launch(self, slot):
        parent, child = self._context.Pipe(duplex=True)
        process = self._context.Process(
            target=self.worker_target,
            args=(child, slot),
            name=f"polytope-extract-{slot}",
            daemon=True,
        )
        process.start()
        child.close()
        return {"slot": slot, "process": process, "connection": parent}

    def _await_ready(self, worker):
        connection = worker["connection"]
        process = worker["process"]
        if not connection.poll(self.startup_timeout):
            raise ExtractError(
                f"extraction subprocess {worker['slot']} startup timed out"
            )
        try:
            status, detail = connection.recv()
        except EOFError as exc:
            raise ExtractError(
                f"extraction subprocess {worker['slot']} died during startup "
                f"(exitcode={process.exitcode})"
            ) from exc
        if status != "ready":
            raise ExtractError(
                f"extraction subprocess {worker['slot']} failed startup: {detail}"
            )
        worker["detail"] = detail
        logging.info(
            "chunks extraction subprocess ready proc=%d pid=%s env=%s endpoints=%s "
            "warm=%.1fms endpoint_names=%s",
            worker["slot"],
            detail.get("pid"),
            detail.get("env"),
            detail.get("warm_endpoints"),
            detail.get("warm_ms", 0.0),
            ",".join(detail.get("warm_endpoint_names", [])),
        )

    @staticmethod
    def _terminate(worker):
        try:
            worker["connection"].close()
        except OSError:
            pass
        process = worker["process"]
        if process.is_alive():
            process.kill()
        process.join(timeout=5)

    def _replace(self, worker):
        slot = worker["slot"]
        self._terminate(worker)
        replacement = self._launch(slot)
        self._await_ready(replacement)
        self._workers[slot] = replacement
        self._available.put(replacement)

    def execute(self, job):
        if self.closed:
            raise ExtractError("extraction subprocess pool is closed")
        try:
            worker = self._available.get(timeout=self.timeout)
        except queue.Empty as exc:
            raise ExtractError(
                "timed out waiting for an extraction subprocess"
            ) from exc
        healthy = False
        try:
            process = worker["process"]
            connection = worker["connection"]
            if not process.is_alive():
                raise EOFError(
                    f"subprocess exited with code {process.exitcode}"
                )
            connection.send(job)
            if not connection.poll(self.timeout):
                raise TimeoutError(
                    f"extraction subprocess {worker['slot']} timed out after "
                    f"{self.timeout:g}s"
                )
            status, response = connection.recv()
            healthy = process.is_alive()
            if status == "ok":
                response["proc"] = worker["slot"]
                return response
            if status == "error":
                raise ExtractError(response["message"])
            raise RuntimeError(f"unknown extraction subprocess response {status!r}")
        except ExtractError:
            if healthy:
                raise
            try:
                self._replace(worker)
            except Exception as respawn_exc:
                logging.error(
                    "failed to respawn extraction subprocess %d: %s",
                    worker["slot"],
                    respawn_exc,
                )
            raise
        except (EOFError, OSError, TimeoutError, BrokenPipeError) as exc:
            try:
                self._replace(worker)
            except Exception as respawn_exc:
                logging.error(
                    "failed to respawn extraction subprocess %d: %s",
                    worker["slot"],
                    respawn_exc,
                )
            raise ExtractError(str(exc)) from exc
        finally:
            if healthy:
                self._available.put(worker)

    def lookup(self, tasks):
        """Run lookup-only sub-batches concurrently and merge successful results."""
        if self.closed:
            raise ExtractError("extraction subprocess pool is closed")
        futures = {
            self._lookup_executor.submit(self.execute, task): task for task in tasks
        }
        locations = {}
        fallback_subbatches = 0
        errors = []
        for future in as_completed(futures):
            task = futures[future]
            try:
                response = future.result()
            except ExtractError as exc:
                logging.warning(
                    "FDB location lookup sub-batch failed; falling back for %d "
                    "field(s): %s",
                    len(task["fields"]),
                    exc,
                )
                errors.append(str(exc))
            else:
                locations.update(response["locations"])
                fallback_subbatches += response.get("fallbacks", 0)
        return {
            "locations": locations,
            "fallbacks": fallback_subbatches,
            "errors": errors,
        }

    def close(self):
        if self.closed:
            return
        self._lookup_executor.shutdown(wait=True)
        self.closed = True
        workers = list(self._workers.values())
        self._workers.clear()
        for worker in workers:
            try:
                worker["connection"].send(None)
            except (BrokenPipeError, OSError):
                pass
        for worker in workers:
            self._terminate(worker)


def _get_process_pool():
    global _process_pool
    if PROC_POOL_SIZE == 0:
        return None
    if _process_pool is None:
        with _process_pool_lock:
            if _process_pool is None:
                _process_pool = ProcessExtractionPool(PROC_POOL_SIZE)
    return _process_pool


def _isolated_profile(job_id):
    return {
        "job": job_id or "-",
        "status": "error",
        "phase": "parse",
        "proc": 0,
        "fields": 0,
        "ranges": 0,
        "points": 0,
        "dtype": "f32",
        "shuffle": 1,
        "cache_hits": 0,
        "cache_misses": 0,
        "fallbacks": 0,
        "lookup_subbatches": 0,
        "lookup_fallbacks": 0,
        "gj_subbatches": 0,
        "lookup_ms": 0.0,
        "lookup_mode": "none",
        "parse_ms": 0.0,
        "enum_ms": 0.0,
        "extract_ms": 0.0,
        "assemble_ms": 0.0,
        "shuffle_ms": 0.0,
        "compress_ms": 0.0,
        "total_ms": 0.0,
        "raw_bytes": 0,
        "payload_bytes": 0,
    }


def _profile_timings(prof):
    return {
        "parse_ms": prof["parse_ms"],
        "enum_ms": prof["enum_ms"],
        "lookup_ms": prof["lookup_ms"],
        "lookup_mode": prof["lookup_mode"],
        "extract_ms": prof["extract_ms"],
        "assemble_ms": prof["assemble_ms"],
        "shuffle_ms": prof["shuffle_ms"],
        "compress_ms": prof["compress_ms"],
        "retrieve_ms": prof["total_ms"],
        "fields": prof["fields"],
        "points": prof["points"],
        "cache_hits": prof["cache_hits"],
        "cache_misses": prof["cache_misses"],
        "fallbacks": prof["fallbacks"],
        "lookup_subbatches": prof["lookup_subbatches"],
        "lookup_fallbacks": prof["lookup_fallbacks"],
        "gj_subbatches": prof["gj_subbatches"],
        "raw_bytes": prof["raw_bytes"],
        "payload_bytes": prof["payload_bytes"],
        "proc": prof["proc"],
    }


def _path_plan(location):
    return {
        "path": location.path,
        "offset": location.offset,
        "host": location.host,
        "port": location.port,
        "scheme": location.scheme,
    }


def _multi_chunks(request):
    if not isinstance(request, dict) or not isinstance(request.get("chunks"), list):
        return None
    chunks = request["chunks"]
    if not chunks:
        raise ExtractError("multi-chunk request has no elements")
    return chunks


def _frame_multi(status_payloads):
    header = bytearray(b"PZMC")
    header.extend(struct.pack("<BI", 1, len(status_payloads)))
    payloads = bytearray()
    for status, payload in status_payloads:
        if status not in (0, 1) or (status == 1 and payload):
            raise ExtractError("invalid multi-chunk element result")
        header.extend(struct.pack("<BQ", status, len(payload)))
        payloads.extend(payload)
    return bytes(header + payloads)


def _is_data_not_found_error(exc):
    text = str(getattr(exc, "message", exc))
    return "DataNotFound" in text or "Matched 0 fields" in text


def _prepare_rust_multi_extract_plan(request, user=None, job_id=None):
    """Parse all elements, deduplicate fields and perform one union lookup."""
    prof = _isolated_profile(job_id)
    started = time.monotonic()
    parsed_elements = []
    union_fields = []
    union_by_key = {}
    total_ranges = 0
    total_points = 0

    chunks = _multi_chunks(request)
    if chunks is None:
        raise ExtractError("request is not a multi-chunk extract")
    for chunk in chunks:
        spec, field_values = parse_extract(chunk)
        spec["registry_grid_hash"] = spec["grid_hash"]
        fields = list(enumerate_fields(field_values, spec["order"]))
        if not fields:
            raise ExtractError("extract request enumerated no fields")
        _apply_hash_override(fields[0], spec)
        indices = []
        for field in fields:
            key = canonical_field_key(field)
            index = union_by_key.get(key)
            if index is None:
                index = len(union_fields)
                union_by_key[key] = index
                union_fields.append(field)
            indices.append(index)
        parsed_elements.append((spec, indices))
        total_ranges += len(spec["ranges"])
        total_points += len(fields) * sum(hi - lo for lo, hi in spec["ranges"])

    parsed = time.monotonic()
    prof["parse_ms"] = _ms(started, parsed)
    prof["fields"] = len(union_fields)
    prof["ranges"] = total_ranges
    prof["points"] = total_points
    prof["dtype"] = "mixed"
    prof["shuffle"] = 0

    cache = _get_location_cache()
    locations = []
    missing = []
    for index, field in enumerate(union_fields):
        location = cache.get(field) if cache.enabled else None
        locations.append(location)
        if location is None:
            prof["cache_misses"] += 1
            missing.append((index, field))
        else:
            prof["cache_hits"] += 1
    prof["enum_ms"] = _ms(parsed, time.monotonic())

    pool = _get_process_pool()
    if pool is None:
        raise ExtractError("extraction subprocess pool is disabled")
    if missing:
        lookup_started = time.monotonic()
        tasks = []
        missing_fields = [field for _index, field in missing]
        for start in range(0, len(missing_fields), LOOKUP_SUBBATCH):
            subset = missing_fields[start : start + LOOKUP_SUBBATCH]
            tasks.append(
                {
                    "task": "lookup",
                    "fields": subset,
                    "batch_request": _batch_request_for_fields(subset),
                }
            )
        prof["lookup_mode"] = "inspect-parallel"
        prof["lookup_subbatches"] = len(tasks)
        lookup_result = pool.lookup(tasks)
        prof["lookup_fallbacks"] = lookup_result["fallbacks"]
        fatal_lookup_errors = [
            error
            for error in lookup_result.get("errors", [])
            if not _is_data_not_found_error(error)
        ]
        if fatal_lookup_errors:
            raise ExtractError(fatal_lookup_errors[0])
        inserts = []
        for index, field in missing:
            location = lookup_result["locations"].get(canonical_field_key(field))
            locations[index] = location
            if location is not None:
                inserts.append((field, location))
        if cache.enabled:
            cache.put_many(inserts)
        prof["lookup_ms"] = _ms(lookup_started, time.monotonic())

    statuses = []
    for _spec, indices in parsed_elements:
        statuses.append(1 if any(locations[index] is None for index in indices) else 0)

    servermap = _get_location_servermap()
    paths = []
    path_index = {}
    for (_spec, indices), status in zip(parsed_elements, statuses):
        if status:
            continue
        for union_index in indices:
            if union_index in path_index:
                continue
            translated = servermap.translate(locations[union_index])
            if translated is None:
                raise ExtractError(
                    "FDB location is not routable for field "
                    f"{_describe(union_fields[union_index])}"
                )
            path_index[union_index] = len(paths)
            paths.append(_path_plan(translated))

    elements = []
    for (spec, indices), status in zip(parsed_elements, statuses):
        elements.append(
            {
                "status": status,
                "path_indices": []
                if status
                else [path_index[index] for index in indices],
                "ranges": [list(item) for item in spec["ranges"]],
                "grid_hash": spec["grid_hash"],
                "dtype": spec["dtype"],
                "shuffle": spec["shuffle"],
            }
        )

    context = None
    if user is not None:
        context = {
            "user": f"{getattr(user, 'realm', '')}:{getattr(user, 'username', '')}"
        }
        if job_id:
            context["job_id"] = job_id

    prof["python_ms"] = _ms(started, time.monotonic())
    files = len({(path["host"], path["port"], path["path"]) for path in paths})
    profile_keys = (
        "job",
        "fields",
        "ranges",
        "points",
        "dtype",
        "shuffle",
        "cache_hits",
        "cache_misses",
        "fallbacks",
        "lookup_mode",
        "lookup_fallbacks",
        "lookup_subbatches",
        "lookup_ms",
        "parse_ms",
        "enum_ms",
        "python_ms",
    )
    profile = {key: prof[key] for key in profile_keys}
    profile.update({"chunks": len(elements), "files": files})
    plan = {
        "kind": "rust_gribjump_extract_v2",
        "paths": paths,
        "elements": elements,
        "zstd_level": ZSTD_LEVEL,
        "context": context,
        "profile": profile,
    }
    return plan, MULTI_CONTENT_TYPE, _profile_timings(prof)


def prepare_rust_extract_plan(request, user=None, job_id=None):
    """Keep parsing/enumeration/FDB lookup in Python and return a compact Rust plan."""
    if _multi_chunks(request) is not None:
        return _prepare_rust_multi_extract_plan(request, user=user, job_id=job_id)
    prof = _isolated_profile(job_id)
    started = time.monotonic()
    spec, field_values = parse_extract(request)
    spec["registry_grid_hash"] = spec["grid_hash"]
    parsed = time.monotonic()
    prof["parse_ms"] = _ms(started, parsed)
    prof["dtype"] = "f32" if spec["dtype"] == "float32" else "f64"
    prof["shuffle"] = 1 if spec["shuffle"] else 0

    fields = list(enumerate_fields(field_values, spec["order"]))
    if not fields:
        raise ExtractError("extract request enumerated no fields")
    prof["fields"] = len(fields)
    prof["ranges"] = len(spec["ranges"])
    prof["points"] = len(fields) * sum(hi - lo for lo, hi in spec["ranges"])
    _apply_hash_override(fields[0], spec)

    cache = _get_location_cache()
    locations = []
    missing = []
    for index, field in enumerate(fields):
        location = cache.get(field) if cache.enabled else None
        locations.append(location)
        if location is None:
            prof["cache_misses"] += 1
            missing.append((index, field))
        else:
            prof["cache_hits"] += 1
    enumerated = time.monotonic()
    prof["enum_ms"] = _ms(parsed, enumerated)

    pool = _get_process_pool()
    if pool is None:
        raise ExtractError("extraction subprocess pool is disabled")
    if missing:
        lookup_started = time.monotonic()
        tasks = []
        missing_fields = [field for _index, field in missing]
        for start in range(0, len(missing_fields), LOOKUP_SUBBATCH):
            subset = missing_fields[start : start + LOOKUP_SUBBATCH]
            tasks.append(
                {
                    "task": "lookup",
                    "fields": subset,
                    "batch_request": _batch_request_for_fields(subset),
                }
            )
        prof["lookup_mode"] = "inspect-parallel"
        prof["lookup_subbatches"] = len(tasks)
        lookup_result = pool.lookup(tasks)
        prof["lookup_fallbacks"] = lookup_result["fallbacks"]
        inserts = []
        for index, field in missing:
            location = lookup_result["locations"].get(canonical_field_key(field))
            if location is None:
                raise ExtractError(
                    f"FDB lookup returned no location for field {_describe(field)}"
                )
            locations[index] = location
            inserts.append((field, location))
        if cache.enabled:
            cache.put_many(inserts)
        prof["lookup_ms"] = _ms(lookup_started, time.monotonic())

    servermap = _get_location_servermap()
    paths = []
    for field, location in zip(fields, locations):
        translated = servermap.translate(location)
        if translated is None:
            raise ExtractError(
                f"FDB location is not routable for field {_describe(field)}"
            )
        paths.append(_path_plan(translated))

    context = None
    if user is not None:
        context = {
            "user": f"{getattr(user, 'realm', '')}:{getattr(user, 'username', '')}"
        }
        if job_id:
            context["job_id"] = job_id

    prof["python_ms"] = _ms(started, time.monotonic())
    profile = {
        key: prof[key]
        for key in (
            "job",
            "fields",
            "ranges",
            "points",
            "dtype",
            "shuffle",
            "cache_hits",
            "cache_misses",
            "fallbacks",
            "lookup_mode",
            "lookup_fallbacks",
            "lookup_subbatches",
            "lookup_ms",
            "parse_ms",
            "enum_ms",
            "python_ms",
        )
    }
    plan = {
        "kind": "rust_gribjump_extract_v1",
        "paths": paths,
        "ranges": [list(item) for item in spec["ranges"]],
        "grid_hash": spec["grid_hash"],
        "dtype": spec["dtype"],
        "shuffle": spec["shuffle"],
        "zstd_level": ZSTD_LEVEL,
        "context": context,
        "profile": profile,
    }
    return plan, CONTENT_TYPE, _profile_timings(prof)


def _run_extract_isolated(request, user=None, job_id=None):
    prof = _isolated_profile(job_id)
    started = time.monotonic()
    try:
        spec, field_values = parse_extract(request)
        spec["registry_grid_hash"] = spec["grid_hash"]
        parsed = time.monotonic()
        prof["parse_ms"] = _ms(started, parsed)
        prof["dtype"] = "f32" if spec["dtype"] == "float32" else "f64"
        prof["shuffle"] = int(spec["shuffle"])

        prof["phase"] = "enum"
        fields = list(enumerate_fields(field_values, spec["order"]))
        prof["fields"] = len(fields)
        prof["ranges"] = len(spec["ranges"])
        prof["points"] = len(fields) * sum(
            hi - lo for lo, hi in spec["ranges"]
        )
        _apply_hash_override(fields[0], spec)
        cache = _get_location_cache()
        locations = []
        missing = []
        if cache.enabled:
            for index, field in enumerate(fields):
                location = cache.get(field)
                locations.append(location)
                if location is None:
                    prof["cache_misses"] += 1
                    missing.append((index, field))
                else:
                    prof["cache_hits"] += 1
        else:
            locations = [None] * len(fields)
        enumerated = time.monotonic()
        prof["enum_ms"] = _ms(parsed, enumerated)

        ctx = None
        if user is not None:
            ctx = {
                "user": f"{getattr(user, 'realm', '')}:{getattr(user, 'username', '')}"
            }
            if job_id:
                ctx["job_id"] = job_id

        pool = _get_process_pool()
        if pool is None:
            raise ExtractError("extraction subprocess pool is disabled")

        if missing:
            prof["phase"] = "lookup"
            lookup_started = time.monotonic()
            lookup_tasks = []
            missing_fields = [field for _index, field in missing]
            for start in range(0, len(missing_fields), LOOKUP_SUBBATCH):
                subset = missing_fields[start : start + LOOKUP_SUBBATCH]
                lookup_tasks.append(
                    {
                        "task": "lookup",
                        "fields": subset,
                        "batch_request": _batch_request_for_fields(subset),
                    }
                )
            prof["lookup_mode"] = "inspect-parallel"
            prof["lookup_subbatches"] = len(lookup_tasks)
            lookup_result = pool.lookup(lookup_tasks)
            batch_locations = lookup_result["locations"]
            prof["lookup_fallbacks"] = lookup_result["fallbacks"]
            inserts = []
            for index, field in missing:
                location = batch_locations.get(canonical_field_key(field))
                if location is not None:
                    locations[index] = location
                    inserts.append((field, location))
            cache.put_many(inserts)
            prof["lookup_ms"] = _ms(lookup_started, time.monotonic())

        parent_lookup_ms = prof["lookup_ms"]
        parent_lookup_mode = prof["lookup_mode"]
        parent_lookup_subbatches = prof["lookup_subbatches"]
        parent_lookup_fallbacks = prof["lookup_fallbacks"]
        prof["phase"] = "subprocess"
        response = pool.execute(
            {
                "fields": fields,
                "field_values": field_values,
                "spec": spec,
                "locations": locations,
                "locations_resolved": cache.enabled,
                "cache_enabled": cache.enabled,
                "ctx": ctx,
                "job_id": job_id,
            }
        )
        for field in response["invalidations"]:
            cache.invalidate(field)
        cache.put_many(response["updates"])
        payload = response["payload"]
        prof.update(response["profile"])
        if parent_lookup_subbatches:
            prof["lookup_ms"] = round(
                parent_lookup_ms + response["profile"]["lookup_ms"], 1
            )
            prof["lookup_mode"] = parent_lookup_mode
            prof["lookup_subbatches"] = (
                parent_lookup_subbatches
                + response["profile"]["lookup_subbatches"]
            )
            prof["lookup_fallbacks"] = (
                parent_lookup_fallbacks
                + response["profile"]["lookup_fallbacks"]
            )
        prof["proc"] = response["proc"]
        prof["status"] = "ok"
        prof["phase"] = "done"
    finally:
        prof["total_ms"] = _ms(started, time.monotonic())
        _log_profile(prof)

    return payload, CONTENT_TYPE, _profile_timings(prof)


def _run_multi_extract_fallback(
    request, pygribjump=None, pyfdb=None, user=None, job_id=None
):
    results = []
    chunks = _multi_chunks(request)
    if chunks is None:
        raise ExtractError("request is not a multi-chunk extract")
    for chunk in chunks:
        try:
            payload, _content_type, _timings = run_extract(
                chunk,
                pygribjump=pygribjump,
                pyfdb=pyfdb,
                user=user,
                job_id=job_id,
            )
            results.append((0, payload))
        except Exception as exc:
            if not _is_data_not_found_error(exc):
                raise
            results.append((1, b""))
    payload = _frame_multi(results)
    return payload, MULTI_CONTENT_TYPE, {"payload_bytes": len(payload), "chunks": len(results)}


def run_extract(request, pygribjump=None, pyfdb=None, user=None, job_id=None):
    """Serve an extract job through the process pool, or retained fallback mode."""
    if _multi_chunks(request) is not None:
        return _run_multi_extract_fallback(
            request,
            pygribjump=pygribjump,
            pyfdb=pyfdb,
            user=user,
            job_id=job_id,
        )
    if PROC_POOL_SIZE > 0 and pygribjump is None and pyfdb is None:
        return _run_extract_isolated(request, user=user, job_id=job_id)
    return _run_extract_fallback(
        request,
        pygribjump=pygribjump,
        pyfdb=pyfdb,
        user=user,
        job_id=job_id,
    )


atexit.register(lambda: _process_pool.close() if _process_pool is not None else None)
