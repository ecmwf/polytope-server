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
With the location cache enabled, all misses in a multi-field job are resolved by
one pyfdb list call; subsequent chunks use pygribjump's path-based API directly.
Cache size zero retains the original request-based extraction path exactly.

Profiling: every job emits exactly one ``chunks-profile`` INFO log line (see
``_log_profile``) with per-phase wall times, so worker-side cost can be read
straight out of ``kubectl logs`` (inside the host's ``python worker logs``
record).
"""

import copy
import itertools
import logging
import os
import re
import threading
import time
from urllib.parse import parse_qs

import numpy as np

from location_cache import (  # type: ignore[import-not-found]
    FieldLocation,
    LocationCache,
    LocationServerMap,
    canonical_field_key,
)

CONTENT_TYPE = "application/octet-stream"

EXTRACT_KEY = "extract"

# Request keys that are never MARS field-identifying keys and must not be sent
# to gribjump in the per-field request string.
_NON_FIELD_KEYS = {"verb", EXTRACT_KEY}

_SUPPORTED_DTYPES = {"float32", "float64"}


def _zstd_level() -> int:
    raw = os.environ.get("POLYTOPE_CHUNKS_ZSTD_LEVEL", "").strip()
    try:
        return int(raw) if raw else 3
    except ValueError:
        return 3


# Read once per process; override via env for profiling experiments.
ZSTD_LEVEL = _zstd_level()


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
_location_cache = None
_location_servermap = None
_location_state_lock = threading.Lock()

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


def _get_fdb(pyfdb):
    """Return the calling thread's lazily-created FDB handle."""
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


def warm_up(pygribjump=None):
    """Warm imports and process-global cache after native client config is set.

    Native GribJump and FDB handles are intentionally not created here: each
    blocking worker thread constructs its own handle lazily on its first job.
    """
    import zstandard  # noqa: F401  # type: ignore[import-not-found]

    if pygribjump is None:
        import pygribjump  # noqa: F401  # type: ignore[import-not-found]
    _get_location_cache()


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


def _lookup_field_element(field, pyfdb):
    """Resolve exactly one FDB list element for a field."""
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
    first = _lookup_field_element(field, pyfdb)
    try:
        return _location_from_element(first)
    except ExtractError as exc:
        raise ExtractError(f"field {_describe(field)}: {exc}") from exc
    except Exception as exc:
        raise ExtractError(
            f"field {_describe(field)}: FDB location lookup returned an invalid location: {exc}"
        ) from exc


def _lookup_field_locations(fields, batch_request, pyfdb):
    """Resolve the requested fields from one FDB list operation.

    FDB list order is not contractual, so each result is joined to its field by
    the canonical metadata identity. Missing, duplicate, or malformed elements
    are omitted; callers preserve the existing request-based per-field fallback.
    """
    wanted = {canonical_field_key(field): field for field in fields}
    identity_names = set().union(*(field.keys() for field in fields)) - _NON_FIELD_KEYS
    locations = {}
    seen = set()
    try:
        iterator = iter(_get_fdb(pyfdb).list(batch_request))
    except Exception as exc:
        logging.warning("Batched FDB location lookup failed: %s", exc)
        return locations

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
            logging.warning("Ignoring FDB list element with invalid metadata: %s", exc)
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


def _field_number_of_data_points(field, pyfdb):
    """Read one field header through pyfdb and return numberOfDataPoints."""
    element = _lookup_field_element(field, pyfdb)
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


def _extract_locations(gj, pygribjump, locations, spec, ctx):
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
    iterator = (
        gj.extract_from_paths(requests, ctx=ctx)
        if ctx is not None
        else gj.extract_from_paths(requests)
    )
    # Materialise inside the retry boundary: the C iterator can also raise.
    return list(iterator)

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
    gj = _get_gribjump(pygribjump)
    try:
        results = gj.extract(requests, ctx=ctx) if ctx is not None else gj.extract(requests)
        return assemble(results, field_requests, spec).tobytes()
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
        "chunks-profile job=%s status=%s phase=%s fields=%d ranges=%d points=%d "
        "dtype=%s shuffle=%d cache=%d/%d fallback=%d lookup_mode=%s t_lookup=%.1fms "
        "t_parse=%.1fms t_enum=%.1fms t_extract=%.1fms t_assemble=%.1fms "
        "t_shuffle=%.1fms t_zstd=%.1fms t_total=%.1fms "
        "raw_bytes=%d bytes=%d zstd_level=%d",
        prof["job"],
        prof["status"],
        prof["phase"],
        prof["fields"],
        prof["ranges"],
        prof["points"],
        prof["dtype"],
        prof["shuffle"],
        prof["cache_hits"],
        prof["cache_misses"],
        prof["fallbacks"],
        prof["lookup_mode"],
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


def run_extract(request, pygribjump=None, pyfdb=None, user=None, job_id=None):
    """Serve an extract job. Returns ``(payload_bytes, content_type, timings)``."""
    if pygribjump is None:
        import pygribjump  # type: ignore[import-not-found]

    prof = {
        "job": job_id or "-",
        "status": "error",
        "phase": "parse",
        "fields": 0,
        "ranges": 0,
        "points": 0,
        "dtype": "f32",
        "shuffle": 1,
        "cache_hits": 0,
        "cache_misses": 0,
        "fallbacks": 0,
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
        gj = _get_gribjump(pygribjump)
        cache = _get_location_cache()
        t_enum = time.monotonic()
        prof["enum_ms"] = _ms(t_parse, t_enum)

        def lookup(field):
            nonlocal pyfdb
            prof["phase"] = "lookup"
            if prof["lookup_mode"] == "none":
                prof["lookup_mode"] = "single"
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
                def extract_requests():
                    requests = build_requests(fields, spec, pygribjump)
                    iterator = (
                        gj.extract(requests, ctx=ctx)
                        if ctx is not None
                        else gj.extract(requests)
                    )
                    return list(iterator)

                results = _extract_with_hash_learning(
                    extract_requests, fields[0], spec, pygribjump, pyfdb, job_id
                )
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
                        extracted = _extract_with_hash_learning(
                            lambda: _extract_locations(
                                gj, pygribjump, [item[2] for item in items], spec, ctx
                            ),
                            items[0][1],
                            spec,
                            pygribjump,
                            pyfdb,
                            job_id,
                        )
                    finally:
                        extract_seconds += time.monotonic() - started
                    for item, result in zip(items, extracted):
                        results_by_index[item[0]] = result

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
                        def extract_fallback():
                            fallback_requests = build_requests(
                                fallback_fields, spec, pygribjump
                            )
                            iterator = (
                                gj.extract(fallback_requests, ctx=ctx)
                                if ctx is not None
                                else gj.extract(fallback_requests)
                            )
                            return list(iterator)

                        fallback_results = _extract_with_hash_learning(
                            extract_fallback,
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
        "raw_bytes": prof["raw_bytes"],
        "payload_bytes": prof["payload_bytes"],
    }
    return payload, CONTENT_TYPE, timings
