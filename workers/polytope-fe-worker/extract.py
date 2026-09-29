# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

"""/chunks/v1 extract path for the fe-worker (design D17, wire contract v0 §2-§4).

A job whose (metkit-expanded) request carries a top-level ``extract`` object is
served here instead of by PolytopeMars: enumerate the fields of the request in
FIELD_ORDER, extract the requested half-open grid-point ranges from every field
with pygribjump, concatenate them into one little-endian float64 buffer and
return it as a single zstd frame.

No polytope-mars, no CoverageJSON. Any missing field, value-count mismatch,
grid-hash mismatch or gribjump error fails the whole job (D5) -- this module
raises and never returns partial results.

gribjump/FDB configuration: this module does not read any config itself. It
relies on the process-scoped ``PolytopeDataSource`` (polytope.py) having
already materialised the worker config's ``gribjump_config`` (and optional
``fdb_config``) to /tmp and exported ``GRIBJUMP_CONFIG_FILE`` /
``FDB5_CONFIG_FILE`` -- exactly what the PolytopeMars path relies on. That
happens once per process. With the location cache enabled, a miss resolves one
field with pyfdb and subsequent chunks use pygribjump's path-based API directly;
size zero retains the original request-based extraction path exactly.

Profiling: every job emits exactly one ``chunks-profile`` INFO log line (see
``_log_profile``) with per-phase wall times, so worker-side cost can be read
straight out of ``kubectl logs`` (inside the host's ``python worker logs``
record).
"""

import itertools
import logging
import os
import time
from urllib.parse import parse_qs

import numpy as np

from location_cache import FieldLocation, LocationCache  # type: ignore[import-not-found]

CONTENT_TYPE = "application/octet-stream"

EXTRACT_KEY = "extract"

# Request keys that are never MARS field-identifying keys and must not be sent
# to gribjump in the per-field request string.
_NON_FIELD_KEYS = {"verb", EXTRACT_KEY}

_SUPPORTED_DTYPES = {"float64"}


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

    ``spec`` = dict(ranges=[(lo, hi), ...], order=[...], grid_hash=str|None,
    dtype="float64"); ``field_values`` = {key: [str, ...]} for every MARS key.
    """
    if not is_extract_request(request):
        raise ExtractError("request has no 'extract' object")
    ext = request[EXTRACT_KEY]

    unknown = set(ext) - {"ranges", "order", "grid_hash", "dtype"}
    if unknown:
        raise ExtractError(f"extract: unknown field(s) {sorted(unknown)}")

    dtype = ext.get("dtype", None)
    if dtype not in _SUPPORTED_DTYPES:
        raise ExtractError(
            f"extract.dtype must be one of {sorted(_SUPPORTED_DTYPES)}, got {dtype!r}"
        )

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

    spec = {"ranges": ranges, "order": list(order), "grid_hash": grid_hash, "dtype": dtype}
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

_gribjump = None
_fdb = None
_location_cache = None


def _get_gribjump(pygribjump):
    """Process-scoped GribJump handle (created after config is set)."""
    global _gribjump
    if _gribjump is None:
        _gribjump = pygribjump.GribJump()
    return _gribjump


def _get_fdb(pyfdb):
    """Process-scoped FDB handle (created after FDB5_CONFIG_FILE is set)."""
    global _fdb
    if _fdb is None:
        _fdb = pyfdb.FDB()
    return _fdb


def _get_location_cache():
    global _location_cache
    if _location_cache is None:
        _location_cache = LocationCache()
    return _location_cache


def _reset_gribjump():  # for tests
    global _gribjump
    _gribjump = None


def _reset_location_state():  # for tests
    global _fdb, _location_cache
    _fdb = None
    _location_cache = None


def warm_up(pygribjump=None):
    """Process-level warm-up, called once after gribjump/FDB config is set."""
    import zstandard  # noqa: F401  # type: ignore[import-not-found]

    if pygribjump is None:
        import pygribjump  # type: ignore[import-not-found]
    _get_gribjump(pygribjump)
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
    # Preserve the outer FDB URI. For remote locations its host/port identify
    # the FDB store endpoint, which gribjump maps to an extraction server via
    # the configured servermap; they are not a direct gribjump endpoint.
    return FieldLocation(
        path=path,
        scheme=scheme,
        offset=_location_int(offset, "offset"),
        length=_location_int(length, "length"),
        host=uri.hostname() or "",
        port=_location_int(uri.port() or 0, "port"),
    )


def _lookup_field_location(field, pyfdb):
    """Resolve exactly one FDB field to its path extraction location."""
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
        pass
    except Exception as exc:
        raise ExtractError(f"field {_describe(field)}: FDB location lookup failed: {exc}") from exc
    else:
        raise ExtractError(f"field {_describe(field)}: FDB location lookup returned multiple fields")
    try:
        return _location_from_element(first)
    except ExtractError as exc:
        raise ExtractError(f"field {_describe(field)}: {exc}") from exc
    except Exception as exc:
        raise ExtractError(
            f"field {_describe(field)}: FDB location lookup returned an invalid location: {exc}"
        ) from exc


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
        "cache=%d/%d t_lookup=%.1fms "
        "t_parse=%.1fms t_enum=%.1fms t_extract=%.1fms t_assemble=%.1fms "
        "t_zstd=%.1fms t_total=%.1fms raw_bytes=%d bytes=%d zstd_level=%d",
        prof["job"],
        prof["status"],
        prof["phase"],
        prof["fields"],
        prof["ranges"],
        prof["points"],
        prof["cache_hits"],
        prof["cache_misses"],
        prof["lookup_ms"],
        prof["parse_ms"],
        prof["enum_ms"],
        prof["extract_ms"],
        prof["assemble_ms"],
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
        "cache_hits": 0,
        "cache_misses": 0,
        "lookup_ms": 0.0,
        "parse_ms": 0.0,
        "enum_ms": 0.0,
        "extract_ms": 0.0,
        "assemble_ms": 0.0,
        "compress_ms": 0.0,
        "total_ms": 0.0,
        "raw_bytes": 0,
        "payload_bytes": 0,
    }
    t0 = time.monotonic()
    try:
        spec, field_values = parse_extract(request)
        t_parse = time.monotonic()
        prof["parse_ms"] = _ms(t0, t_parse)

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
        gj = _get_gribjump(pygribjump)
        cache = _get_location_cache()
        requests = None if cache.enabled else build_requests(fields, spec, pygribjump)
        t_enum = time.monotonic()
        prof["enum_ms"] = _ms(t_parse, t_enum)

        def lookup(field):
            nonlocal pyfdb
            prof["phase"] = "lookup"
            started = time.monotonic()
            try:
                if pyfdb is None:
                    import pyfdb as imported_pyfdb  # type: ignore[import-not-found]

                    pyfdb = imported_pyfdb
                return _lookup_field_location(field, pyfdb)
            finally:
                prof["lookup_ms"] += _ms(started, time.monotonic())

        try:
            prof["phase"] = "extract"
            if not cache.enabled:
                results = gj.extract(requests, ctx=ctx) if ctx is not None else gj.extract(requests)
                t_extract = time.monotonic()
                prof["extract_ms"] = _ms(t_enum, t_extract)
            else:
                locations = []
                for field in fields:
                    location = cache.get(field)
                    if location is None:
                        prof["cache_misses"] += 1
                        location = lookup(field)
                        cache.put(field, location)
                    else:
                        prof["cache_hits"] += 1
                    locations.append(location)

                prof["phase"] = "extract"
                extract_seconds = 0.0
                started = time.monotonic()
                try:
                    results = _extract_locations(
                        gj, pygribjump, locations, spec, ctx
                    )
                    extract_seconds += time.monotonic() - started
                except RuntimeError:
                    extract_seconds += time.monotonic() - started
                    # The path API reports a batch error without an index. Refresh
                    # every candidate location so the stale entry is guaranteed to
                    # be invalidated, then retry the batch exactly once.
                    fresh_locations = []
                    for field in fields:
                        cache.invalidate(field)
                        fresh_location = lookup(field)
                        cache.put(field, fresh_location)
                        fresh_locations.append(fresh_location)
                    prof["phase"] = "extract"
                    started = time.monotonic()
                    try:
                        results = _extract_locations(
                            gj, pygribjump, fresh_locations, spec, ctx
                        )
                        extract_seconds += time.monotonic() - started
                    except RuntimeError as exc:
                        extract_seconds += time.monotonic() - started
                        raise ExtractError(
                            f"gribjump location extraction failed after refresh: {exc}"
                        ) from exc
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

        prof["phase"] = "zstd"
        prof["raw_bytes"] = out.nbytes
        # Compress straight from the numpy buffer (no intermediate bytes copy).
        payload = compress(out)
        t_zstd = time.monotonic()
        prof["compress_ms"] = _ms(t_assemble, t_zstd)
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
        "extract_ms": prof["extract_ms"],
        "assemble_ms": prof["assemble_ms"],
        "compress_ms": prof["compress_ms"],
        "retrieve_ms": prof["total_ms"],
        "fields": prof["fields"],
        "points": prof["points"],
        "cache_hits": prof["cache_hits"],
        "cache_misses": prof["cache_misses"],
        "raw_bytes": prof["raw_bytes"],
        "payload_bytes": prof["payload_bytes"],
    }
    return payload, CONTENT_TYPE, timings
