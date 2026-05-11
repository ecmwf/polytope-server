import json
import math
import os
import tempfile
import threading


class _LazyEccodes:
    _module = None

    def _load(self):
        if self._module is None:
            import eccodes as _eccodes

            self._module = _eccodes
        return self._module

    def __getattr__(self, name):
        return getattr(self._load(), name)


eccodes = _LazyEccodes()

_GRID_CACHE = None
_GRID_CACHE_LOCK = threading.Lock()


def _grid_cache_enabled():
    return os.environ.get("POLYTOPE_DISABLE_GRID_CACHE", "").lower() not in {"1", "true", "yes", "on"}


def _read_exact_into(data_handle, length):
    # Use readinto() for RemoteFDB-backed handles. DataHandle.read(length)
    # still probes size() first, which emits backend NotImplemented noise even
    # though the pyfdb fallback eventually returns the bytes.
    buffer = bytearray(length)
    view = memoryview(buffer)
    offset = 0

    while offset < length:
        bytes_read = data_handle.readinto(view[offset:])
        if bytes_read <= 0:
            raise EOFError(f"Short GRIB read: wanted {length} bytes, got {offset}")
        offset += bytes_read

    return bytes(buffer)


def read_first_grib_message(data_handle, data_length):
    if data_length <= 0:
        raise ValueError(f"Invalid data handle length: {data_length}")

    return _read_exact_into(data_handle, data_length)


def get_first_grib_message(req):
    import pyfdb

    with pyfdb.FDB() as fdb:
        list_iter = fdb.list(req)
        try:
            first_element = next(list_iter)
        except StopIteration as exc:
            raise ValueError("FDB list returned no fields") from exc

        with first_element.data_handle as dh:
            if dh is None:
                raise ValueError("List element has no data handle")
            field_size = first_element.length()
            msg_bytes = read_first_grib_message(dh, field_size)

    gid = eccodes.codes_new_from_message(msg_bytes)
    return gid


def get_gridspec_lamebert_conformal(gid):
    to_rad = math.pi / 180

    md5hash = eccodes.codes_get(gid, "md5GridSection")

    earth_round = (eccodes.codes_get(gid, "shapeOfTheEarth") == 0) or (eccodes.codes_get(gid, "shapeOfTheEarth") == 6)

    if earth_round:
        if eccodes.codes_get(gid, "shapeOfTheEarth") == 6:
            radius = 6371229
        elif eccodes.codes_get(gid, "shapeOfTheEarth") == 0:
            radius = 6367470
    else:
        radius = None

    nv = eccodes.codes_get(gid, "NV")
    nx = eccodes.codes_get(gid, "Nx")
    ny = eccodes.codes_get(gid, "Ny")
    LoVInDegrees = eccodes.codes_get(gid, "LoV") / 1000000
    Dx = eccodes.codes_get(gid, "Dx")
    Dy = eccodes.codes_get(gid, "Dy")
    latFirstInRadians = eccodes.codes_get(gid, "latitudeOfFirstGridPoint") / 1000000 * to_rad
    lonFirstInRadians = eccodes.codes_get(gid, "longitudeOfFirstGridPoint") / 1000000 * to_rad
    LoVInRadians = eccodes.codes_get(gid, "LoV") / 1000000 * to_rad
    Latin1InRadians = eccodes.codes_get(gid, "Latin1") / 1000000 * to_rad
    Latin2InRadians = eccodes.codes_get(gid, "Latin2") / 1000000 * to_rad
    LaDInRadians = eccodes.codes_get(gid, "LaD") / 1000000 * to_rad

    gridspec = {
        "type": "lambert_conformal",
        "earth_round": earth_round,
        "radius": radius,
        "nv": nv,
        "nx": nx,
        "ny": ny,
        "LoVInDegrees": LoVInDegrees,
        "Dx": Dx,
        "Dy": Dy,
        "latFirstInRadians": latFirstInRadians,
        "lonFirstInRadians": lonFirstInRadians,
        "LoVInRadians": LoVInRadians,
        "Latin1InRadians": Latin1InRadians,
        "Latin2InRadians": Latin2InRadians,
        "LaDInRadians": LaDInRadians,
    }
    return (gridspec, md5hash)


def get_gridspec_icon(gid):
    md5hash = eccodes.codes_get(gid, "md5GridSection")
    gridspec = {}
    return (gridspec, md5hash)


def get_gridspec_and_hash(gid):
    grid_type = eccodes.codes_get(gid, "gridType")
    if grid_type == "lambert_lam":
        return get_gridspec_lamebert_conformal(gid)
    elif grid_type == "icon":
        return get_gridspec_icon(gid)
    else:
        raise ValueError(f"Unsupported grid type: {grid_type}")


def _grid_cache_file():
    return os.path.join(os.path.dirname(__file__), "grid_cache.json")


def _load_cache():
    try:
        with open(_grid_cache_file(), "r", encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        return {}
    except Exception:
        return {}


def _save_cache(cache):
    grid_cache_file = _grid_cache_file()
    dirpath = os.path.dirname(grid_cache_file)
    os.makedirs(dirpath, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=dirpath, prefix=".grid_cache.")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(cache, fh, indent=2, sort_keys=True)
        os.replace(tmp, grid_cache_file)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except Exception:
                pass


def _cache_key(req_georef):
    try:
        return json.dumps(req_georef, sort_keys=True, default=str)
    except Exception:
        return str(req_georef)


def _get_cache_locked():
    global _GRID_CACHE
    if _GRID_CACHE is None:
        _GRID_CACHE = _load_cache()
    return _GRID_CACHE


def lookup_grid_config_local(req):
    if "georef" not in req.keys():
        return
    req_georef = req["georef"]
    cache_key = _cache_key(req_georef)
    cache_enabled = _grid_cache_enabled()

    if cache_enabled:
        with _GRID_CACHE_LOCK:
            cache = _get_cache_locked()
            if cache_key in cache:
                entry = cache[cache_key]
                return (entry.get("gridspec"), entry.get("md5hash"))

    gid = get_first_grib_message(req)
    try:
        gridspec, md5hash = get_gridspec_and_hash(gid)
    finally:
        eccodes.codes_release(gid)

    if not cache_enabled:
        return (gridspec, md5hash)

    with _GRID_CACHE_LOCK:
        cache = _get_cache_locked()
        if cache_key in cache:
            entry = cache[cache_key]
            return (entry.get("gridspec"), entry.get("md5hash"))

        cache[cache_key] = {"gridspec": gridspec, "md5hash": md5hash}
        try:
            _save_cache(cache)
        except Exception:
            pass
        return (gridspec, md5hash)
