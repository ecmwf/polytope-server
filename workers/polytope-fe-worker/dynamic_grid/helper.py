import logging
import os
from urllib.parse import urljoin

import requests
from covjsonkit.param_db import get_param_id_from_db


def normalise_lookup_value(key, value):
    value = value.split("/") if isinstance(value, str) else value

    if isinstance(value, list):
        if key == "georef" and len(value) != 1:
            raise ValueError("Grid lookup requires a single georef")
        if len(value) == 0:
            return None
        value = value[0]

    if isinstance(value, dict):
        return None

    if key == "param" and not str(value).lstrip("-").isdigit():
        try:
            value = get_param_id_from_db(value)
        except Exception:
            logging.warning("Could not convert param shortname '%s' to param id", value)

    return value


def build_grid_lookup_request(request_dict):
    lookup_request = {}
    for key, value in request_dict.items():
        if key in {"feature", "format"}:
            continue
        normalised = normalise_lookup_value(key, value)
        if normalised is not None:
            lookup_request[key] = normalised
    return lookup_request


def gridspec_to_grid_config(gridspec, md5hash):
    if gridspec.get("type") != "lambert_conformal":
        return None

    return {
        "name": "mapper",
        "type": "lambert_conformal",
        "md5_hash": md5hash,
        "is_spherical": gridspec.get("earth_round"),
        "radius": gridspec.get("radius"),
        "nv": gridspec.get("nv"),
        "nx": gridspec.get("nx"),
        "ny": gridspec.get("ny"),
        "LoVInDegrees": gridspec.get("LoVInDegrees"),
        "Dx": gridspec.get("Dx"),
        "Dy": gridspec.get("Dy"),
        "latFirstInRadians": gridspec.get("latFirstInRadians"),
        "lonFirstInRadians": gridspec.get("lonFirstInRadians"),
        "LoVInRadians": gridspec.get("LoVInRadians"),
        "Latin1InRadians": gridspec.get("Latin1InRadians"),
        "Latin2InRadians": gridspec.get("Latin2InRadians"),
        "LaDInRadians": gridspec.get("LaDInRadians"),
        "axes": ["latitude", "longitude"],
    }


def lookup_grid_config_local(req):
    from .local import lookup_grid_config_local as _lookup_grid_config_local

    return _lookup_grid_config_local(req)


def lookup_grid_config_remote(req, service_url, timeout=None, retries=None, retry_timeout=None):
    url = urljoin(service_url.rstrip("/") + "/", "lookup-grid-config")
    if timeout is None:
        timeout = float(os.environ.get("POLYTOPE_DYNAMIC_GRID_SERVICE_TIMEOUT", "1"))
    if retries is None:
        retries = int(os.environ.get("POLYTOPE_DYNAMIC_GRID_SERVICE_RETRIES", "1"))
    if retry_timeout is None:
        retry_timeout = float(os.environ.get("POLYTOPE_DYNAMIC_GRID_SERVICE_RETRY_TIMEOUT", "5"))

    timeouts = [timeout] + [retry_timeout] * retries
    last_error = None
    for request_timeout in timeouts:
        try:
            response = requests.post(url, json={"request": req}, timeout=request_timeout)
            response.raise_for_status()
            payload = response.json()
            return (payload["gridspec"], payload["md5hash"])
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_error = exc

    if last_error is not None:
        raise last_error
    raise RuntimeError("dynamic grid remote lookup failed without a captured error")


def lookup_grid_config(req, service_url=None):
    service_url = service_url or os.environ.get("POLYTOPE_DYNAMIC_GRID_SERVICE_URL")
    if service_url:
        return lookup_grid_config_remote(req, service_url)
    return lookup_grid_config_local(req)


def replace_dynamic_grid_options(config_options, req, service_url=None):
    if "georef" not in req.keys():
        raise ValueError("Grid lookup requires request.georef")
    gridspec, md5hash = lookup_grid_config(req, service_url=service_url)
    grid_config = gridspec_to_grid_config(gridspec, md5hash)
    if grid_config is None:
        return False

    for axis_conf in config_options.get("axis_config", []):
        for idx, transformation in enumerate(axis_conf.get("transformations", [])):
            if transformation.get("name") == "mapper":
                axis_conf["transformations"][idx] = grid_config
                return True
    return False
