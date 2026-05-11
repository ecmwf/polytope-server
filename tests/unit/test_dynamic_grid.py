import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import requests

from polytope_server.dynamic_grid import local as switching_grid_local
from polytope_server.dynamic_grid.helper import (
    build_grid_lookup_request,
    lookup_grid_config,
    lookup_grid_config_remote,
    normalise_lookup_value,
)


class _MockHandler(BaseHTTPRequestHandler):
    response_payload = {
        "gridspec": {
            "type": "lambert_conformal",
            "earth_round": True,
            "radius": 6371229,
            "nv": 0,
            "nx": 10,
            "ny": 20,
            "LoVInDegrees": 1.0,
            "Dx": 1000.0,
            "Dy": 1000.0,
            "latFirstInRadians": 0.1,
            "lonFirstInRadians": 0.2,
            "LoVInRadians": 0.3,
            "Latin1InRadians": 0.4,
            "Latin2InRadians": 0.5,
            "LaDInRadians": 0.6,
        },
        "md5hash": "abc123",
    }
    seen_request = None

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length).decode("utf-8"))
        _MockHandler.seen_request = payload
        body = json.dumps(_MockHandler.response_payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        return


class _MockServer:
    def __enter__(self):
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _MockHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        host, port = self.server.server_address
        self.url = f"http://{host}:{port}"
        return self.url

    def __exit__(self, exc_type, exc, tb):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


def test_lookup_grid_config_remote_service():
    req = {"georef": "u1516b", "class": "d1"}
    with _MockServer() as url:
        gridspec, md5hash = lookup_grid_config(req, service_url=url)

    assert md5hash == "abc123"
    assert gridspec["type"] == "lambert_conformal"
    assert _MockHandler.seen_request == {"request": req}


def test_lookup_grid_config_remote_retries_on_timeout(monkeypatch):
    req = {"georef": "u1516b", "class": "d1"}
    calls = []

    class _Response:
        def raise_for_status(self):
            return None

        def json(self):
            return _MockHandler.response_payload

    def _fake_post(url, json, timeout):
        calls.append(timeout)
        if len(calls) == 1:
            raise requests.Timeout("slow first attempt")
        return _Response()

    monkeypatch.setattr(requests, "post", _fake_post)

    gridspec, md5hash = lookup_grid_config_remote(req, "http://example.com")

    assert md5hash == "abc123"
    assert gridspec["type"] == "lambert_conformal"
    assert calls == [1.0, 5.0]


def test_normalise_lookup_value_picks_first_for_non_georef_lists():
    assert normalise_lookup_value("step", ["9", "10"]) == "9"


def test_normalise_lookup_value_rejects_multi_georef():
    with pytest.raises(ValueError, match="single georef"):
        normalise_lookup_value("georef", ["a", "b"])


def test_build_grid_lookup_request_skips_feature_and_uses_first_values():
    req = {
        "class": ["d1", "d2"],
        "georef": "gcgkrb",
        "step": ["9", "10"],
        "feature": {"type": "timeseries"},
    }

    assert build_grid_lookup_request(req) == {"class": "d1", "georef": "gcgkrb", "step": "9"}


@pytest.fixture(autouse=True)
def reset_switching_grid_memory_cache(monkeypatch):
    monkeypatch.setattr(switching_grid_local, "_GRID_CACHE", None)


def test_lookup_grid_config_local_saves_and_uses_memory_cache(tmp_path, monkeypatch):
    req = {"georef": "u1516b", "class": "d1"}
    cache_file = tmp_path / "grid_cache.json"
    gridspec = {"type": "lambert_conformal", "nx": 10, "ny": 20}
    calls = []
    load_calls = []
    save_calls = []
    releases = []
    original_load_cache = switching_grid_local._load_cache
    original_save_cache = switching_grid_local._save_cache

    def _load_cache():
        load_calls.append(True)
        return original_load_cache()

    def _save_cache(cache):
        save_calls.append(cache.copy())
        return original_save_cache(cache)

    monkeypatch.setattr(switching_grid_local, "_grid_cache_file", lambda: str(cache_file))
    monkeypatch.setattr(switching_grid_local, "_load_cache", _load_cache)
    monkeypatch.setattr(switching_grid_local, "_save_cache", _save_cache)
    monkeypatch.setattr(switching_grid_local, "get_first_grib_message", lambda request: calls.append(request) or "gid")
    monkeypatch.setattr(switching_grid_local, "get_gridspec_and_hash", lambda gid: (gridspec, "abc123"))
    monkeypatch.setattr(switching_grid_local.eccodes, "codes_release", lambda gid: releases.append(gid))

    assert switching_grid_local.lookup_grid_config_local(req) == (gridspec, "abc123")
    assert cache_file.exists()

    monkeypatch.setattr(
        switching_grid_local,
        "get_first_grib_message",
        lambda request: pytest.fail("cache hit should not read from FDB"),
    )

    assert switching_grid_local.lookup_grid_config_local(req) == (gridspec, "abc123")
    assert calls == [req]
    assert len(load_calls) == 1
    assert len(save_calls) == 1
    assert releases == ["gid"]


def test_lookup_grid_config_local_skips_cache_when_disabled(tmp_path, monkeypatch):
    req = {"georef": "u1516b", "class": "d1"}
    cache_file = tmp_path / "grid_cache.json"
    gids = []
    releases = []

    def _get_first_grib_message(request):
        gid = f"gid-{len(gids)}"
        gids.append(gid)
        return gid

    monkeypatch.setenv("POLYTOPE_DISABLE_GRID_CACHE", "1")
    monkeypatch.setattr(switching_grid_local, "_grid_cache_file", lambda: str(cache_file))
    monkeypatch.setattr(switching_grid_local, "get_first_grib_message", _get_first_grib_message)
    monkeypatch.setattr(switching_grid_local, "get_gridspec_and_hash", lambda gid: ({"gid": gid}, gid))
    monkeypatch.setattr(switching_grid_local, "_save_cache", lambda cache: pytest.fail("cache save should be disabled"))
    monkeypatch.setattr(switching_grid_local.eccodes, "codes_release", lambda gid: releases.append(gid))

    assert switching_grid_local.lookup_grid_config_local(req) == ({"gid": "gid-0"}, "gid-0")
    assert switching_grid_local.lookup_grid_config_local(req) == ({"gid": "gid-1"}, "gid-1")
    assert gids == ["gid-0", "gid-1"]
    assert releases == ["gid-0", "gid-1"]
    assert not cache_file.exists()
