import os
import threading
from concurrent.futures import ThreadPoolExecutor
from http.server import ThreadingHTTPServer

import requests

from polytope_server.dynamic_grid.service import SwitchingGridHandler

SERVICE_URL = os.environ.get("POLYTOPE_DYNAMIC_GRID_TEST_URL", "http://127.0.0.1:9126")

# From polytope-config/tools/sample_requests/bologna.json,
# request id: deode-switching-grids-u1516b-pr164.
# The dynamic-grid service expects the lookup/pre_path keys, not the feature body.
DEODE_SWITCHING_GRID_U1516B_REQUEST = {
    "class": "d1",
    "dataset": "on-demand-extremes-dt",
    "expver": "0099",
    "stream": "oper",
    "date": "20250601",
    "time": "0000",
    "type": "fc",
    "georef": "u1516b",
    "levtype": "sfc",
    "step": "13h30m",
    "param": "3074",
}


class _DynamicGridServer:
    def __enter__(self):
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), SwitchingGridHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        host, port = self.server.server_address
        return f"http://{host}:{port}"

    def __exit__(self, exc_type, exc, tb):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


def test_dynamic_grid_service_healthz():
    response = requests.get(f"{SERVICE_URL}/healthz", timeout=5)

    response.raise_for_status()
    assert response.json() == {"ok": True}


def test_dynamic_grid_service_lookup_bologna_deode_request():
    payload = _lookup_bologna_deode_request()

    assert_valid_bologna_deode_response(payload)


def test_dynamic_grid_service_rejects_request_without_georef():
    with _DynamicGridServer() as service_url:
        response = requests.post(
            f"{service_url}/lookup-grid-config",
            json={"request": {"class": "d1"}},
            timeout=5,
        )

    assert response.status_code == 400
    assert response.json() == {"error": "request.georef is required"}


def test_dynamic_grid_service_handles_concurrent_lookup_requests():
    workers = 8

    with ThreadPoolExecutor(max_workers=workers) as pool:
        payloads = list(pool.map(lambda _: _lookup_bologna_deode_request(), range(workers)))

    md5hashes = {payload["md5hash"] for payload in payloads}
    assert len(md5hashes) == 1
    for payload in payloads:
        assert_valid_bologna_deode_response(payload)


def _lookup_bologna_deode_request():
    response = requests.post(
        f"{SERVICE_URL}/lookup-grid-config",
        json={"request": DEODE_SWITCHING_GRID_U1516B_REQUEST},
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def assert_valid_bologna_deode_response(payload):
    assert isinstance(payload["gridspec"], dict)
    assert payload["gridspec"]["type"] == "lambert_conformal"
    assert payload["gridspec"]["nx"] > 0
    assert payload["gridspec"]["ny"] > 0
    assert isinstance(payload["md5hash"], str)
    assert payload["md5hash"]
