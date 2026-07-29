# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

"""
Integration test: polytope-client <-> polytope-server <-> mock data backend.

Flow:
  1. Start a mock HTTP backend that returns fake GRIB data.
  2. Write a server config pointing bits at that backend.
  3. Start polytope-server.
  4. Use polytope-client to submit a retrieve request and download the result.
  5. Assert the downloaded bytes match what the mock sent.
"""

import os
import signal
import socket
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).parent.parent
SERVER_BIN = REPO_ROOT / "target" / "debug" / "polytope-server"

try:
    from polytope.api import Client as _PolytopeClient
    _POLYTOPE_CLIENT_AVAILABLE = True
except ImportError:
    _POLYTOPE_CLIENT_AVAILABLE = False

requires_polytope_client = pytest.mark.skipif(
    not _POLYTOPE_CLIENT_AVAILABLE, reason="polytope-client not installed"
)


def _import_polytope_client():
    from polytope.api import Client
    return Client

FAKE_GRIB = b"\x00\x01\x02\x03GRIB_FAKE_DATA\xff\xfe"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_port(port, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"Port {port} did not open within {timeout}s")


# ---------------------------------------------------------------------------
# Mock GRIB backend
# ---------------------------------------------------------------------------


class GribHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        # consume body
        length = int(self.headers.get("Content-Length", 0))
        self.rfile.read(length)

        self.send_response(200)
        self.send_header("Content-Type", "application/x-grib")
        self.send_header("Content-Length", str(len(FAKE_GRIB)))
        self.end_headers()
        self.wfile.write(FAKE_GRIB)

    def log_message(self, *_):
        pass  # silence default stdout logging


def start_mock_backend():
    port = free_port()
    server = HTTPServer(("127.0.0.1", port), GribHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return port, server


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def mock_backend():
    port, server = start_mock_backend()
    yield port
    server.shutdown()


@pytest.fixture(scope="module")
def polytope_server(mock_backend):
    server_port = free_port()

    config = textwrap.dedent(f"""\
        server:
          host: "127.0.0.1"
          port: {server_port}

        polytope:
          site: tst
          env: tst

        bits:
          collections:
            all:
              - default:
                  - target::http:
                      url: "http://127.0.0.1:{mock_backend}/"
    """)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(config)
        config_path = f.name

    proc = subprocess.Popen(
        [str(SERVER_BIN), config_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        wait_for_port(server_port)
        yield f"http://127.0.0.1:{server_port}"
    finally:
        proc.terminate()
        proc.wait(timeout=5)
        os.unlink(config_path)


@pytest.fixture()
def client(polytope_server):
    Client = pytest.importorskip("polytope.api").Client
    return Client(
        address=polytope_server,
        user_key="test-key",
        user_email="test@example.com",
        insecure=True,
        quiet=True,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_health(polytope_server):
    import json, urllib.request

    with urllib.request.urlopen(f"{polytope_server}/api/v1/test") as r:
        body = json.loads(r.read())
    assert body == {"message": "Polytope server is alive"}


def test_collections(polytope_server):
    import json, urllib.request

    with urllib.request.urlopen(f"{polytope_server}/api/v1/collections") as r:
        body = json.loads(r.read())
    assert body == {"message": ["all"]}


def test_retrieve(client):
    with tempfile.NamedTemporaryFile(suffix=".grib", delete=False) as f:
        output = f.name

    try:
        client.retrieve(
            "all",
            {"class": "od", "stream": "oper"},
            output_file=output,
        )
        downloaded = Path(output).read_bytes()
        assert downloaded == FAKE_GRIB
    finally:
        os.unlink(output)


def test_retrieve_unknown_collection_returns_error(client):
    """Unknown collection name → 404 from v1, surfaced as HTTPResponseError."""
    from polytope.api import helpers

    with pytest.raises(helpers.HTTPResponseError):
        client.retrieve(
            "totally-made-up-collection",
            {"class": "rd"},
            output_file="/tmp/should_not_exist.grib",
        )


# ---------------------------------------------------------------------------
# v2 tests (direct HTTP — no polytope-client wrapper needed)
# ---------------------------------------------------------------------------


def test_v2_health(polytope_server):
    import urllib.request

    with urllib.request.urlopen(f"{polytope_server}/api/v2/health") as r:
        assert r.read().decode() == "Polytope server is alive"


def test_v2_no_collections_endpoint(polytope_server):
    import json, urllib.request

    with urllib.request.urlopen(f"{polytope_server}/api/v2/collections") as r:
        body = json.loads(r.read())
    assert body == {"collections": ["all"]}


def test_v2_submit_and_retrieve(polytope_server):
    import json, urllib.request

    req = urllib.request.Request(
        f"{polytope_server}/api/v2/all/requests",
        data=json.dumps({"class": "od", "stream": "oper"}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as r:
        assert r.status == 200
        assert r.read() == FAKE_GRIB


def test_v2_cancel(polytope_server):
    import json, urllib.request

    # Submit via v1 to get a job ID without blocking on inline poll.
    req = urllib.request.Request(
        f"{polytope_server}/api/v1/requests/all",
        data=json.dumps({"verb": "retrieve", "request": {"class": "od"}}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as r:
        assert r.status == 202
        # The job ID is in the Location header ("./  <id>"), not the body.
        location = r.headers.get("Location")
        job_id = location.split("/")[-1]

    cancel_req = urllib.request.Request(
        f"{polytope_server}/api/v2/requests/{job_id}",
        method="DELETE",
    )
    with urllib.request.urlopen(cancel_req) as r:
        assert r.status == 200
        assert json.loads(r.read())["status"] == "cancelled"


# ---------------------------------------------------------------------------
# Auth-o-tron integration
# ---------------------------------------------------------------------------

JWT_SECRET = "integration-test-secret"
VALID_USER = "testuser"
VALID_PASSWORD = "testpass"
VALID_REALM = "testrealm"


class AuthOTronHandler(BaseHTTPRequestHandler):
    """Minimal auth-o-tron mock: validates Basic auth → returns signed JWT."""

    def do_GET(self):
        if self.path != "/authenticate":
            self.send_error(404)
            return

        import base64
        from jose import jwt

        auth = self.headers.get("Authorization", "")
        if not auth.startswith("Basic "):
            self.send_response(401)
            self.send_header("WWW-Authenticate", 'Basic realm="test"')
            self.end_headers()
            return

        try:
            decoded = base64.b64decode(auth[6:]).decode()
            user, password = decoded.split(":", 1)
        except Exception:
            self.send_response(401)
            self.send_header("WWW-Authenticate", "Bearer")
            self.end_headers()
            return

        if user != VALID_USER or password != VALID_PASSWORD:
            self.send_response(401)
            self.send_header("WWW-Authenticate", "Bearer")
            self.end_headers()
            return

        token = jwt.encode(
            {
                "username": user,
                "realm": VALID_REALM,
                "roles": ["default"],
                "exp": int(time.time()) + 3600,
            },
            JWT_SECRET,
            algorithm="HS256",
        )

        self.send_response(200)
        self.send_header("Authorization", f"Bearer {token}")
        self.end_headers()
        self.wfile.write(b"Authenticated successfully")

    def log_message(self, *_):
        pass


def start_mock_authotron():
    port = free_port()
    server = HTTPServer(("127.0.0.1", port), AuthOTronHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return port, server


@pytest.fixture(scope="module")
def mock_authotron():
    port, server = start_mock_authotron()
    yield port
    server.shutdown()


@pytest.fixture(scope="module")
def authed_polytope_server(mock_backend, mock_authotron):
    """polytope-server with auth-o-tron enabled."""
    server_port = free_port()

    config = textwrap.dedent(f"""\
        server:
          host: "127.0.0.1"
          port: {server_port}

        polytope:
          site: tst
          env: tst

        authentication:
          url: "http://127.0.0.1:{mock_authotron}"
          secret: "{JWT_SECRET}"

        bits:
          collections:
            all:
              - default:
                  - target::http:
                      url: "http://127.0.0.1:{mock_backend}/"
    """)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(config)
        config_path = f.name

    proc = subprocess.Popen(
        [str(SERVER_BIN), config_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        wait_for_port(server_port)
        yield f"http://127.0.0.1:{server_port}"
    finally:
        proc.terminate()
        proc.wait(timeout=5)
        os.unlink(config_path)


def test_auth_health_is_public(authed_polytope_server):
    import urllib.request

    with urllib.request.urlopen(f"{authed_polytope_server}/api/v2/health") as r:
        assert r.status == 200


def test_auth_reject_no_credentials(authed_polytope_server):
    import json, urllib.error, urllib.request

    req = urllib.request.Request(
        f"{authed_polytope_server}/api/v2/all/requests",
        data=json.dumps({"class": "od"}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(req)
        assert False, "expected 401"
    except urllib.error.HTTPError as e:
        assert e.code == 401
        assert "WWW-Authenticate" in e.headers


def test_auth_reject_bad_credentials(authed_polytope_server):
    import base64, json, urllib.error, urllib.request

    creds = base64.b64encode(b"wrong:creds").decode()
    req = urllib.request.Request(
        f"{authed_polytope_server}/api/v1/collections",
        headers={"Authorization": f"Basic {creds}"},
    )
    try:
        urllib.request.urlopen(req)
        assert False, "expected 401"
    except urllib.error.HTTPError as e:
        assert e.code == 401


def test_auth_valid_credentials_pass_through(authed_polytope_server):
    import base64, urllib.request

    creds = base64.b64encode(f"{VALID_USER}:{VALID_PASSWORD}".encode()).decode()
    req = urllib.request.Request(
        f"{authed_polytope_server}/api/v2/health",
        headers={"Authorization": f"Basic {creds}"},
    )
    with urllib.request.urlopen(req) as r:
        assert r.status == 200


def test_auth_v2_submit_with_valid_credentials(authed_polytope_server):
    import base64, json, urllib.request

    creds = base64.b64encode(f"{VALID_USER}:{VALID_PASSWORD}".encode()).decode()
    req = urllib.request.Request(
        f"{authed_polytope_server}/api/v2/all/requests",
        data=json.dumps({"class": "od", "stream": "oper"}).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Basic {creds}",
        },
        method="POST",
    )
    with urllib.request.urlopen(req) as r:
        assert r.status == 200
        assert r.read() == FAKE_GRIB


def test_auth_v1_requires_auth(authed_polytope_server):
    import urllib.error, urllib.request

    # /api/v1/test is intentionally public (health check); collections and
    # requests require authentication.
    for path in ["/api/v1/collections", "/api/v1/requests"]:
        try:
            urllib.request.urlopen(f"{authed_polytope_server}{path}")
            assert False, f"expected 401 for {path}"
        except urllib.error.HTTPError as e:
            assert e.code == 401, f"{path} returned {e.code}, expected 401"


# ---------------------------------------------------------------------------
# v1 error propagation: worker rejection surfaces as a PolytopeError, not
# as a silent success or an unexpected HTTP status code on the status poll.
# ---------------------------------------------------------------------------

BACKEND_ERROR_MESSAGE = "Unknown param 999: invalid MARS request"


class RejectingHandler(BaseHTTPRequestHandler):
    """Mock backend that always refuses with a 422 + plain-text reason,
    simulating a datasource rejecting an invalid order."""

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        self.rfile.read(length)
        body = BACKEND_ERROR_MESSAGE.encode()
        self.send_response(422)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


@pytest.fixture(scope="module")
def rejecting_backend():
    port = free_port()
    server = HTTPServer(("127.0.0.1", port), RejectingHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    yield port
    server.shutdown()


@pytest.fixture(scope="module")
def rejecting_polytope_server(rejecting_backend):
    server_port = free_port()
    config = textwrap.dedent(f"""\
        server:
          host: "127.0.0.1"
          port: {server_port}

        polytope:
          site: tst
          env: tst

        bits:
          collections:
            test-collection:
              - default:
                  - target::http:
                      url: "http://127.0.0.1:{rejecting_backend}/"
    """)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(config)
        config_path = f.name
    proc = subprocess.Popen(
        [str(SERVER_BIN), config_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        wait_for_port(server_port)
        yield f"http://127.0.0.1:{server_port}"
    finally:
        proc.terminate()
        proc.wait(timeout=5)
        os.unlink(config_path)


@pytest.fixture()
def rejecting_client(rejecting_polytope_server):
    Client = pytest.importorskip("polytope.api").Client
    return Client(
        address=rejecting_polytope_server,
        user_key="test-key",
        user_email="test@example.com",
        insecure=True,
        quiet=True,
    )


@requires_polytope_client
def test_v1_rejected_order_raises_polytope_error(rejecting_client):
    """An order rejected by the backend (worker-level error) must raise a
    PolytopeError via the v1 API — not silently succeed, not crash with an
    unexpected HTTP status on the status-poll endpoint.

    The v1 poll endpoint now returns 202 {"status": "failed", "message": ...}
    instead of 400, so the error is surfaced through the client's own
    status-check branch rather than through try_request's 4xx guard.
    We verify this by checking the exception description starts with
    "The request failed with the following error" (the client's polling
    branch), rather than "HTTP CLIENT ERROR (400)" (the old 4xx guard).
    """
    from polytope.api import helpers

    with pytest.raises(helpers.PolytopeError) as exc_info:
        rejecting_client.retrieve(
            "test-collection",
            {"class": "od", "param": "999"},
            output_file="/tmp/should_not_be_written.grib",
        )

    assert not Path("/tmp/should_not_be_written.grib").exists(), \
        "output file must not have been written for a failed request"

    err_str = str(exc_info.value)
    # The error must NOT have come from try_request's >=400 guard — that was
    # the old behaviour where the status endpoint returned 400.
    assert "HTTP CLIENT ERROR (400)" not in err_str, (
        f"Got a 400 response on the status endpoint — the old broken behaviour. Got: {err_str}"
    )
    # The failure reason must appear somewhere in the exception string.
    assert "no route matched the request" in err_str, (
        f"Expected failure reason in exception, got: {err_str}"
    )
