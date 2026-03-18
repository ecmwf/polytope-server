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
VENV_PYTHON = REPO_ROOT / ".venv" / "bin" / "python"

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

        bits:
          routes:
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
    # Import here so the venv is active when this runs
    sys.path.insert(
        0, str(REPO_ROOT / ".venv" / "lib" / "python3.12" / "site-packages")
    )
    from polytope.api import Client

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
    import urllib.request

    with urllib.request.urlopen(f"{polytope_server}/api/v1/test") as r:
        assert r.read().decode() == "Polytope server is alive"


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
            "test-collection",
            {"class": "od", "stream": "oper"},
            output_file=output,
        )
        downloaded = Path(output).read_bytes()
        assert downloaded == FAKE_GRIB
    finally:
        os.unlink(output)


def test_retrieve_unknown_collection_still_works(client):
    """Collection name is ignored by the server; any name should route fine."""
    with tempfile.NamedTemporaryFile(suffix=".grib", delete=False) as f:
        output = f.name

    try:
        client.retrieve(
            "totally-made-up-collection",
            {"class": "rd"},
            output_file=output,
        )
        assert Path(output).read_bytes() == FAKE_GRIB
    finally:
        os.unlink(output)


# ---------------------------------------------------------------------------
# v2 tests (direct HTTP — no polytope-client wrapper needed)
# ---------------------------------------------------------------------------


def test_v2_health(polytope_server):
    import urllib.request

    with urllib.request.urlopen(f"{polytope_server}/api/v2/health") as r:
        assert r.read().decode() == "Polytope server is alive"


def test_v2_no_collections_endpoint(polytope_server):
    import urllib.error, urllib.request

    try:
        urllib.request.urlopen(f"{polytope_server}/api/v2/collections")
        assert False, "expected 404"
    except urllib.error.HTTPError as e:
        assert e.code == 404


def test_v2_submit_and_retrieve(polytope_server):
    import json, urllib.request

    # POST → 303 → poll loop → 200 with data, all followed automatically.
    req = urllib.request.Request(
        f"{polytope_server}/api/v2/requests",
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
        job_id = json.loads(r.read())["id"]

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

        authentication:
          url: "http://127.0.0.1:{mock_authotron}"
          secret: "{JWT_SECRET}"

        bits:
          routes:
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
        f"{authed_polytope_server}/api/v2/requests",
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
        f"{authed_polytope_server}/api/v2/requests",
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

    for path in ["/api/v1/test", "/api/v1/collections", "/api/v1/requests"]:
        try:
            urllib.request.urlopen(f"{authed_polytope_server}{path}")
            assert False, f"expected 401 for {path}"
        except urllib.error.HTTPError as e:
            assert e.code == 401, f"{path} returned {e.code}, expected 401"
