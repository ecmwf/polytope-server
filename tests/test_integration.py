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
            default:
              - target::http:
                  url: "http://127.0.0.1:{mock_backend}/"
    """)

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False
    ) as f:
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
    sys.path.insert(0, str(REPO_ROOT / ".venv" / "lib" / "python3.12" / "site-packages"))
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
    assert body == ["all"]


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
