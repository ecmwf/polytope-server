#
# Copyright 2022 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
#

import os

import pytest

from polytope_server.common.datasource.mars import MARSDataSource
from polytope_server.common.request import PolytopeRequest
from polytope_server.common.user import User

FIFO_BUFFER_SIZE = 2 * 1024 * 1024


@pytest.fixture
def fake_mars_command(tmp_path):
    script = tmp_path / "fake_mars.py"
    script.write_text(
        "#!/usr/bin/env python3\n"
        "import os\n"
        "import re\n"
        "import sys\n"
        "import time\n"
        "\n"
        "request = open(sys.argv[1]).read()\n"
        'match = re.search(r\'target="([^"]+)"\', request)\n'
        "target = match.group(1) if match else None\n"
        "mode = os.environ['POLYTOPE_TEST_MARS_MODE']\n"
        "\n"
        "if mode == 'stall_before_open':\n"
        "    while True:\n"
        "        time.sleep(1)\n"
        "elif mode == 'stall_after_open':\n"
        "    with open(target, 'wb', buffering=0):\n"
        "        while True:\n"
        "            time.sleep(1)\n"
        "elif mode == 'stall_mid_stream':\n"
        "    with open(target, 'wb', buffering=0) as f:\n"
        "        chunk = b'x' * 65536\n"
        f"        for _ in range({FIFO_BUFFER_SIZE} // len(chunk)):\n"
        "            f.write(chunk)\n"
        "        while True:\n"
        "            time.sleep(1)\n"
        "elif mode == 'stall_file_io':\n"
        "    while True:\n"
        "        time.sleep(1)\n"
        "else:\n"
        "    raise RuntimeError(f'Unsupported mode: {mode}')\n"
    )
    script.chmod(0o755)
    return str(script)


def make_request():
    request = PolytopeRequest()
    request.user = User("test-user", "test-realm")
    request.user.attributes = {
        "ecmwf-email": "test@example.com",
        "ecmwf-apikey": "secret",
    }
    request.coerced_request = {}
    return request


def make_datasource(tmp_path, command, timeout, use_file_io=False):
    return MARSDataSource(
        {
            "name": "mars",
            "type": "mars",
            "command": command,
            "protocol": "remote",
            "tmp_dir": str(tmp_path),
            "timeout": timeout,
            "use_file_io": use_file_io,
        }
    )


def assert_destroyed(ds, request):
    ds.destroy(request)
    if ds.subprocess is not None and ds.subprocess.subprocess is not None:
        assert ds.subprocess.returncode() is not None
    assert not os.path.exists(ds.request_file)
    if ds.use_file_io:
        assert not os.path.exists(ds.output_file)
    else:
        assert not os.path.exists(ds.fifo.path)


def test_mars_timeout_before_fifo_open(tmp_path, fake_mars_command, monkeypatch):
    monkeypatch.setenv("POLYTOPE_TEST_MARS_MODE", "stall_before_open")
    ds = make_datasource(tmp_path, fake_mars_command, timeout=0.5)
    request = make_request()

    try:
        with pytest.raises(TimeoutError, match="timed out"):
            ds.dispatch(request, None)
    finally:
        assert_destroyed(ds, request)


def test_mars_timeout_after_fifo_open_before_data(tmp_path, fake_mars_command, monkeypatch):
    monkeypatch.setenv("POLYTOPE_TEST_MARS_MODE", "stall_after_open")
    ds = make_datasource(tmp_path, fake_mars_command, timeout=0.5)
    request = make_request()

    try:
        with pytest.raises(TimeoutError, match="timed out"):
            ds.dispatch(request, None)
    finally:
        assert_destroyed(ds, request)


def test_mars_timeout_mid_stream(tmp_path, fake_mars_command, monkeypatch):
    monkeypatch.setenv("POLYTOPE_TEST_MARS_MODE", "stall_mid_stream")
    ds = make_datasource(tmp_path, fake_mars_command, timeout=1.5)
    request = make_request()

    try:
        assert ds.dispatch(request, None)

        result = ds.result(request)
        first_chunk = next(result)
        assert first_chunk == b"x" * FIFO_BUFFER_SIZE

        with pytest.raises(TimeoutError, match="timed out"):
            next(result)
    finally:
        assert_destroyed(ds, request)


def test_mars_timeout_with_file_io(tmp_path, fake_mars_command, monkeypatch):
    monkeypatch.setenv("POLYTOPE_TEST_MARS_MODE", "stall_file_io")
    ds = make_datasource(tmp_path, fake_mars_command, timeout=0.5, use_file_io=True)
    request = make_request()

    try:
        with pytest.raises(TimeoutError, match="timed out"):
            ds.dispatch(request, None)
    finally:
        assert_destroyed(ds, request)
