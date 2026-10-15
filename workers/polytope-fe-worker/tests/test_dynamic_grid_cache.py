# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

"""The dynamic-grid cache must not share a grid between requests that differ in anything but step/param/levelist/levtype."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from dynamic_grid import local

BASE = {
    "class": "od",
    "stream": "oper",
    "type": "fc",
    "date": "20260101",
    "time": "0000",
    "expver": "aagw",
    "georef": "u09tvk",
    "levtype": "sfc",
    "param": "167",
    "step": "0",
}


@pytest.fixture
def fake_fdb(monkeypatch, tmp_path):
    monkeypatch.setattr(local, "_grid_cache_file", lambda: str(tmp_path / "grid_cache.json"))
    monkeypatch.setattr(local, "_GRID_CACHE", None)
    monkeypatch.delenv("POLYTOPE_DISABLE_GRID_CACHE", raising=False)

    hashes = {"aagw": "f86aef8a2ff9210d02f1c8f90bf13f88", "aab0": "17c7c24ff8d8b5ca3b595e36abe3fe92"}
    calls = []

    def fake_first_message(req):
        calls.append(req)
        return req["expver"]

    monkeypatch.setattr(local, "get_first_grib_message", fake_first_message)
    monkeypatch.setattr(local, "get_gridspec_and_hash", lambda gid: ({"type": gid}, hashes[gid]))
    monkeypatch.setattr(local.eccodes, "codes_release", lambda gid: None)
    return calls


def test_same_georef_different_expver_does_not_share_grid(fake_fdb):
    aagw = local.lookup_grid_config_local(dict(BASE, expver="aagw"))
    aab0 = local.lookup_grid_config_local(dict(BASE, expver="aab0"))

    assert aagw[1] == "f86aef8a2ff9210d02f1c8f90bf13f88"
    assert aab0[1] == "17c7c24ff8d8b5ca3b595e36abe3fe92"
    assert len(fake_fdb) == 2


def test_grid_invariant_keys_share_cache_entry(fake_fdb):
    first = local.lookup_grid_config_local(BASE)
    second = local.lookup_grid_config_local(dict(BASE, step="6", param="228", levtype="pl", levelist="500"))

    assert first == second
    assert len(fake_fdb) == 1


def test_different_date_does_not_share_cache_entry(fake_fdb):
    local.lookup_grid_config_local(BASE)
    local.lookup_grid_config_local(dict(BASE, date="20260102"))

    assert len(fake_fdb) == 2


def test_cache_key_is_canonical():
    a = {"georef": "u09tvk", "expver": "aagw", "Date": 20260101, "step": "0", "param": "167"}
    b = {"param": ["1", "2"], "date": "20260101", "expver": ["aagw"], "georef": "u09tvk"}
    assert local._cache_key(a) == local._cache_key(b)
    assert local._cache_key({"georef": "x", "number": "1/2"}) == local._cache_key({"georef": "x", "number": ["2", "1"]})
    assert local._cache_key(a) != local._cache_key(dict(a, expver="aab0"))
