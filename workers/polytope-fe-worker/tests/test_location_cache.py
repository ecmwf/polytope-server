# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

import logging
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

import location_cache  # type: ignore[import-not-found]  # noqa: E402


def field(name):
    return {"verb": "retrieve", "param": name, "date": "20200101", "extract": {}}


def location(name):
    return location_cache.FieldLocation(f"/{name}", "file", 0, 10)


def test_lru_eviction_at_configured_size():
    cache = location_cache.LocationCache(size=2, ttl_secs=60)
    cache.put(field("a"), location("a"))
    cache.put(field("b"), location("b"))
    assert cache.get(field("a")) == location("a")  # a is now most recently used
    cache.put(field("c"), location("c"))

    assert cache.get(field("b")) is None
    assert cache.get(field("a")) == location("a")
    assert cache.get(field("c")) == location("c")
    assert cache.stats()["evictions"] == 1


def test_ttl_expiry_invalidates_entry():
    now = [100.0]
    cache = location_cache.LocationCache(size=2, ttl_secs=5, clock=lambda: now[0])
    cache.put(field("a"), location("a"))
    now[0] = 104.9
    assert cache.get(field("a")) == location("a")
    now[0] = 105.0

    assert cache.get(field("a")) is None
    assert cache.stats()["invalidations"] == 1
    assert cache.stats()["misses"] == 1


def test_key_is_sorted_and_excludes_non_field_keys():
    left = {"verb": "retrieve", "date": "20200101", "param": "167", "extract": {"x": 1}}
    right = {"param": "167", "date": "20200101"}
    assert location_cache.canonical_field_key(left) == location_cache.canonical_field_key(right)


def test_size_zero_is_disabled_and_does_not_record_misses():
    cache = location_cache.LocationCache(size=0, ttl_secs=60)
    cache.put(field("a"), location("a"))
    assert cache.get(field("a")) is None
    assert cache.stats() == {
        "hits": 0,
        "misses": 0,
        "evictions": 0,
        "invalidations": 0,
        "entries": 0,
        "size": 0,
    }


def test_servermap_translates_internal_host_and_port_zero(tmp_path, caplog):
    caplog.set_level(logging.INFO)
    config = tmp_path / "gribjump.yaml"
    config.write_text(
        "servermap:\n"
        "  - fdb: mn5-prod-store6-ope.mn5.apps.example:10000\n"
        "    gribjump: mn5-prod-store6-ope.mn5.apps.example:10001\n"
    )
    servermap = location_cache.LocationServerMap.from_config(config)
    internal = location_cache.FieldLocation(
        "/archive/data", "fdb", 1, 2, "mn5-prod-store6.novalocal", 0
    )

    translated = servermap.translate(internal)
    servermap.translate(internal)

    assert translated == location_cache.FieldLocation(
        "/archive/data",
        "fdb",
        1,
        2,
        "mn5-prod-store6-ope.mn5.apps.example",
        10000,
    )
    messages = [record.getMessage() for record in caplog.records]
    assert len([message for message in messages if "Translating cached FDB" in message]) == 1


def test_servermap_requires_exact_normalised_store_stem():
    servermap = location_cache.LocationServerMap(
        [{"fdb": "mn5-prod-store6-ope.example:10000"}]
    )
    near_match = location_cache.FieldLocation(
        "/archive/data", "fdb", 1, 2, "mn5-prod-store60.novalocal", 0
    )

    assert servermap.translate(near_match) is None
