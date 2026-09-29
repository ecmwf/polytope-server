# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

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
