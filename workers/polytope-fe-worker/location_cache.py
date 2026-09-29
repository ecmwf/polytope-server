# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

"""Process-local LRU cache of FDB field locations for /chunks/v1."""

from collections import OrderedDict
from dataclasses import dataclass
import os
import threading
import time


DEFAULT_SIZE = 4096
DEFAULT_TTL_SECS = 3600.0
_NON_FIELD_KEYS = {"verb", "extract"}


@dataclass(frozen=True)
class FieldLocation:
    """The parts of an FDB location consumed by PathExtractionRequest."""

    path: str
    scheme: str
    offset: int
    length: int
    host: str = ""
    port: int = 0


def canonical_field_key(field_request):
    """Return an insertion-order-independent key for a single field request."""
    return tuple(
        sorted(
            (str(key), str(value))
            for key, value in field_request.items()
            if key not in _NON_FIELD_KEYS
        )
    )


def _nonnegative_int(value, default):
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return default


def _nonnegative_float(value, default):
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return default


def _env_nonnegative_int(name, default):
    raw = os.environ.get(name, "").strip()
    return default if not raw else _nonnegative_int(raw, default)


def _env_nonnegative_float(name, default):
    raw = os.environ.get(name, "").strip()
    return default if not raw else _nonnegative_float(raw, default)


class LocationCache:
    """Thread-safe TTL/LRU cache keyed by canonical single-field requests."""

    def __init__(self, size=None, ttl_secs=None, clock=None):
        self.size = (
            _env_nonnegative_int("POLYTOPE_CHUNKS_LOCCACHE_SIZE", DEFAULT_SIZE)
            if size is None
            else _nonnegative_int(size, DEFAULT_SIZE)
        )
        self.ttl_secs = (
            _env_nonnegative_float("POLYTOPE_CHUNKS_LOCCACHE_TTL_SECS", DEFAULT_TTL_SECS)
            if ttl_secs is None
            else _nonnegative_float(ttl_secs, DEFAULT_TTL_SECS)
        )
        self._clock = clock or time.monotonic
        self._entries = OrderedDict()
        self._lock = threading.Lock()
        self._stats = {"hits": 0, "misses": 0, "evictions": 0, "invalidations": 0}

    @property
    def enabled(self):
        return self.size > 0

    def get(self, field_request):
        if not self.enabled:
            return None
        key = canonical_field_key(field_request)
        now = self._clock()
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                self._stats["misses"] += 1
                return None
            location, expires_at = entry
            if now >= expires_at:
                del self._entries[key]
                self._stats["misses"] += 1
                self._stats["invalidations"] += 1
                return None
            self._entries.move_to_end(key)
            self._stats["hits"] += 1
            return location

    def put(self, field_request, location):
        if not self.enabled:
            return
        key = canonical_field_key(field_request)
        expires_at = self._clock() + self.ttl_secs
        with self._lock:
            if key in self._entries:
                del self._entries[key]
            self._entries[key] = (location, expires_at)
            while len(self._entries) > self.size:
                self._entries.popitem(last=False)
                self._stats["evictions"] += 1

    def invalidate(self, field_request):
        if not self.enabled:
            return False
        key = canonical_field_key(field_request)
        with self._lock:
            removed = self._entries.pop(key, None) is not None
            if removed:
                self._stats["invalidations"] += 1
            return removed

    def clear(self):
        with self._lock:
            self._entries.clear()

    def stats(self):
        with self._lock:
            return {**self._stats, "entries": len(self._entries), "size": self.size}
