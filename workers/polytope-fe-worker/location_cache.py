# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

"""Process-local LRU cache of FDB field locations for /chunks/v1."""

from collections import OrderedDict
from dataclasses import dataclass, replace
import logging
import os
import threading
import time

import yaml


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


def _store_stem(host):
    """Return the exact first-label store stem used to join FDB host aliases."""
    label = str(host or "").rstrip(".").split(".", 1)[0].lower()
    return label[:-4] if label.endswith("-ope") else label


def _parse_endpoint(value):
    """Parse the host:port form used by gribjump's servermap."""
    host, separator, raw_port = str(value or "").strip().rpartition(":")
    if not separator or not host or not raw_port:
        return None
    try:
        port = int(raw_port)
    except ValueError:
        return None
    if not 0 < port <= 65535:
        return None
    return host.rstrip("."), port


class LocationServerMap:
    """Translate pyfdb location hosts to configured gribjump FDB endpoints."""

    def __init__(self, servermap=()):
        endpoints = {}
        ambiguous = set()
        for entry in servermap or ():
            if not isinstance(entry, dict):
                continue
            endpoint = _parse_endpoint(entry.get("fdb"))
            if endpoint is None:
                continue
            stem = _store_stem(endpoint[0])
            if not stem:
                continue
            if stem in endpoints and endpoints[stem] != endpoint:
                ambiguous.add(stem)
            else:
                endpoints[stem] = endpoint
        for stem in ambiguous:
            endpoints.pop(stem, None)
            logging.warning("Ignoring ambiguous gribjump servermap FDB stem %s", stem)
        self._endpoints = endpoints
        self._logged_hosts = set()
        self._lock = threading.Lock()

    @classmethod
    def from_config(cls, path=None):
        path = path or os.environ.get("GRIBJUMP_CONFIG_FILE", "/tmp/gribjump.yaml")
        try:
            with open(path, encoding="utf-8") as stream:
                config = yaml.safe_load(stream) or {}
            servermap = config.get("servermap", []) if isinstance(config, dict) else []
        except (OSError, yaml.YAMLError) as exc:
            logging.warning("Cannot load gribjump servermap from %s: %s", path, exc)
            servermap = []
        return cls(servermap)

    def translate(self, location):
        """Return a location routed through servermap, or None when unmappable."""
        endpoint = self._endpoints.get(_store_stem(location.host))
        if endpoint is None:
            return None
        external_host, external_port = endpoint
        source = f"{location.host}:{location.port}"
        with self._lock:
            first = source not in self._logged_hosts
            self._logged_hosts.add(source)
        if first:
            logging.info(
                "Translating cached FDB location %s to %s:%d via gribjump servermap",
                source,
                external_host,
                external_port,
            )
        return replace(location, host=external_host, port=external_port)


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
