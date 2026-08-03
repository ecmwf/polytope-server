#!/usr/bin/env python3
"""Select container images affected by a list of repository paths.

The script reads newline-delimited repository-relative paths from standard input
and writes a GitHub Actions dynamic-matrix object to standard output.

Keep the dependency fan-out deliberately conservative: a false positive costs a
container build, whereas a false negative validates a stale image.
"""

from __future__ import annotations

import json
import sys


IMAGES = {
    "frontend": "eccr.ecmwf.int/polytope/frontend",
    "polytope-fe-worker": "eccr.ecmwf.int/polytope/polytope-fe-worker",
    "fdb-worker": "eccr.ecmwf.int/polytope/fdb-worker",
    "mars-worker": "eccr.ecmwf.int/polytope/mars-worker",
    "test-worker": "eccr.ecmwf.int/polytope/test-worker",
    "polytope-loadgen": "eccr.ecmwf.int/polytope/polytope-loadgen",
}

ALL_IMAGES = set(IMAGES)
WORKERS = {
    "polytope-fe-worker",
    "fdb-worker",
    "mars-worker",
    "test-worker",
}


def selected_images(paths: list[str]) -> set[str]:
    selected: set[str] = set()

    for path in paths:
        # Root build metadata, shared build configuration, and workflow changes
        # can affect every artifact or its validation behaviour.
        if path in {"Cargo.toml", "Cargo.lock", ".dockerignore", "skaffold.yaml"}:
            return ALL_IMAGES
        if path.startswith(".github/") or path.startswith("docker/"):
            return ALL_IMAGES

        if path.startswith("frontend/") or path.startswith("utils/metkit/"):
            selected.add("frontend")
        elif path.startswith("workers/mars-worker/"):
            selected.add("mars-worker")
        elif path.startswith("workers/fdb-worker/"):
            selected.add("fdb-worker")
        elif path.startswith("workers/polytope-fe-worker/"):
            selected.add("polytope-fe-worker")
        elif path.startswith("workers/test-worker/"):
            selected.add("test-worker")
        elif path.startswith("loadgen/"):
            selected.add("polytope-loadgen")
        elif path.startswith("workers/common/"):
            selected.update(WORKERS)
        elif path.startswith("observability/"):
            selected.add("frontend")
            selected.update(WORKERS)

    return selected


def main() -> None:
    paths = [line.strip() for line in sys.stdin if line.strip()]
    names = selected_images(paths)
    matrix = {
        "include": [
            {"name": name, "image": IMAGES[name]}
            for name in IMAGES
            if name in names
        ]
    }
    json.dump(matrix, sys.stdout, separators=(",", ":"))


if __name__ == "__main__":
    main()
