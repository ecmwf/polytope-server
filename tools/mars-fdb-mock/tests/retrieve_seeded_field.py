#!/usr/bin/env python3
"""Retrieve one seeded mars-fdb-mock field with fdb-read and validate bytes."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


def run(command: list[str], *, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    print("+ " + " ".join(command), flush=True)
    return subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env)


def marker_selector(marker: Path) -> str:
    for line in marker.read_text(encoding="utf-8").splitlines():
        if line.startswith("selector="):
            return line.split("=", 1)[1]
    raise RuntimeError(f"marker {marker} does not contain a selector= line")


def mars_request(selector: str) -> str:
    keys = dict(item.split("=", 1) for item in selector.split(",") if "=" in item)
    order = ["class", "expver", "stream", "type", "levtype", "date", "time", "domain", "step", "param"]
    return "retrieve,\n" + ",\n".join(f"    {key}={keys[key]}" for key in order if key in keys) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--marker", type=Path, default=Path(os.environ.get("MARS_FDB_MOCK_SEED_MARKER", "/data/fdb/.seeded")))
    parser.add_argument("--config", type=Path, default=Path(os.environ.get("FDB5_CONFIG_FILE", "/tmp/dhshome/etc/fdb/config.yaml")))
    parser.add_argument("--out", type=Path, default=Path("/tmp/seed/retrieved.grib"))
    args = parser.parse_args()

    selector = marker_selector(args.marker)
    request = mars_request(selector)
    request_path = args.out.with_suffix(".request")
    request_path.parent.mkdir(parents=True, exist_ok=True)
    request_path.write_text(request, encoding="utf-8")
    print(request, end="")

    env = os.environ.copy()
    env["FDB5_CONFIG_FILE"] = str(args.config)
    env["DHSHOME"] = str(args.config.parents[2])

    completed = run(["fdb-read", f"--config={args.config}", str(request_path), str(args.out)], env=env)
    print(completed.stdout or "", end="")
    if completed.returncode != 0:
        print("fdb-read failed; fdbsvr/DHS retrieval may still need later integration work", file=sys.stderr)
        return completed.returncode

    if not args.out.is_file() or args.out.stat().st_size == 0:
        print(f"retrieval output {args.out} is empty", file=sys.stderr)
        return 1

    head = args.out.read_bytes()[:4]
    if head != b"GRIB":
        print(f"retrieval output {args.out} is non-empty but does not start with GRIB", file=sys.stderr)
        return 1

    print(f"retrieved non-empty GRIB: {args.out} ({args.out.stat().st_size} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
