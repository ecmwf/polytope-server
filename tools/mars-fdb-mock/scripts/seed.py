#!/usr/bin/env python3
"""Seed the mars-fdb-mock FDB with a tiny normalised opendata smoke set."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Iterable


DEFAULT_MARKER = Path("/data/fdb/.seeded")
DEFAULT_SEED_DIR = Path("/tmp/seed")
DEFAULT_DHSHOME = Path("/tmp/dhshome")
DEFAULT_DATA_DIR = Path("/data/fdb")
REQUEST_KEYS = ("class", "expver", "stream", "date", "time", "domain", "type", "levtype", "step", "param")


class SeedError(RuntimeError):
    pass


def log(message: str) -> None:
    print(f"mars-fdb-mock seed: {message}", flush=True)


def run(command: list[str], *, check: bool = True, capture: bool = False, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    log("+ " + " ".join(command))
    return subprocess.run(
        command,
        check=check,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.STDOUT if capture else None,
        env=env,
    )


def truthy(value: str | None) -> bool:
    return value is not None and value.lower() in {"1", "true", "yes", "y", "on", "force"}


def copy_tree_contents(source: Path, destination: Path) -> None:
    if not source.is_dir():
        raise SeedError(f"configuration source {source} does not exist")
    destination.mkdir(parents=True, exist_ok=True)
    for child in source.iterdir():
        target = destination / child.name
        if child.is_dir():
            if target.exists():
                shutil.rmtree(target)
            shutil.copytree(child, target)
        else:
            shutil.copy2(child, target)


def prepare_dhshome(dhshome: Path, data_dir: Path, etc_dir: Path) -> Path:
    dhshome.mkdir(parents=True, exist_ok=True)
    (dhshome / "tmp").mkdir(parents=True, exist_ok=True)
    (dhshome / "log").mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)
    root = dhshome / "data" / "fdb" / "root"
    root.mkdir(parents=True, exist_ok=True)

    if not (dhshome / "etc" / "fdb" / "config.yaml").is_file() or not (dhshome / "etc" / "fdb" / "schema").is_file():
        log(f"installing runtime configuration from {etc_dir} to {dhshome / 'etc'}")
        copy_tree_contents(etc_dir, dhshome / "etc")

    config = dhshome / "etc" / "fdb" / "config.yaml"
    if not config.is_file():
        raise SeedError(f"FDB config was not installed at {config}")
    return config


def newest_cached_grib(cache_dir: Path) -> Path | None:
    candidates = sorted(cache_dir.glob("*.grib"), key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def sample_candidates() -> Iterable[Path]:
    env = os.environ.get("MARS_FDB_MOCK_ECCODES_SAMPLE")
    if env:
        yield Path(env)
    for base in (
        Path("/usr/share/eccodes/samples"),
        Path("/opt/mars-fdb-mock/share/eccodes/samples"),
        Path("/usr/local/share/eccodes/samples"),
    ):
        yield base / "regular_ll_sfc_grib1.tmpl"


def build_sample_fallback(target: Path) -> bool:
    for sample in sample_candidates():
        if sample.is_file():
            log(f"using ecCodes sample fallback {sample}")
            run([
                "grib_set",
                "-s",
                "centre=ecmf,dataDate=20260519,dataTime=0,stepRange=0,paramId=167,class=od,expver=9999,stream=oper,type=fc,levtype=sfc",
                str(sample),
                str(target),
            ])
            return True
    return False


def download_opendata(target: Path, cache_dir: Path) -> bool:
    try:
        from ecmwf.opendata import Client
    except Exception as exc:  # pragma: no cover - depends on container packaging
        log(f"ecmwf-opendata import failed: {exc}")
        return False

    request = {
        "stream": "oper",
        "type": "fc",
        "levtype": "sfc",
        "step": [0],
        "param": ["2t"],
    }
    log(f"downloading tiny ecmwf-opendata request {request} to {target}")
    try:
        Client(source="ecmwf").retrieve(request, target=str(target))
    except Exception as exc:
        log(f"ecmwf-opendata download failed: {exc}")
        return False

    if not target.is_file() or target.stat().st_size == 0:
        log("ecmwf-opendata returned no GRIB bytes")
        return False

    cache_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(target, cache_dir / "opendata-smoke.grib")
    log(f"downloaded {target.stat().st_size} bytes and refreshed {cache_dir / 'opendata-smoke.grib'}")
    return True


def make_seed_grib(seed_dir: Path, data_dir: Path) -> Path:
    seed_dir.mkdir(parents=True, exist_ok=True)
    source = seed_dir / "opendata.raw.grib"
    cache_dir = data_dir / "seed-cache"

    cached = newest_cached_grib(cache_dir)
    if truthy(os.environ.get("MARS_FDB_MOCK_PREFER_CACHE")) and cached:
        log(f"using preferred cached GRIB {cached}")
        shutil.copy2(cached, source)
    elif not download_opendata(source, cache_dir):
        cached = newest_cached_grib(cache_dir)
        if cached:
            log(f"using cached GRIB fallback {cached}")
            shutil.copy2(cached, source)
        elif build_sample_fallback(source):
            cache_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, cache_dir / "eccodes-sample-smoke.grib")
        else:
            raise SeedError(
                "could not download opendata and no deterministic fallback GRIB is available; "
                "retry later or provide a cached GRIB under /data/fdb/seed-cache"
            )

    normalised = seed_dir / "opendata.normalised.grib"
    # levtype is derived from GRIB product definition and is read-only for the
    # downloaded field; the opendata request already constrains it to sfc.
    run([
        "grib_set",
        "-s",
        "class=od,expver=9999,stream=oper,type=fc",
        str(source),
        str(normalised),
    ])
    if normalised.stat().st_size == 0:
        raise SeedError("normalised GRIB is empty")
    return normalised


def grib_get(grib: Path, keys: Iterable[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for key in keys:
        completed = run(["grib_get", "-p", key, str(grib)], check=False, capture=True)
        value = (completed.stdout or "").strip().splitlines()[-1:] or [""]
        if completed.returncode == 0 and value[0] and value[0] != "MISSING":
            result[key] = value[0]
    return result


def fdb_selector(metadata: dict[str, str]) -> str:
    date = metadata.get("date") or metadata.get("dataDate")
    time = metadata.get("time") or metadata.get("dataTime") or "0"
    step = metadata.get("step") or metadata.get("endStep") or metadata.get("stepRange") or "0"
    param = metadata.get("param") or metadata.get("paramId") or "167"
    domain = metadata.get("domain") or "g"
    if date is None:
        raise SeedError(f"could not determine date from GRIB metadata: {metadata}")
    return ",".join(
        [
            "class=od",
            "expver=9999",
            "stream=oper",
            f"date={date}",
            f"time={int(time):04d}",
            f"domain={domain}",
            "type=fc",
            "levtype=sfc",
            f"step={step}",
            f"param={param}",
        ]
    )


def ingest_and_verify(grib: Path, config: Path, marker: Path, force: bool) -> str:
    env = os.environ.copy()
    env["FDB5_CONFIG_FILE"] = str(config)
    env["DHSHOME"] = str(config.parents[2])

    run(["fdb-write", f"--config={config}", str(grib)], env=env)

    metadata = grib_get(grib, (*REQUEST_KEYS, "dataDate", "dataTime", "endStep", "paramId"))
    selector = fdb_selector(metadata)
    completed = run(["fdb-list", f"--config={config}", "--porcelain", selector], capture=True, env=env)
    output = completed.stdout or ""
    print(output, end="")
    if "expver=9999" not in output or "class=od" not in output:
        raise SeedError(f"fdb-list did not confirm the normalised selector {selector}")

    marker.parent.mkdir(parents=True, exist_ok=True)
    tmp_marker = marker.with_suffix(".seeded.tmp")
    tmp_marker.write_text(
        "seeded=true\n"
        f"forced={str(force).lower()}\n"
        f"grib={grib}\n"
        f"selector={selector}\n",
        encoding="utf-8",
    )
    tmp_marker.replace(marker)
    log(f"wrote marker {marker}")
    return selector


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true", help="reseed even when the marker exists")
    parser.add_argument("--marker", type=Path, default=Path(os.environ.get("MARS_FDB_MOCK_SEED_MARKER", DEFAULT_MARKER)))
    parser.add_argument("--seed-dir", type=Path, default=Path(os.environ.get("MARS_FDB_MOCK_SEED_DIR", DEFAULT_SEED_DIR)))
    parser.add_argument("--dhshome", type=Path, default=Path(os.environ.get("DHSHOME", DEFAULT_DHSHOME)))
    parser.add_argument("--data-dir", type=Path, default=Path(os.environ.get("FDB_HOME", DEFAULT_DATA_DIR)))
    parser.add_argument("--etc-dir", type=Path, default=Path(os.environ.get("MARS_FDB_MOCK_ETC_DIR", "/opt/mars-fdb-mock/etc")))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    force = args.force or truthy(os.environ.get("MARS_FDB_MOCK_SEED_FORCE"))
    if args.marker.exists() and not force:
        log(f"marker {args.marker} exists; skipping download and ingest")
        print(args.marker.read_text(encoding="utf-8"), end="")
        return 0

    try:
        config = prepare_dhshome(args.dhshome, args.data_dir, args.etc_dir)
        grib = make_seed_grib(args.seed_dir, args.data_dir)
        selector = ingest_and_verify(grib, config, args.marker, force)
        log(f"verified seeded selector {selector}")
        return 0
    except SeedError as exc:
        log(f"ERROR: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
