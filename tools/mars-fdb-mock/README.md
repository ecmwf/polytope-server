# MARS FDB mock upstream build spike

This notes the upstream `ecmwf/mars-server-bundle` spike for building the runtime tools needed by the future `mars-fdb-mock` image: `fdbsvr`, `fdb-write`, and `marsadm`.

## Selected build host

Selected host: cleaned local WSL2 instance `DESKTOP-Q9D0LQ8`.

Reason: the bundle build needs a Linux filesystem with tens of GiB free; this WSL2 instance currently has enough free disk and avoids copying private sources to another machine.

Host snapshot recorded during the spike:

```text
$ hostname
DESKTOP-Q9D0LQ8

$ uname -a
Linux DESKTOP-Q9D0LQ8 6.6.87.2-microsoft-standard-WSL2 #1 SMP PREEMPT_DYNAMIC Thu Jun  5 18:30:46 UTC 2025 x86_64 x86_64 x86_64 GNU/Linux

$ nproc
32

$ free -h
Mem: 47Gi total, 32Gi available
Swap: 16Gi total, 15Gi available

$ df -h / /tmp /home/james/work/code/polytope-server
Filesystem      Size  Used Avail Use% Mounted on
/dev/sdd       1007G  606G  351G  64% /
/dev/sdd       1007G  606G  351G  64% /
/dev/sdd       1007G  606G  351G  64% /

$ df -BG / /tmp /home/james/work/code/polytope-server
Filesystem     1G-blocks  Used Available Use% Mounted on
/dev/sdd           1007G  606G      351G  64% /
/dev/sdd           1007G  606G      351G  64% /
/dev/sdd           1007G  606G      351G  64% /
```

## Repository access

Access to the required private repositories was confirmed with GitHub CLI and SSH without writing tokens to disk:

```text
$ gh repo view ecmwf/mars-server-bundle --json nameWithOwner,visibility,defaultBranchRef,viewerPermission
{"defaultBranch":"develop","permission":"ADMIN","repo":"ecmwf/mars-server-bundle","visibility":"INTERNAL"}

$ gh repo view ecmwf/dhskit --json nameWithOwner,visibility,defaultBranchRef,viewerPermission
{"defaultBranch":"develop","permission":"ADMIN","repo":"ecmwf/dhskit","visibility":"INTERNAL"}

$ gh repo view ecmwf/mars-server --json nameWithOwner,visibility,defaultBranchRef,viewerPermission
{"defaultBranch":"develop","permission":"ADMIN","repo":"ecmwf/mars-server","visibility":"INTERNAL"}

$ ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new -T git@github.com
Hi <redacted>! You've successfully authenticated, but GitHub does not provide shell access.
```

## Reproducible build transcript

Working directory used for the spike: `/tmp/mars-fdb-mock-spike`.

```text
$ mkdir -p /tmp/mars-fdb-mock-spike
$ cd /tmp/mars-fdb-mock-spike
$ time gh repo clone ecmwf/mars-server-bundle -- --recursive
# wall clock: 0m02s

$ gh repo clone ecmwf/ecbuild -- --depth 1 --branch develop
# wall clock: 0m02s

$ cd /tmp/mars-fdb-mock-spike/mars-server-bundle
$ cmake -S . -B build-minimal \
    -DLOCALCONFIG=$PWD/ecmwf-cds.cmake \
    -DENABLE_AEC=OFF \
    -DENABLE_DUMMY_TAPES=ON \
    -DENABLE_FDB5_SERVER=ON \
    -DENABLE_MPI=OFF \
    -DENABLE_FORTRAN=OFF \
    -DENABLE_PYTHON=OFF \
    -DENABLE_TESTS=OFF \
    -DENABLE_MARS2GRIB=OFF \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=/opt/mars-fdb-mock
# wall clock: 1m18s on first configure after component clones

$ cmake --build build-minimal --target fdbsvr fdb-write marsadm --parallel "$(nproc)"
# wall clock: 1m07s

$ cmake --build build-minimal --parallel "$(nproc)"
# wall clock: 0m21s after target build

$ cmake --install build-minimal --prefix /tmp/mars-fdb-mock-spike/install-minimal/opt/mars-fdb-mock
# wall clock: 0m01s
```

CMake cache flags from the successful build:

```text
CMAKE_BUILD_TYPE:STRING=Release
CMAKE_INSTALL_PREFIX:PATH=/opt/mars-fdb-mock
ENABLE_AEC:BOOL=OFF
ENABLE_DUMMY_TAPES:BOOL=ON
ENABLE_FDB5_SERVER:BOOL=ON
ENABLE_FORTRAN:BOOL=OFF
ENABLE_MARS2GRIB:BOOL=OFF
ENABLE_MPI:BOOL=OFF
ENABLE_PYTHON:BOOL=OFF
ENABLE_TESTS:BOOL=OFF
```

Measured disk usage:

```text
/tmp/mars-fdb-mock-spike/install-minimal: 90M
/tmp/mars-fdb-mock-spike/mars-server-bundle/build-minimal: 493M
/tmp/mars-fdb-mock-spike total after several failed/successful builds: 3.1G
```

Expected clean build size is roughly 1.5-2.0 GiB for sources plus one build tree and one install tree. Keep the 50 GiB host requirement because Docker layers, debug/retry builds, package caches, and future dependency changes can multiply that quickly.

## Component revisions used

The successful build used `mars-server-bundle` `develop` plus component `develop` heads resolved at configure time. Pin these exact SHAs in the Dockerfile rather than relying on moving branches:

```text
mars-server-bundle 2786a97f386ddd7db3b403d8ac4451eb8368e03e  7.29.18.0-28-g2786a97
ecbuild            158ae84807f2436248ab397f13c5ad43ec15c45d  3.14.2
libaec             7204505af7d6635734fc12a38d6bd0a6253c9c6d  v1.1.4
eckit              9daf0377f2fd0ddc2ee1bdd47c7064ea037a28c6  2.0.6-39-g9daf0377f
eccodes            cf28d1a6eb4ab14da16055a48268cef9096bf7dc  2.46.2-136-gcf28d1a6e
odc                424ce3dcd9d569d42f40e6acd28cdc6308352d6a  1.6.2-5-g424ce3dc
metkit             b280949ef1e60490a45bfc66b9c96bf3af2fd260  1.18.1-21-gb280949e
fdb5               54bfbb4d645f5a335bb2b732400ad976964100df  5.21.2-96-g54bfbb4d
dhskit             13c161d98ba9d66c8a9b026f2dd173d402ac4378  0.8.5-14-g13c161d
mars-server        bcb1e412fe5abee45d9c7042275c346d5932cb35  7.30.12-66-gbcb1e412
```

## Build findings and blockers

- Private `dhskit` and `mars-server` clone successfully over SSH from the bundle CMake configure.
- `-DENABLE_DUMMY_TAPES=ON` works and the configure summary reports `DUMMY_TAPES enabled`.
- `ecmwf-cds.cmake` is the right local config starting point because it skips `mars-hpss` and `mars-adsm`.
- `-DENABLE_FDB5_SERVER=ON` is required; otherwise the DHS FDB server support is not built.
- The default AEC path failed on this host before all components configured:
  `CRITICAL - libaec was found in the source tree but could not be loaded from .../build/libaec`.
  For this spike the successful build used `-DENABLE_AEC=OFF`. For the Dockerfile, prefer installing a system `libaec-dev` or validating that opendata GRIBs do not require CCSDS/AEC before accepting this runtime trade-off.
- Current `develop` with default `ENABLE_MARS2GRIB=ON` builds `libmetkit.so` with references to `eckit::geo::grid::ORCA`, then executables such as `fdbsvr` and `fdb-write` fail to link:
  `undefined reference to 'typeinfo for eckit::geo::grid::ORCA'` and `undefined reference to 'eckit::geo::grid::ORCA::arrangement[abi:cxx11]() const'`.
- Pinning `metkit` to tag `1.18.1` was tried and did not resolve the ORCA link failure. The successful runtime-scope build disables `MARS2GRIB`, which is not needed by `fdbsvr`/`fdb-write` for the mock happy path.
- Escalation note: if upstream owners require a full default bundle build with `MARS2GRIB=ON`, escalate the missing `eckit_geo` link propagation/API mismatch with the two ORCA symbols above rather than patching private sources locally.

## Runtime artifacts

Successful install prefix layout:

```text
/opt/mars-fdb-mock/bin
/opt/mars-fdb-mock/include
/opt/mars-fdb-mock/lib
/opt/mars-fdb-mock/lib/cmake
/opt/mars-fdb-mock/lib/pkgconfig
/opt/mars-fdb-mock/share
/opt/mars-fdb-mock/share/eckit
/opt/mars-fdb-mock/share/metkit
/opt/mars-fdb-mock/share/plugins
```

Key binaries installed:

```text
bin/fdbsvr     25432 bytes
bin/fdb-write  58784 bytes
bin/marsadm    86480 bytes
bin/fdb-list   61696 bytes
bin/fdb-read   61896 bytes
bin/fdb-server 18784 bytes
bin/mars       143088 bytes
bin/grib_set   164360 bytes
bin/grib_get   168792 bytes
```

Relevant installed shared libraries:

```text
libAdm.so
libCommon.so
libDhs.so
libFdbserver.so
libMars.so
libOdbsvr.so
libOs.so
libdhskit.so
libdhskit_admlib.so
libeccodes.so
libeccodes_memfs.so
libeckit.so
libeckit_cmd.so
libeckit_codec.so
libeckit_distributed.so
libeckit_geo.so
libeckit_geometry.so
libeckit_linalg.so
libeckit_maths.so
libeckit_mpi.so
libeckit_option.so
libeckit_spec.so
libeckit_sql.so
libeckit_web.so
libfdb5.so
libmars-hsm-plugin.so
libmars-httpsvr-plugin.so
libmetkit.so
libodccore.so
libodctest.so
libodctools.so
```

`ldd` on the key executables resolves against the install prefix plus base OS libraries. For a slim runtime image, copy the install prefix and ensure the base image provides at least `libstdc++.so.6`, `libgcc_s.so.1`, `libc.so.6`, `libm.so.6`, the dynamic loader, and `libtinfo.so.6` for `marsadm`.

Runtime notes:

- `fdbsvr --help` is not a help command; it starts a server and listens on the default port `7654`.
- `fdb-write` prints usage when invoked without GRIB paths.
- `marsadm` starts successfully and detects its home from the install prefix.
- The mock runtime will still need a `DHSHOME` layout and FDB config/schema under the container filesystem/PVC; this spike only identifies the compiled upstream artifacts.

## Multi-stage Docker image

The image source is in `tools/mars-fdb-mock/Dockerfile`. It builds the pinned `mars-server-bundle` revisions above in a builder stage, installs into `/opt/mars-fdb-mock`, and then copies only that install prefix plus the `ldd`-resolved runtime shared libraries into a fresh `debian:bookworm-slim` runtime stage.

Runtime contents provided by this task:

- `fdbsvr`, `fdb-write`, `fdb-list`, `fdb-read`, `marsadm`, `grib_get`, and `grib_set` from the upstream install prefix.
- Python 3 for the seed script. The local smoke script installs `seed/requirements.txt` inside its ephemeral seed container until the image is rebuilt with those Python dependencies.
- Default writable layout under `/dhshome` and `/data/fdb`.
- `scripts/entrypoint.sh`, which prepares the runtime directories and starts the requested command.

Build from the repository root:

```sh
cd /home/james/work/code/polytope-server
docker build \
  --build-arg GIT_AUTH_TOKEN="$(gh auth token)" \
  -t eccr.ecmwf.int/polytope/mars-fdb-mock:dev-1 \
  -f tools/mars-fdb-mock/Dockerfile \
  .
```

Notes:

- The token is consumed only by the builder stage. It is not declared in the runtime stage and is not converted to an environment variable.
- The builder writes `/root/.netrc` and the Git `insteadOf` rules only inside the single `RUN` command that clones/configures/builds the private bundle components; the same command removes `/root/.netrc` and `/root/.gitconfig` before the layer is committed.
- The runtime stage starts from `debian:bookworm-slim`, not from the builder stage.
- This image is intentionally not listed in `skaffold.yaml`; build and push it manually when needed.

### Runtime linker check

`fdbsvr --help` is not safe as a help command because upstream treats it like a server start in some builds. Use a command that loads the installed runtime without requiring a running FDB instead:

```sh
docker run --rm eccr.ecmwf.int/polytope/mars-fdb-mock:dev-1 fdb-write
```

`fdb-write` should print usage or an input-file error. Exit status may be non-zero, but it must not fail with `error while loading shared libraries`.

For a broader local probe, run:

```sh
cd /home/james/work/code/polytope-server/tools/mars-fdb-mock
./scripts/smoke.sh eccr.ecmwf.int/polytope/mars-fdb-mock:dev-1
```

This smoke script now performs the local seed, FDB list, direct FDB read where supported, runtime-start, token-history, and marker-idempotency checks described below.

### Post-build token verification

After every local build, inspect the image history and runtime filesystem before pushing:

```sh
docker history --no-trunc eccr.ecmwf.int/polytope/mars-fdb-mock:dev-1
docker run --rm eccr.ecmwf.int/polytope/mars-fdb-mock:dev-1 sh -c 'grep -r ghp_ /etc /opt /root /home 2>/dev/null; cat /root/.netrc /root/.gitconfig 2>/dev/null || true'
```

Confirm that the output contains no GitHub token material, no `.netrc` credentials, and no token-bearing Git config before pushing the image.

### Local build result for this image task

Built locally on the selected WSL2 host with:

```sh
cd /home/james/work/code/polytope-server
docker build \
  --build-arg GIT_AUTH_TOKEN="$(gh auth token)" \
  -t eccr.ecmwf.int/polytope/mars-fdb-mock:dev-1 \
  -f tools/mars-fdb-mock/Dockerfile \
  .
```

Result: succeeded, producing `eccr.ecmwf.int/polytope/mars-fdb-mock:dev-1`.

Build adjustments found during containerisation:

- The builder installs CMake 3.31.6 because the pinned `libaec` source requires CMake newer than Debian bookworm's packaged 3.25.1.
- The builder installs `libncurses-dev` so `eckit_cmd` is built; `marsadm` depends on it.
- The Docker build keeps `-DENABLE_AEC=OFF`, matching the successful spike, because the bundle still fails to configure with source-built AEC in this environment. Validate whether the selected opendata seed needs CCSDS/AEC before enabling AEC in a later change.

Runtime/linker validation passed with:

```sh
docker run --rm eccr.ecmwf.int/polytope/mars-fdb-mock:dev-1 fdb-write
```

Expected output is `fdb-write` usage with exit status 1; this proves the binary and its shared libraries load.

The local image smoke also passed:

```sh
cd /home/james/work/code/polytope-server
tools/mars-fdb-mock/scripts/smoke.sh eccr.ecmwf.int/polytope/mars-fdb-mock:dev-1
```

Security verification completed for the built tag. The required checks produced no token material:

```sh
docker history --no-trunc eccr.ecmwf.int/polytope/mars-fdb-mock:dev-1
docker run --rm eccr.ecmwf.int/polytope/mars-fdb-mock:dev-1 sh -c 'grep -r ghp_ /etc /opt /root /home 2>/dev/null; cat /root/.netrc /root/.gitconfig 2>/dev/null || true'
```

## Runtime FDB/MARS config

The runtime configuration under `tools/mars-fdb-mock/etc/` mirrors the upstream `mars-server/dev/fdbdev` layout for `DHSHOME=/tmp/dhshome`:

- `etc/fdb/config.yaml` uses a local TOC FDB rooted at `/tmp/dhshome/data/fdb/root`.
- `etc/fdb/schema` indexes the narrow smoke dataset.
- `etc/config/mars` enables the FDB-backed server path and keeps the small upstream development limits.
- `etc/disks/fdb` and `etc/disks/df` point MARS disk accounting at the FDB root with a 95% threshold.
- `etc/marsPermissions`, `etc/marsLimits`, and `etc/marsPriorities` are present as empty development defaults, matching upstream `fdbdev`.

Current schema shape:

```text
[ class=od, expver=9999, stream=oper, date, time, domain
    [ type=fc, levtype=sfc
        [ step, param ]]]
```

`domain` is included because a local ECMWF GRIB ingestion probe exposes `domain=g`. `model` is intentionally omitted until real opendata seed ingestion shows it in the indexed metadata.

The local probe below validates the config with an ecCodes sample GRIB, normalised to the collision-avoidance selector. The normal opendata seed flow is implemented by `scripts/seed.py` and is the preferred validation path.

```sh
tmp=$(mktemp -d)
cleanup() {
  docker run --rm -u 0 -v "$tmp:/cleanup" debian:bookworm-slim \
    sh -c 'chmod -R 0777 /cleanup' >/dev/null 2>&1 || true
  rm -rf "$tmp"
}
trap cleanup EXIT

mkdir -p "$tmp/dhshome/data/fdb/root" "$tmp/dhshome/tmp" "$tmp/dhshome/log"
cp -a tools/mars-fdb-mock/etc "$tmp/dhshome/"

grib_set \
  -s centre=ecmf,dataDate=20260519,dataTime=0,stepRange=0,paramId=167,class=od,expver=9999,stream=oper,type=fc,levtype=sfc \
  /usr/share/eccodes/samples/regular_ll_sfc_grib1.tmpl \
  "$tmp/probe.grib"

grib_get -p class,expver,stream,type,levtype,date,time,step,param,domain "$tmp/probe.grib"
chmod -R a+rwX "$tmp/dhshome" "$tmp/probe.grib"

docker run --rm \
  -v "$tmp/dhshome:/tmp/dhshome" \
  -v "$tmp/probe.grib:/tmp/probe.grib:ro" \
  -e DHSHOME=/tmp/dhshome \
  -e FDB5_CONFIG_FILE=/tmp/dhshome/etc/fdb/config.yaml \
  eccr.ecmwf.int/polytope/mars-fdb-mock:dev-1 \
  fdb-write --config=/tmp/dhshome/etc/fdb/config.yaml /tmp/probe.grib

docker run --rm \
  -v "$tmp/dhshome:/tmp/dhshome" \
  -e DHSHOME=/tmp/dhshome \
  -e FDB5_CONFIG_FILE=/tmp/dhshome/etc/fdb/config.yaml \
  eccr.ecmwf.int/polytope/mars-fdb-mock:dev-1 \
  fdb-list --config=/tmp/dhshome/etc/fdb/config.yaml --porcelain \
  class=od,expver=9999,stream=oper,date=20260519,time=0000,domain=g,type=fc,levtype=sfc,step=0,param=167
```

Expected key inspection output:

```text
od 9999 oper fc sfc 20260519 0 0 167.128 g
{class=od,expver=9999,stream=oper,date=20260519,time=0000,domain=g}{type=fc,levtype=sfc}{step=0,param=167}
```

## Opendata seed flow

`scripts/seed.py` seeds a tiny surface forecast smoke set into the local FDB:

- downloads one `ecmwf-opendata` field (`stream=oper,type=fc,levtype=sfc,step=0,param=2t`, indexed by FDB as `param=167`) to `/tmp/seed`;
- normalises the GRIB metadata to the collision-avoidance selector with `grib_set`, especially `class=od,expver=9999,stream=oper,type=fc,levtype=sfc`;
- ingests the normalised GRIB with `fdb-write --config=/tmp/dhshome/etc/fdb/config.yaml`;
- verifies the field with `fdb-list --porcelain` using the exact date/time/domain/step/param observed from the GRIB;
- writes `/data/fdb/.seeded` only after successful ingest and verification.

Idempotency controls:

- default marker: `/data/fdb/.seeded`;
- force CLI flag: `--force`;
- force environment variable: `MARS_FDB_MOCK_SEED_FORCE=1`;
- marker override: `MARS_FDB_MOCK_SEED_MARKER=/path/to/.seeded`;
- seed work directory override: `MARS_FDB_MOCK_SEED_DIR=/tmp/seed`;
- runtime config source override: `MARS_FDB_MOCK_ETC_DIR=/path/to/etc`;
- prefer an existing cached fallback before attempting a live download: `MARS_FDB_MOCK_PREFER_CACHE=1`.

If the public opendata request is rate-limited or unavailable, the script falls back deterministically to a cached GRIB under `/data/fdb/seed-cache`. If no cached GRIB exists, it tries a local ecCodes sample (`regular_ll_sfc_grib1.tmpl`) when present. The default intended path remains the live `ecmwf-opendata` download; the fallback exists only to keep repeated local smoke runs deterministic after one successful download or on images that provide ecCodes samples.

## Local seed and smoke test

Run the full local smoke from `tools/mars-fdb-mock`:

```sh
./scripts/smoke.sh eccr.ecmwf.int/polytope/mars-fdb-mock:dev-1
```

The smoke script uses a persistent bind-mounted data directory, defaulting to `tools/mars-fdb-mock/.smoke-data`. Override it with:

```sh
MARS_FDB_MOCK_SMOKE_DATA_DIR=/tmp/mars-fdb-mock-smoke ./scripts/smoke.sh eccr.ecmwf.int/polytope/mars-fdb-mock:dev-1
```

Checks performed:

- runtime linker checks for `fdb-write` and `fdbsvr`;
- installs `seed/requirements.txt` inside the ephemeral seed container if `ecmwf-opendata` is not already available;
- runs `scripts/seed.py` against persistent `/data/fdb` and `/tmp/dhshome` mounts;
- confirms `/data/fdb/.seeded` exists and contains the exact selector;
- runs `fdb-list --porcelain` for that selector and requires `expver=9999` in the indexed keys;
- runs `tests/retrieve_seeded_field.py`, which uses `fdb-read` to retrieve one field and validates non-empty GRIB bytes when direct local FDB retrieval is supported;
- starts `fdbsvr` long enough to prove the runtime process starts against the seeded config;
- runs the seed script a second time and requires the marker-based `skipping download and ingest` message;
- repeats the runtime token-history and filesystem token checks.

The expected `fdb-list` key shape after a successful opendata seed is:

```text
{class=od,expver=9999,stream=oper,date=<YYYYMMDD>,time=<HHMM>,domain=g}{type=fc,levtype=sfc}{step=0,param=167}
```

`fdb-list` may display the first summary line with the local parameter table suffix, for example `167.128`, while the request selector remains `param=167`.

## Bologna-dev deployment and live verification

Target used for the first live deployment attempt:

- release: `polytope-dev`
- namespace: `dps-dev`
- helmfile environment: `bologna-dev`
- developer values: `DEV_NAME=majh`
- image: `eccr.ecmwf.int/polytope/mars-fdb-mock:dev-1`
- frontend URL expected from the developer values: `https://polytope-majh.ecmwf.int`

### Push

```sh
cd /home/james/work/code/polytope-server
if docker manifest inspect eccr.ecmwf.int/polytope/mars-fdb-mock:dev-1 >/dev/null 2>&1; then
  echo "remote image manifest exists"
else
  docker push eccr.ecmwf.int/polytope/mars-fdb-mock:dev-1
fi
```

Observed on 2026-05-19: the local Podman-backed `docker manifest inspect` did not return success for this OCI single-image manifest, so `docker push eccr.ecmwf.int/polytope/mars-fdb-mock:dev-1` was run and completed successfully with manifest written.

### Deploy

Use the existing bologna-dev kube context and the `majh` dev values:

```sh
kubectl config current-context
# expected: bologna-dps-dev

cd /home/james/work/code/polytope-config
DEV_NAME=majh helmfile -e bologna-dev sync
```

Observed on 2026-05-19: `kubectl config current-context` returned `bologna-dps-dev`, but the helmfile sync was blocked by the Helm release record exceeding the Kubernetes Secret size limit:

```text
Error: UPGRADE FAILED: create: failed to create: Secret "sh.helm.release.v1.polytope-dev.v3" is invalid: data: Too long: may not be more than 1048576 bytes
```

The existing deployed release remained at revision 2:

```sh
helm -n dps-dev status polytope-dev --show-desc
kubectl -n dps-dev get secrets -l owner=helm,name=polytope-dev \
  -o custom-columns=NAME:.metadata.name,STATUS:.metadata.labels.status,VERSION:.metadata.labels.version \
  --sort-by=.metadata.labels.version
```

Observed release state:

```text
NAME: polytope-dev
NAMESPACE: dps-dev
STATUS: deployed
REVISION: 2
DESCRIPTION: Upgrade complete

sh.helm.release.v1.polytope-dev.v1   superseded   1
sh.helm.release.v1.polytope-dev.v2   deployed     2
```

Because the upgrade failed before revision 3 was stored, the `mars-fdb-mock` objects were not present.

### Kubernetes object and seed-log checks

After a successful deploy, these commands should show the StatefulSet pod, ClusterIP Service on port 9000, and bound PVC, then either a first seed or marker skip in the init container logs:

```sh
kubectl -n dps-dev get pods,svc,pvc | grep mars-fdb-mock
kubectl -n dps-dev get statefulset polytope-dev-mars-fdb-mock -o wide
kubectl -n dps-dev logs statefulset/polytope-dev-mars-fdb-mock -c seed

# Optional marker inspection once the pod exists:
kubectl -n dps-dev exec statefulset/polytope-dev-mars-fdb-mock -c fdbsvr -- \
  sh -c 'ls -l /data/fdb/.seeded && cat /data/fdb/.seeded'
```

Observed on 2026-05-19 after the blocked helmfile sync:

```text
kubectl -n dps-dev get pods,svc,pvc | grep mars-fdb-mock
# no output

kubectl -n dps-dev logs statefulset/polytope-dev-mars-fdb-mock -c seed
# error: error from server (NotFound): statefulsets.apps "polytope-dev-mars-fdb-mock" not found in namespace "dps-dev"
```

### Live request payload

The seeded smoke request routed to the mock server is intentionally narrow:

```json
{
  "class": "od",
  "stream": "oper",
  "type": "fc",
  "date": "-1",
  "time": "0000",
  "step": "0",
  "levtype": "sfc",
  "expver": "9999",
  "domain": "g",
  "param": "167"
}
```

For a full retrieval after the deployment blocker is cleared, use the exact seeded date/time written in `/data/fdb/.seeded` if it differs from the relative `date=-1` route sample. Expected result: the Polytope request completes successfully and downloads a non-empty GRIB payload for `class=od,expver=9999,stream=oper,type=fc,levtype=sfc,domain=g,step=0,param=167`; the file should start with the bytes `GRIB`.

Route-only verification, which validates dispatch to the `mars-fdb-mock` branch without requiring full data retrieval, can be run with:

```sh
cd /home/james/work/code/polytope-config
pytest -e bologna-dev --verify -k mars-fdb-mock --kube-proxy socks5://127.0.0.1:1080 -vv
```

Full SDK retrieval should use the same payload against `https://polytope-majh.ecmwf.int` with the normal bologna-dev test credentials and, when testing the route sample path, the mock roles header `Polytope-Mock-Roles: ecmwf:/MARS/valid_forecast`.

Observed on 2026-05-19: live retrieval and route verification were not run to completion because the helmfile sync did not create the mock StatefulSet, Service, PVC, worker pool, or route changes. A pytest-based verification attempt in this local environment would also need the `polytope` Python SDK installed; the current interpreter reports `ModuleNotFoundError: No module named 'polytope'` when test fixtures import `polytope.api.Client`.

### Troubleshooting commands

```sh
# Confirm image availability from the workstation. With this Podman-backed docker
# CLI, `docker manifest inspect` may fail on OCI single-image manifests; a pull is
# the most direct check.
docker pull eccr.ecmwf.int/polytope/mars-fdb-mock:dev-1

# Render the intended bologna-dev/majh manifest without touching the cluster.
cd /home/james/work/code/polytope-chart
helm template polytope-dev . \
  -f ../polytope-config/common.yaml \
  -f ../polytope-config/auxiliary/schedule.yaml \
  -f ../polytope-config/location/bologna/config.yaml \
  -f ../polytope-config/environment/dev.yaml \
  -f ../polytope-config/workers/test.yaml \
  -f ../polytope-config/dev/majh.yaml \
  | grep -n "mars-fdb-mock" -C 3

# Inspect Helm release state and failed revision symptoms.
helm -n dps-dev status polytope-dev --show-desc
helm -n dps-dev history polytope-dev
kubectl -n dps-dev get secrets -l owner=helm,name=polytope-dev

# Inspect mock workload once the release storage-size blocker is fixed.
kubectl -n dps-dev describe statefulset polytope-dev-mars-fdb-mock
kubectl -n dps-dev describe pod -l app.kubernetes.io/name=mars-fdb-mock
kubectl -n dps-dev logs statefulset/polytope-dev-mars-fdb-mock -c seed
kubectl -n dps-dev logs statefulset/polytope-dev-mars-fdb-mock -c fdbsvr
kubectl -n dps-dev get endpoints polytope-dev-mars-fdb-mock -o wide

# Inspect worker-side routing/connectivity once deployed.
kubectl -n dps-dev get pods | grep mars-fdb-mock
kubectl -n dps-dev logs deploy/polytope-dev-mars-fdb-mock --tail=200
kubectl -n dps-dev get configmap polytope-dev-broker-config -o yaml | grep -n "fdb-mock" -C 5
kubectl -n dps-dev get configmap polytope-dev-mars-fdb-mock-config -o yaml
```
