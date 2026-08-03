# C++ library base images

The application images (frontend and the workers) depend on heavy C++ stacks
from ECMWF — eckit, eccodes, metkit, fdb, gribjump and the MARS client. Building
those from source takes tens of minutes and used to happen *inside* every app
image build.

These stacks are now built **once**, published to ECCR as standalone images, and
pulled by the app Dockerfiles via a `FROM ${..._LIBS_IMAGE}` line. Day-to-day app
builds compile only Rust and never touch a C++ compiler.

## The images

| Directory | Image | Contents | Consumed by |
|-----------|-------|----------|-------------|
| [`metkit/`](metkit/Dockerfile) | `eccr.ecmwf.int/polytope/cpp-metkit-libs` | eckit + metkit → `/opt/metkit` | `frontend/Dockerfile` |
| [`fdb/`](fdb/Dockerfile) | `eccr.ecmwf.int/polytope/cpp-fdb-libs` | eckit, eccodes, metkit, fdb → `/opt/fdb` | `workers/fdb-worker/Dockerfile` |
| [`fdb-gribjump/`](fdb-gribjump/Dockerfile) | `eccr.ecmwf.int/polytope/cpp-fdb-gribjump-libs` | eckit, libaec, eccodes, metkit, fdb, gribjump → `/opt/fdb` | `workers/polytope-fe-worker/Dockerfile` |
| [`mars/`](mars/Dockerfile) | `eccr.ecmwf.int/polytope/cpp-mars-libs` | MARS client bundle → `/opt/mars-client` | `workers/mars-worker/Dockerfile` |

`fdb` and `fdb-gribjump` are separate images on purpose: the fdb-worker and
fe-worker pin different upstream versions (e.g. fdb 5.19.2 vs 5.21.3), so they
cannot share one image.

## Stripped (default) vs `-debug` images

The library `.so`/`.a`/binaries are built `RelWithDebInfo` (ecbuild's default),
so they ship with DWARF debug symbols — the bulk of the image size. Each
Dockerfile therefore has two final targets:

| Target | Image | Symbols | Size (fdb-gribjump) |
|--------|-------|---------|---------------------|
| `libs` (default) | `cpp-<name>-libs` | stripped | **147 MB** |
| `libs-debug` | `cpp-<name>-libs-debug` | full | 419 MB |

`strip --strip-unneeded` on the shared libraries keeps the exported dynamic
symbols (so consumers still link and load them) while dropping ~260 MB of debug
info. Runtime behaviour is identical; you only lose symbol names in gdb
backtraces — which is exactly what the `-debug` companion is for. The `-debug`
image shares the same tag under a `-debug` repo suffix (e.g.
`cpp-fdb-gribjump-libs-debug:fdb5.21.3-gribjump0.12.0-r1`).

The consuming app Dockerfiles pull the stripped default; grab the `-debug`
image only when you need to symbolicate a crash in a C++ library.

## The `TAG` file

Each image directory has a `TAG` file containing a single line — the tag the
image is **published under** for releases and the tag the consuming Dockerfile
**pulls by default**. For example `fdb/TAG` holds `fdb5.19.2-r1`, which produces
`eccr.ecmwf.int/polytope/cpp-fdb-libs:fdb5.19.2-r1`.

Why a file and not just a hardcoded tag?

- It is the single source of truth the publish workflow reads, so the tag you
  publish and the tag you consume can never drift apart by a typo in two places.
- It makes the current version of each stack greppable and reviewable in one
  place.

**Tags are immutable.** The publish workflow refuses to overwrite an existing
tag. When you change what goes into an image you must give it a new tag.

Naming convention: describe the stack's headline versions, then a `-rN`
"revision" suffix that you bump for rebuilds that don't change library versions
(base-image refresh, build-flag tweak, etc.):

- `cpp-metkit-libs:metkit1.17.0-r1`
- `cpp-fdb-libs:fdb5.19.2-r1`
- `cpp-fdb-gribjump-libs:fdb5.21.3-gribjump0.12.0-r1`
- `cpp-mars-libs:7.1.9-r2`

The app Dockerfiles carry the current tag as the default of a build arg
(`FDB_LIBS_IMAGE`, `METKIT_LIBS_IMAGE`, `MARS_LIBS_IMAGE`), so a normal `docker
build` / `skaffold build` needs no extra flags.

## Bumping a library version

1. Edit the version `ARG`s in `docker/cpp-libs/<name>/Dockerfile`.
2. Put a new tag in `docker/cpp-libs/<name>/TAG` (bump the version part, reset
   `-r1`; or just bump `-rN` for a no-version-change rebuild).
3. Publish the new library image (see [Dev builds](#dev-builds-git-based-tags)
   or [Releasing](#releasing-canonical-tags)).
4. Update the matching `*_LIBS_IMAGE` default in the consuming app Dockerfile to
   the new tag.

Steps 3 and 4 are what make the new libraries actually take effect; until the
consumer's build arg points at the new tag, nothing changes.

## Tag policy

The skaffold config mirrors the root `skaffold.yaml` tag policy:

| Scenario | How to invoke | Resulting tag |
|----------|--------------|---------------|
| Dev build | omit `FIXED_TAG` | git commit hash (e.g. `cce9ed8`) |
| Dev build with prefix | `PREFIX=dev-` | `dev-cce9ed8` |
| Release | `FIXED_TAG=$(cat <img>/TAG)` | contents of `TAG` file |

`ignoreChanges: true` means a dirty working tree does not block the git tag
derivation (same behaviour as the app builds).

## Dev builds (git-based tags)

Use these when iterating on a library Dockerfile or pinning a new upstream
commit. The image is tagged with the current git commit hash, which lets you
reference it unambiguously when building the consuming app image.

Run from **this directory** (`docker/cpp-libs/`). Skaffold resolves each
artifact's `context:` (e.g. `mars`) relative to the current working directory,
not to the location of `skaffold.yaml`.

```bash
cd docker/cpp-libs

# Build and push one image; tag = git commit hash
GH_TOKEN="$GH_TOKEN" skaffold build --push \
  --filename skaffold.yaml \
  --build-image eccr.ecmwf.int/polytope/cpp-mars-libs

# Capture the tag skaffold assigned:
TAG=$(git rev-parse --short HEAD)   # same value skaffold used

# Build the consuming mars-worker against your dev library:
cd ../..
docker build -f workers/mars-worker/Dockerfile \
  --build-arg MARS_LIBS_IMAGE=eccr.ecmwf.int/polytope/cpp-mars-libs:$TAG \
  --build-arg GIT_AUTH_TOKEN="$GH_TOKEN" \
  -t mars-worker:dev .
```

> You can also use `PREFIX=dev-` to make dev images visually distinct from
> release images in the registry.

## Releasing (canonical tags)

Set `FIXED_TAG` to the contents of the relevant `TAG` file. Skaffold uses that
value as the image tag instead of the git commit.

```bash
cd docker/cpp-libs

# Publish the canonical versioned image (and its debug companion):
FIXED_TAG=$(cat fdb/TAG) skaffold build --push \
  --filename skaffold.yaml \
  --build-image eccr.ecmwf.int/polytope/cpp-fdb-libs \
  --build-image eccr.ecmwf.int/polytope/cpp-fdb-libs-debug

# mars additionally needs GH_TOKEN:
FIXED_TAG=$(cat mars/TAG) GH_TOKEN="$GH_TOKEN" skaffold build --push \
  --filename skaffold.yaml \
  --build-image eccr.ecmwf.int/polytope/cpp-mars-libs \
  --build-image eccr.ecmwf.int/polytope/cpp-mars-libs-debug
```

> Build one stack per invocation with `--build-image`. Without it, skaffold
> builds all eight artifacts (four stacks × stripped + debug) and applies the
> same `FIXED_TAG` to every image — almost never what you want.

### Via CI (preferred for releases)

The workflow [`.github/workflows/cpp-libs.yaml`](../../.github/workflows/cpp-libs.yaml)
runs skaffold with `FIXED_TAG` from the `TAG` file. PRs touching `docker/cpp-libs/`
only run a Dockerfile lint (`docker buildx build --check`) — the images are never
built in PR CI (hours of C++ compilation). **Publishing is manual dispatch only**:

```bash
gh workflow run cpp-libs.yaml -f image=fdb        # or: metkit | fdb-gribjump | mars | all
```
