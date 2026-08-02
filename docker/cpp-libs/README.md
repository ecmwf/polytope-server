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
image is **published under** and the tag the consuming Dockerfile **pulls**. For
example `fdb/TAG` holds `fdb5.19.2-r1`, which produces
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
- `cpp-mars-libs:7.1.9-r1`

The app Dockerfiles carry the current tag as the default of a build arg
(`FDB_LIBS_IMAGE`, `METKIT_LIBS_IMAGE`, `MARS_LIBS_IMAGE`), so a normal `docker
build` / `skaffold build` needs no extra flags.

## Bumping a library version

1. Edit the version `ARG`s in `docker/cpp-libs/<name>/Dockerfile`.
2. Put a new tag in `docker/cpp-libs/<name>/TAG` (bump the version part, reset
   `-r1`; or just bump `-rN` for a no-version-change rebuild).
3. Publish the image (see below).
4. Update the matching `*_LIBS_IMAGE` default in the consuming Dockerfile to the
   new tag.

Steps 3 and 4 are what make the new libraries actually take effect; until the
consumer's build arg points at the new tag, nothing changes.

## Publishing

These images are built with **skaffold**, the same tool used for the application
images. Their skaffold config is [`skaffold.yaml`](skaffold.yaml) in this
directory (separate from the app config at the repo root, so app builds never
rebuild the libraries).

The workflow [`.github/workflows/cpp-libs.yaml`](../../.github/workflows/cpp-libs.yaml)
runs skaffold to build and push. PRs touching `docker/cpp-libs/` only run a
Dockerfile lint (`docker buildx build --check`) — the images are never built in
PR CI (hours of C++ compilation). **Publishing is manual dispatch only**:

```bash
gh workflow run cpp-libs.yaml -f image=all      # or: metkit | fdb | fdb-gribjump | mars
```

### Building/pushing by hand

Requires `docker login eccr.ecmwf.int`. Build one image at a time, passing its
tag via `FIXED_TAG` (skaffold applies it as the image tag). Image names are the
`--build-image` targets.

Run these **from this directory** (`docker/cpp-libs/`). Skaffold resolves each
artifact's `context:` (e.g. `metkit`) relative to the current working directory,
not to the location of `skaffold.yaml`, so invoking it from the repo root fails
with `context "metkit" does not exist`.

```bash
cd docker/cpp-libs

FIXED_TAG=$(cat fdb/TAG) \
  skaffold build --push \
    --filename skaffold.yaml \
    --build-image eccr.ecmwf.int/polytope/cpp-fdb-libs

# mars needs a GitHub token for private clones (source build); set RPM_REPO too
# if you build the RPM variant (MARS_BUILD_FROM_SOURCE=false, set in the Dockerfile):
FIXED_TAG=$(cat mars/TAG) GH_TOKEN="$GH_TOKEN" \
  skaffold build --push \
    --filename skaffold.yaml \
    --build-image eccr.ecmwf.int/polytope/cpp-mars-libs
```

> Build one image per invocation with `--build-image`. `skaffold build` without
> it builds all four and applies the single `FIXED_TAG` to every image, which is
> almost never what you want.

## Local development against a library image

To test app changes against a locally built (unpushed) library image, build the
library with skaffold (or plain `docker build`) and point the consuming build arg
at your local tag:

```bash
# build the library image from docker/cpp-libs/ (see note above on context resolution)
( cd docker/cpp-libs && FIXED_TAG=dev skaffold build --push=false \
    --filename skaffold.yaml \
    --build-image eccr.ecmwf.int/polytope/cpp-fdb-libs )

# then build the consuming app image from the repo root
docker build -f workers/fdb-worker/Dockerfile \
  --build-arg FDB_LIBS_IMAGE=eccr.ecmwf.int/polytope/cpp-fdb-libs:dev \
  --build-arg GIT_AUTH_TOKEN="$GH_TOKEN" -t fdb-worker:dev .
```
