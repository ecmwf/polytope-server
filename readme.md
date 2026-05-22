# polytope-server

[![Static Badge](https://github.com/ecmwf/codex/raw/refs/heads/main/Project%20Maturity/incubating_badge.svg)](https://github.com/ecmwf/codex/raw/refs/heads/main/Project%20Maturity)


> \[!IMPORTANT\]
> This software is **Incubating** and subject to ECMWF's guidelines on [Software Maturity](https://github.com/ecmwf/codex/raw/refs/heads/main/Project%20Maturity).

<!-- :warning: This project is BETA and will be experimental for the forseable future. Interfaces and functionality are likely to change, and the project itself may be scrapped. DO NOT use this software in any project/software that is operational. -->

Polytope provides a REST API for access to hypercube data, stored in various data sources (FDBs, MARS, etc.). Polytope is comprised of a number of microservices:

* **frontend**: REST API running Falcon/Flask
* **worker**: responsible for fetching data and pushing it to a staging area for download
* **broker**: responsible for scheduling pending requests
* **garbage-collector**: responsible for cleaning up old requests and freeing up space in the staging area

Through common abstraction layers, these components speak to various other services:

* **request_store**: database to track requests (MongoDB)
* **queue**: queue to dispatch requests to workers (RabbitMQ)
* **staging**: object-store to serve and receive data (S3, BasicHTTPServer)
* **authentication**: for authenticating users to Polytope (ECMWF API, Basic)
* **authorization**: for managing authorization to different collections (ECMWF LDAP)
* **caching**: caching of web requests to various services (MongoDB, Redis, Memcached)

## Build and deploy

Build images from this repository with Skaffold. Site-specific and
secret-bearing values live in `skaffold.env`; GribJump/FDB source-build
dependency pins live in `docker/gribjump/deps.env` so the same bundle can be
built inside or outside Docker.

```bash
# Create skaffold.env with SKAFFOLD_DEFAULT_REPO, rpm_repo,
# mars_config_repo, and mars_config_branch.
PREFIX=dev_ skaffold build
```

With the default settings, the worker expects the helper images from
`docker/mars-client/` and `docker/gribjump/` to exist already. Build those
first, or override the worker modes to `off`/`rpm` as needed before running the
main `skaffold build`.

The worker now wires its optional dependencies with explicit per-component mode
variables:

- `worker_mars_c_mode=off|rpm|image`
- `worker_mars_c_image=<image-ref>`
- `worker_mars_cpp_mode=off|rpm|image`
- `worker_mars_cpp_image=<image-ref>`
- `worker_gribjump_mode=off|image`
- `worker_gribjump_image=<image-ref>`

All three components default to `image` mode. In that default mode, the worker
will derive helper image references automatically from `SKAFFOLD_DEFAULT_REPO`
(when set) plus the same tag patterns used by the dedicated helper Skaffold
configs. If `SKAFFOLD_DEFAULT_REPO` is unset, the worker falls back to local
image names with the same computed tags.

Helper tags are intentionally low-friction now: they depend only on the
selected helper ref, branch, or version inputs, not on the surrounding
`polytope-server` git state. The default tag formats are:

- MARS C: `PREFIX + mc-<sanitized mars_client_c_bundle_ref>`
- MARS C++: `PREFIX + mcpp-<sanitized mars_client_cpp_bundle_ref>`
- GribJump/FDB: `PREFIX + gj-<sanitized gribjump_version>`

If you build helpers with non-default refs or versions, pass the same
variables into the main worker build so its derived `worker_*_image` values
still match. Otherwise, set `worker_mars_c_image`, `worker_mars_cpp_image`,
and/or `worker_gribjump_image` explicitly.

The local rpm-backed MARS path is still available through `worker_mars_c_mode`
and `worker_mars_cpp_mode`. Those stages install `mars-client=6.34.4.11` and
`mars-client-cpp=7.1.9.1` by default, while still allowing the versions to be
overridden through Skaffold build args. GribJump does not advertise an rpm
mode; invalid mode values fail the worker build clearly.

To build publishable source-built replacement base images for the C and C++
MARS clients, use the dedicated config under `docker/mars-client/`:

```bash
PREFIX=dev_ docker/mars-client/skaffold.sh build -p mars-c

PREFIX=dev_ docker/mars-client/skaffold.sh build -p mars-cpp
```

These images source-build the C and C++ MARS clients from
`ecmwf/mars-client-bundle` into the same locations used by the worker build:
`mars-base-c` provides `/opt/ecmwf/mars-client` and the `mars` binary, while
`mars-base-cpp` provides `/opt/ecmwf/mars-client-cpp`.
The source build uses a BuildKit secret backed by `GITHUB_TOKEN` so private
GitHub dependencies can be fetched without baking the token into the final
image. `docker/mars-client/skaffold.sh` runs from the repository root and will
populate `GITHUB_TOKEN` from `gh auth token` when needed.

The dedicated MARS helper profiles tag their outputs from the selected bundle
refs only, so the default worker image-mode lookup will match as long as you
reuse the same `PREFIX` and `mars_client_*_bundle_ref` values.

To build the publishable source-built GribJump/FDB image that the worker can
copy from, use the dedicated bundle config under `docker/gribjump/`:

```bash
PREFIX=dev_ docker/gribjump/skaffold.sh build
```

This image builds the source bundle into `/opt/polytope/gribjump-source` and
the worker-installable wheels into `/opt/polytope/gribjump-source-wheels`.
`docker/gribjump/skaffold.sh` is GribJump-only: it always runs
`skaffold -f docker/gribjump/skaffold.yaml` from the repository root. Because
`docker/gribjump/skaffold.yaml` sets `context: .`, the Docker build context is
the whole `polytope-server/` tree.

The dedicated GribJump helper tag is version-only: `PREFIX + gj-<gribjump>`.
The ecbuild, libaec, eckit, eccodes, metkit, and FDB input pins are recorded
on the published image as labels and remain part of the image digest
provenance, so they can still be inspected without expanding the tag.

For a default image-mode worker build, the usual order is:

```bash
PREFIX=dev_ docker/mars-client/skaffold.sh build -p mars-c
PREFIX=dev_ docker/mars-client/skaffold.sh build -p mars-cpp
PREFIX=dev_ docker/gribjump/skaffold.sh build
PREFIX=dev_ skaffold build
```

To make the worker consume previously built source-built replacements,
override the `worker_*_image` values directly while keeping the matching mode in
`image`:

```bash
PREFIX=dev_ \
  worker_mars_c_mode=image \
  worker_mars_c_image=registry.example/mars-base-c:tag \
  worker_mars_cpp_mode=image \
  worker_mars_cpp_image=registry.example/mars-base-cpp:tag \
  worker_gribjump_mode=image \
  worker_gribjump_image=registry.example/gribjump-source-worker-python:tag \
  skaffold build
```

To switch components off independently:

```bash
PREFIX=dev_ \
  worker_mars_c_mode=off \
  worker_mars_cpp_mode=off \
  worker_gribjump_mode=off \
  skaffold build
```

To keep the local rpm-backed MARS path while disabling GribJump:

```bash
PREFIX=dev_ \
  worker_mars_c_mode=rpm \
  worker_mars_cpp_mode=rpm \
  worker_gribjump_mode=off \
  skaffold build
```

This keeps the source-built path optional while reusing the same worker copy
contract across the blank, local rpm-backed, and helper-image paths.

Use `skaffold build --file-output=/tmp/builds.json` or `skaffold build -q` when
you need the exact published image references for a later worker build.

To build the same source GribJump/FDB environment outside Docker:

```bash
docker/gribjump/build.sh
source docker/gribjump/install/profile
```

Deployments are managed from `polytope-config`, for example:

```bash
cd ../polytope-config
./deploy.sh <location> <environment>
```

Use `./deploy.sh --template-dir /tmp/manifests <location> <environment>` for a
render-only dry run.

## Testing

Unit tests:
```bash
python -m pytest tests/unit
```

Generic integration tests (a deployment is required):
```
python -m pytest tests/integration --config </path/to/config1.yaml> --config </path/to/config2.yaml>
```


## Acknowledgements

Past and current funding and support is listed in the adjoining [Acknowledgements](./ACKNOWLEDGEMENTS.rst).
