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
secret-bearing values live in `skaffold.env`; source-build dependency pins live
in `env_build/deps.env` so the same GribJump/FDB environment can be built inside
or outside Docker.

```bash
# Create skaffold.env with SKAFFOLD_DEFAULT_REPO, rpm_repo,
# mars_config_repo, and mars_config_branch.
PREFIX=dev_ env_build/skaffold-with-deps.sh build
```

The default worker build keeps the existing apt-based MARS path. That path now
defaults to `mars-client=6.33.20.2` and `mars-client-cpp=7.1.9.1`, while still
allowing the versions to be overridden through Skaffold build args.

To build publishable source-built replacement base images for the C and C++
MARS clients:

```bash
GITHUB_TOKEN="$(gh auth token)" \
  PREFIX=dev_ env_build/skaffold-with-deps.sh build -f skaffold.mars-client.yaml
```

These images source-build the C and C++ MARS clients from
`ecmwf/mars-client-bundle` into the same locations used by the worker build:
`mars-base-c` provides `/opt/ecmwf/mars-client` and the `mars` binary, while
`mars-base-cpp` provides `/opt/ecmwf/mars-client-cpp`.
The source build uses a BuildKit secret backed by `GITHUB_TOKEN` so private
GitHub dependencies can be fetched without baking the token into the final
image.

To build the publishable source-built GribJump/FDB image that the worker can
copy from:

```bash
PREFIX=dev_ env_build/skaffold-with-deps.sh build -f skaffold.gribjump-source.yaml
```

This image builds the source bundle into `/opt/polytope/gribjump-source` and
the worker-installable wheels into `/opt/polytope/gribjump-source-wheels`.

The main worker build resolves `mars_base_c` and `mars_base_cpp` like this:

- `1`: use the existing local apt-backed stage (`mars-base-c` or `mars-base-cpp`)
- empty or unset: use `blank-base`
- any other value: pass it through as an external image reference

`gribjump_source_base` should be either unset or an explicit image reference.

To make the worker consume previously built source-built replacements,
override `mars_base_c`, `mars_base_cpp`, and `gribjump_source_base` directly:

```bash
PREFIX=dev_ \
  mars_base_c=registry.example/mars-base-c:tag \
  mars_base_cpp=registry.example/mars-base-cpp:tag \
  gribjump_source_base=registry.example/gribjump-source-worker-python:tag \
  env_build/skaffold-with-deps.sh build
```

This keeps the source-built path optional while reusing the same worker copy
contract as the current apt-based and source-built stages. Leave
`gribjump_source_base` unset to skip copying GribJump into the worker, or set it
to the exact image tag produced by `skaffold build -f skaffold.gribjump-source.yaml`.

Use `skaffold build --file-output=/tmp/builds.json` or `skaffold build -q` when
you need the exact published image references for a later worker build.

To build the same source GribJump/FDB environment outside Docker:

```bash
env_build/build.sh
source env_build/install/profile
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
