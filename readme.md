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
