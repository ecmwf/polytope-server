Testing if this links to JIRA, don't merge.

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
* **authentication**: for authenticating users to Polytope (ECMWF API, Basic MongoDB, API Key MongoDB)
* **authorization**: for managing authorization to different collections (ECMWF LDAP, MongoDB)
* **identity**: for registering users with Polytope (MongoDB)
* **api-keys**: for generating Polytope API keys (MongoDB)
* **caching**: caching of web requests to various services (MongoDB, Redis, Memcached)

## Quick Start

```bash
cd polytope-deployment
export SKAFFOLD_IMAGE_REGISTRY=localhost:32000
skaffold dev
```

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
