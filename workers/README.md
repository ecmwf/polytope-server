<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# workers

The worker side of the repository is split into separate crates so each worker can evolve or move independently.

## Layout

- `workers/common/` — shared remote-worker runtime and protocol client
- `workers/polytope-fe-worker/` — Polytope worker
- `workers/fdb-worker/` — FDB worker
- `workers/mars-worker/` — Mars worker stub

## Run commands

```bash
cargo run -p polytope-fe-worker -- --broker-url http://127.0.0.1:9001 --config-path worker-config.yaml
cargo run -p fdb-worker -- --broker-url http://127.0.0.1:9001 --config-path worker-config.yaml
cargo run -p mars-worker -- --broker-url http://127.0.0.1:9001 --config-path worker-config.yaml
```

## Streaming contract

Workers use the BITS remote-worker protocol documented in `../../bits/docs/src/external-workers.md`.

- success data is uploaded as a streamed request body to `/complete/data/{job_id}`
- control outcomes use `/complete/reject/{job_id}`, `/complete/error/{job_id}`, and `/complete/redirect/{job_id}`

`job_id` values are opaque request IDs. Worker code should echo them back exactly as supplied by the broker and must not parse broker ownership, site, environment, or ordering from the ID text.

That keeps successful results fully streamed from worker to broker to end user.
