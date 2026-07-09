<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# Request IDs

Polytope request IDs are opaque public identifiers returned by BITS. Clients and server code must store and pass them back exactly as received, without parsing structure from the string.

The current IDs are 26-character lower-case Crockford base32 strings. They encode routing information for BITS internally, including the deployment `site`, `env`, broker slot, timestamp, and random bytes. That layout is an implementation detail: HTTP routes, clients, workers, logs, and authorization checks should not infer ownership, broker identity, creation order, or deployment from the visible text of an ID.

## Site and environment tags

Each deployment must have short stable tags:

```yaml
polytope:
  site: bol
  env: dev
```

The tags are one to three lower-case alphanumeric characters. They identify the deployment to Polytope configuration and are injected into the BITS configuration by the server config plumbing. Until that plumbing is enabled in this repository, keep the inner BITS config in sync explicitly:

```yaml
bits:
  site: bol
  env: dev
```

Use the same tags consistently for a deployment. Do not derive them ad hoc from pod names, namespaces, or release names.

## Cutover behavior

The new ID format is a hard cutover. Legacy persisted or in-flight requests whose IDs were created by an older BITS runtime are not routable by the new runtime.

Deployment options are therefore operational, not automatic:

- drain in-flight work before rolling out the new ID runtime; or
- explicitly accept loss for work active at rollout time.

Persisted legacy records can remain orphaned after rollout. If they matter operationally, clean them up after deployment with a backend-specific one-shot maintenance action rather than adding runtime compatibility paths.

## Implementation rules

- Treat request IDs as opaque strings in APIs, clients, workers, examples, and docs.
- Do not split request IDs or check for broker-specific textual structure.
- Do not use lexical ID ordering as creation-time ordering; use an explicit timestamp field when ordering matters.
- Do not add user-facing decode tools or CLI commands for request IDs.
