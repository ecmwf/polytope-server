<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# Model Context Protocol frontend

Polytope can expose a Model Context Protocol (MCP) endpoint at `/mcp`. The
endpoint is another frontend over the same BITS broker used by the v1, v2,
OpenMeteo, and EDR APIs.

The MCP endpoint is asynchronous-first. Tools submit Polytope jobs, return
opaque request IDs, and let the MCP client poll until the job is ready.
Long-running jobs continue in Polytope after an MCP tool call returns `pending`.

## Configuration

```yaml
mcp:
  catalogue_url: "https://catalogue.lumi.apps.dte.destination-earth.eu/"
  inline_result_max_bytes: 65536
  allowed_hosts:
    - polytope.example.org
```

Fields:

- `catalogue_url` is advertised in tool results and server instructions so
  agents can discover datasets outside Polytope.
- `inline_result_max_bytes` is the largest JSON/text result the MCP frontend may
  place directly into a tool result.
- `allowed_hosts` and `allowed_origins` configure the RMCP Streamable HTTP
  transport checks. If `allowed_hosts` is empty, host validation is disabled.

The Helm chart renders this block when `mcp.enabled: true` is set in values.

## Endpoint

The endpoint path is always:

```text
/mcp
```

MCP protocol versioning is negotiated inside the protocol `initialize` request
and, for HTTP, through the `MCP-Protocol-Version` header. The URL is not
versioned.

## Authentication

`/mcp` is protected by the same frontend authentication middleware as
`/api/v1`, `/api/v2`, and `/edr`.

Missing credentials on `/mcp` ask auth-o-tron for the authentication challenge
and return its `WWW-Authenticate` header. This lets MCP clients discover
OAuth/protected-resource metadata when auth-o-tron is configured for it.

MCP tools never accept secrets as tool arguments. The authenticated `AuthUser`
from the HTTP request is copied into the BITS job user context, so existing role
checks, audit fields, quota inputs, and mocked admin identities keep working.

## Tools

Initial tools:

- `polytope_whoami` — returns the authenticated username, realm, roles, and scope names.
- `polytope_list_collections` — lists configured Polytope collections and the
  optional catalogue URL.
- `polytope_submit` — submits a request to a collection and returns a request ID.
- `polytope_poll` — polls a request ID once, optionally long-polling for up to
  60 seconds in that MCP call.
- `polytope_cancel` — cancels a request ID.

`polytope_submit` sets `metadata.api = "mcp"`, `metadata.collection`, and
`metadata.buffer_full_output = true`. MCP clients should receive download URLs
or small inline JSON/text, not arbitrary GRIB/NetCDF bytes in the model context.

## Result policy

When a job is ready:

- Redirect results are returned as `delivery: "redirect"` with `download_url`.
- Small text/JSON direct results are returned as `delivery: "inline"`.
- Binary or oversized direct streams are not inlined. The tool returns a
  structured error explaining that the route or worker should return a redirect
  for MCP downloads.
