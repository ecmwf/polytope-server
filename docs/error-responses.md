<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# Error responses

Every response that reaches a user carries a request ID, and every error body is
a single, self-contained, human-readable message that says what happened, what to
do, and where to raise a support ticket — quoting the request ID.

This is produced centrally by one outer middleware
(`frontend/src/support.rs`), so no handler can forget it and the shape is
uniform across the whole API.

## The contract

- **Success and error responses** carry an `X-Request-Id` header. It is reused
  from an inbound `X-Request-Id` when the caller (or an ingress) supplied one,
  otherwise minted in the BITS request-ID format. Treat it as an opaque string
  (see [request-ids.md](./request-ids.md)).
- **Error responses** (HTTP status `>= 400`) have a JSON body of exactly:

  ```json
  { "message": "<one self-contained sentence or two>" }
  ```

  A single string field, and nothing else. Downstream clients that flatten a
  response object into their output must never receive a non-string value here;
  keeping the body to one string field is a hard requirement, enforced by
  `frontend/tests/support_errors.rs`.

  API surfaces with external error contracts keep their native body shape:
  `/edr/*` preserves OGC EDR error responses, and `/openmeteo/*` preserves the
  Open-Meteo `{"error": true, "reason": "..."}` shape when that API is enabled.
  They still receive the `X-Request-Id` and security headers.

Clients should display `message` verbatim. The request ID is inside the text and
also in the `X-Request-Id` header.

## Wording by error class

The lead sentence and the advice depend on the status class:

| Status | Class | Message shape |
| --- | --- | --- |
| 401, 403 | auth | "Your request was not authorised: …. Check your credentials … If you believe you should have access, open a support ticket at …" |
| other 4xx | client | "Your request could not be processed: …. Check your request … If you believe this is a mistake or need help, open a support ticket at …" |
| 429, 529 | overloaded | "Polytope is temporarily overloaded …. Please wait a few seconds and retry. If this keeps happening, open a support ticket at …" |
| 5xx | server | "Polytope encountered an internal error …. This has been logged. Please retry shortly; if the problem persists, open a support ticket at …" |

The trailing clause is:

- `open a support ticket at <url> and quote your request ID <id>.` when both a
  support URL and a request ID are available;
- it omits the "quote your request ID" part when no ID is available, and falls
  back to "contact Polytope support" when no URL is configured.

Example (a 404 on an ECMWF deployment):

```json
{
  "message": "Your request could not be processed: unknown collection 'ecmwf-clymate'. Check your request and try again. If you believe this is a mistake or need help, open a support ticket at https://support.ecmwf.int/ and quote your request ID 3k7p9q2r5s8t1v4w6x0y2z5a8b."
}
```

## Choosing the support URL

The URL is resolved per error from `support` config:

```yaml
support:
  default_url: https://platform.destine.eu/contact/   # the deployment operator
  realms:                                             # optional per-realm override
    ecmwf: https://support.ecmwf.int/
    desp: https://platform.destine.eu/contact/
    destine: https://platform.destine.eu/contact/
```

- The **deployment default** covers every error, including the many that occur
  before authentication (missing/invalid credentials, malformed request,
  overload) where no realm is known.
- The **realm override** applies whenever the request is authenticated and the
  user's realm is mapped — for all error classes, including 5xx. A single
  deployment can serve multiple communities (e.g. an `ecmwf` user and a `desp`
  user on the same cluster), so the support desk follows the user, not just the
  cluster.

If `support` is absent the messages still render; they just say "contact
Polytope support" without a link.
