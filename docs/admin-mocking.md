<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# Admin mocking

Admin mocking is an operational facility for configured Polytope admins. It lets an admin submit a protected request with one or both of these headers:

- `Polytope-Mock-Roles` — change the effective realm and non-admin roles used by downstream collection authorization.
- `Polytope-Mock-Time` — change the clock used by Polytope's `ScheduleReleased` embargo check for the submitted job.

Use these headers for controlled operational checks only. Put credentials only in the normal authentication mechanism, for example the `Authorization` header or the Python client's normal authentication configuration. Never place bearer tokens, cookies, passwords, API keys, or other credentials in `extra_headers` or other mock-header configuration.

## Shared authorization requirements

Mock headers require a real authenticated user. A request containing either mock header is rejected when the server cannot identify a real user, including auth-disabled operation and anonymous requests without authentication material.

Only a real authenticated user whose real realm and role match the server `admin_bypass_roles` configuration may use mock headers. Non-admin users are rejected before downstream route checks run.

If both headers are present, they are parsed and authorized independently but under the same real-admin requirement. A malformed `Polytope-Mock-Roles` value is reported before a malformed `Polytope-Mock-Time` value.

## Shared audit model

Accepted mock requests emit structured audit events. The audit data records the real authenticated identity, the mock value accepted by Polytope, the request path, and `request_id` from `X-Request-Id` when present.

The accepted-request events are:

- `polytope_mock_roles_accepted`
- `polytope_mock_time_accepted`

When an accepted mocked request submits a job, Polytope also emits job-submission audit events that include the submitted `job_id`:

- `polytope_mock_roles_job_submitted`
- `polytope_mock_time_job_submitted`

For mock time, audit records the normalised RFC3339 UTC timestamp and the static header identity `polytope-mock-time`; it does not store or log the raw accepted header value.

## `Polytope-Mock-Roles`

`Polytope-Mock-Roles` lets a configured Polytope admin make a protected request as their own account but with a different effective realm and non-admin role set. Use it for operational checks of role-gated collections without creating temporary users or granting real access.

### Header grammar

Send exactly one header value:

```text
Polytope-Mock-Roles: <realm>:<role>,<role>,...
```

Example:

```text
Polytope-Mock-Roles: beta:viewer,data_access
```

Rules:

- The value must be valid UTF-8.
- Surrounding whitespace around the value, realm, and roles is ignored.
- The realm must be non-empty.
- At least one role is required.
- Empty role segments are rejected.
- Role names must not contain `:`.
- Control characters are rejected.
- Multiple `Polytope-Mock-Roles` header values are rejected.

### Admin-role rejection

A mocked role list must not include any configured admin role for the mocked realm. For example, if `alpha:admin` is configured as an admin-bypass role, `Polytope-Mock-Roles: alpha:admin` is rejected.

### Effective identity semantics

Usernames cannot be mocked. Mocked-role requests keep only the real username/version from the authenticated user. They replace the effective realm and roles with the header value.

Mocked-role requests do not inherit real admin scopes or attributes. The effective mocked user has empty `scopes` and empty `attributes`, even if the real admin account has production scopes or attributes.

This header does not bypass collection authorization. After the server accepts the header, normal downstream role checks run against the mocked realm and mocked roles. Mocked requests are not marked with the admin-bypass flag.

### Role audit fields

Accepted mocked-role requests log:

- `real_username`
- `real_realm`
- `mocked_realm`
- `mocked_roles`
- `path`
- `request_id`, when present

The job-submission event also includes `job_id`.

### Role examples

```sh
curl -fsS \
  -H "Authorization: Bearer ${POLYTOPE_TOKEN}" \
  -H "Polytope-Mock-Roles: beta:viewer,data_access" \
  "${POLYTOPE_URL%/}/api/v2/collections"
```

```python
from polytope.api import Client

client = Client(
    extra_headers={"Polytope-Mock-Roles": "beta:viewer,data_access"},
)

collections = client.collections()
```

## `Polytope-Mock-Time`

`Polytope-Mock-Time` lets a configured Polytope admin submit a request with a mocked UTC instant for Polytope's schedule-release check.

Only `ScheduleReleased` consumes mocked time. Other server timestamps, token expiry checks, job creation timestamps, logs, OpenMeteo request-date construction, and downstream archive systems continue to use their normal clocks.

### Header grammar and UTC semantics

Send exactly one header value in either of these forms:

```text
Polytope-Mock-Time: HH:MM[:SS]
Polytope-Mock-Time: <RFC3339 datetime>
```

Examples:

```text
Polytope-Mock-Time: 12:30
Polytope-Mock-Time: 12:30:45
Polytope-Mock-Time: 2040-05-06T07:08:09Z
Polytope-Mock-Time: 2040-05-06T08:08:09+01:00
```

Rules:

- The value must be valid UTF-8.
- Surrounding whitespace is ignored.
- Control characters are rejected.
- Multiple `Polytope-Mock-Time` header values are rejected.
- `HH:MM[:SS]` values are interpreted as a time on today's UTC date. Seconds default to `00` when omitted.
- Time-only values are UTC-only and must not include a timezone suffix such as `Z` or `+01:00`.
- Full RFC3339 datetimes are accepted as instants and normalised to UTC for storage and audit. A non-UTC offset is converted to the equivalent UTC instant.
- Midnight does not roll to another date. For example, if today's UTC date is `2030-01-02`, `00:05` means `2030-01-02T00:05:00Z`.

### Precedence and persistence

`ScheduleReleased` chooses the time for its release check in this order:

1. `Polytope-Mock-Time` accepted on the request and stored in job metadata as `metadata.admin_overrides.mock_now_rfc3339`.
2. Static action configuration `ScheduleReleased.now_rfc3339`.
3. The server wall clock.

The accepted header therefore takes precedence over static `ScheduleReleased.now_rfc3339` configuration for that job. Restored jobs retain the persisted `metadata.admin_overrides.mock_now_rfc3339` value, so a schedule check re-run after job recovery uses the same mocked instant that was submitted originally.

### Time audit fields

Accepted mocked-time requests log:

- `real_username`
- `real_realm`
- `mocked_now` as a normalised RFC3339 UTC timestamp
- `path`
- `request_id`, when present
- `header = "polytope-mock-time"`

The job-submission event also includes `job_id`.

### Time examples

```sh
curl -fsS \
  -H "Authorization: Bearer ${POLYTOPE_TOKEN}" \
  -H "Polytope-Mock-Time: 12:30:00" \
  "${POLYTOPE_URL%/}/api/v2/collections"

curl -fsS \
  -H "Authorization: Bearer ${POLYTOPE_TOKEN}" \
  -H "Polytope-Mock-Time: 2040-05-06T07:08:09Z" \
  "${POLYTOPE_URL%/}/api/v2/collections"
```

```python
from polytope.api import Client

client = Client(
    extra_headers={"Polytope-Mock-Time": "2040-05-06T07:08:09Z"},
)

collections = client.collections()
```

## Python `extra_headers` usage

The Python client can send mock headers through `extra_headers`:

```python
from polytope.api import Client

client = Client(
    extra_headers={
        "Polytope-Mock-Roles": "beta:viewer,data_access",
        "Polytope-Mock-Time": "2040-05-06T07:08:09Z",
    },
)
```

Use `extra_headers` only for non-secret operational headers. Credentials must use the client's normal authentication configuration and must never be placed in `extra_headers`.

## Scope and risks

- Mock time bypasses the embargo gate only inside Polytope's schedule check.
- Actual archive backends such as MARS do not see the mocked clock.
- Any data present in the archive before its scheduled release becomes retrievable by admins using this header.
- Audit review on `polytope_mock_time_accepted` and `polytope_mock_time_job_submitted` is the sole post-hoc control.
