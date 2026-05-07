# Admin role mocking

`Polytope-Mock-Roles` lets a configured Polytope admin make a protected request as their own account but with a different effective realm and non-admin role set. Use it for operational checks of role-gated collections without creating temporary users or granting real access.

## Header grammar

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

## Authorization requirements

Only a real authenticated user whose real realm and role match the server `admin_bypass_roles` configuration may use this header. Non-admin users are rejected before downstream route checks run.

A mocked role list must not include any configured admin role for the mocked realm. For example, if `alpha:admin` is configured as an admin-bypass role, `Polytope-Mock-Roles: alpha:admin` is rejected.

This header does not bypass collection authorization. After the server accepts the header, normal downstream role checks run against the mocked realm and mocked roles. Mocked requests are not marked with the admin-bypass flag.

## Effective identity semantics

Usernames cannot be mocked. Mocked requests keep only the real username/version from the authenticated user. They replace the effective realm and roles with the header value.

Mocked requests do not inherit real admin scopes/attributes. The effective mocked user has empty `scopes` and empty `attributes`, even if the real admin account has production scopes or attributes.

## Audit fields

Accepted mocked-role requests log an audit event with:

- `real_username`
- `real_realm`
- `mocked_realm`
- `mocked_roles`
- `path`
- `request_id` from `X-Request-Id`, when present

When an accepted mocked request submits a job, the job-submission audit event also includes `job_id`.

## curl example

Put credentials only in the normal authentication mechanism, such as the `Authorization` header shown here. Credentials must never be placed in extra headers.

```sh
curl -fsS \
  -H "Authorization: Bearer ${POLYTOPE_TOKEN}" \
  -H "Polytope-Mock-Roles: beta:viewer,data_access" \
  "${POLYTOPE_URL}/api/v2/collections"
```

## Python client example

Configure the mock header through the client's safe extra-header support. Do not include `Authorization`, cookies, tokens, passwords, API keys, or other credentials in `extra_headers`; credentials must never be placed in extra headers.

```python
from polytope.api import Client

client = Client(
    extra_headers={"Polytope-Mock-Roles": "beta:viewer,data_access"},
)

collections = client.collections()
```
