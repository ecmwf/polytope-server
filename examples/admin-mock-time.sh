#!/usr/bin/env bash
set -euo pipefail

: "${POLYTOPE_URL:?Set POLYTOPE_URL to the Polytope server base URL, for example https://polytope.example.org}"
: "${POLYTOPE_TOKEN:?Set POLYTOPE_TOKEN to a real configured admin bearer token}"

# Accepted forms:
# - HH:MM:SS, interpreted on today's UTC date
# - RFC3339 datetime, normalised to UTC by the server
mock_time="12:34:56"
# mock_time="2030-01-02T12:34:56Z"

curl -fsS \
  -H "Authorization: Bearer ${POLYTOPE_TOKEN}" \
  -H "Polytope-Mock-Time: ${mock_time}" \
  "${POLYTOPE_URL%/}/api/v2/collections"

cat <<'PYTHON_EXAMPLE'

Equivalent Python client configuration:

from polytope.api import Client

client = Client(
    extra_headers={"Polytope-Mock-Time": "12:34:56"},
    # Or use a full RFC3339 datetime:
    # extra_headers={"Polytope-Mock-Time": "2030-01-02T12:34:56Z"},
)

collections = client.collections()

Credentials must use the client's normal authentication configuration. Never place
Authorization, cookies, tokens, passwords, API keys, or other credentials in
extra_headers.
PYTHON_EXAMPLE
