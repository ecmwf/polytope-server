#!/usr/bin/env bash

# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

: "${POLYTOPE_URL:?Set POLYTOPE_URL to the Polytope server base URL, for example https://polytope.example.org}"
: "${POLYTOPE_TOKEN:?Set POLYTOPE_TOKEN to a real configured admin bearer token}"

mock_roles="beta:viewer,data_access"

curl -fsS \
  -H "Authorization: Bearer ${POLYTOPE_TOKEN}" \
  -H "Polytope-Mock-Roles: ${mock_roles}" \
  "${POLYTOPE_URL%/}/api/v2/collections"

cat <<'PYTHON_EXAMPLE'

Equivalent Python client configuration:

from polytope.api import Client

client = Client(
    extra_headers={"Polytope-Mock-Roles": "beta:viewer,data_access"},
)

collections = client.collections()

Credentials must use the client's normal authentication configuration. Never place
Authorization, cookies, tokens, passwords, API keys, or other credentials in
extra_headers.
PYTHON_EXAMPLE
