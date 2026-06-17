# loadgen

Single-pod async load generator for Polytope BOBS download flow.

## Environment variables

Required:

- `LOADGEN_FRONTEND_URL`: frontend base URL with no trailing slash.
- `LOADGEN_COLLECTION`: collection for `POST /api/v1/requests/<collection>`.
- `LOADGEN_AUTH`: full `Authorization` header value.
- `LOADGEN_PAYLOAD_JSON`: JSON value used as the request body's `request` field; the binary wraps it as `{"verb":"retrieve","request":...}`.
- `LOADGEN_BOBS_SVC_TEMPLATE`: internal BOBS service URL template, e.g. `http://rel-bobs-{ordinal}:3000`.

Optional:

- `LOADGEN_MOCK_REALM`: enables per-request mock identity when non-empty.
- `LOADGEN_MOCK_ROLE`: default `default`.
- `LOADGEN_MOCK_USER_PREFIX`: default `mock-`.
- `LOADGEN_WARMUP_ITERS`: default `5`.
- `LOADGEN_CONCURRENCY`: default `64`.
- `LOADGEN_TOTAL_ITERS`: default `512`.
- `LOADGEN_RAMP_SECONDS`: default `30`.
- `LOADGEN_POLL_INTERVAL_MS`: default `250`.
- `LOADGEN_POLL_TIMEOUT_S`: default `600`.
- `LOADGEN_MAX_ERROR_RATE`: default `0.01`.

## Docker build

```sh
docker build -f loadgen/Dockerfile \
  --build-arg PACKAGE_NAME=loadgen \
  --build-arg BIN_NAME=loadgen \
  --build-arg GIT_AUTH_TOKEN="$GIT_AUTH_TOKEN" \
  -t polytope-loadgen .
```

The binary emits one `LOADGEN_SUMMARY:{...}` JSON line for log scraping.
