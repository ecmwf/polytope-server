<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# Job Metadata Options

## Overview

Job metadata is a trusted server-side key-value store attached to each job. It carries configuration and policy decisions made by routing rules, authentication middleware, and other trusted server components. **Client requests cannot write to job metadata** — it is isolated from user-controllable input.

## Trust Boundary

Metadata exists on the server side of the trust boundary:

- **Trusted sources**: Routing configuration, authentication middleware, admin override headers (after auth), transform actions in routing rules.
- **Untrusted sources**: Client request JSON body, query parameters, most HTTP headers.

Client-supplied fields such as `metadata`, `polytope_mars`, `pre_path`, `use_catalogue`, and similar keys in the request body **must never** be merged into `job.metadata`. The server treats these as part of the user-controlled request and routes/validates them separately.

## Reserved Metadata Keys

The following metadata keys are reserved for specific trusted purposes:

- `cost`: Job cost estimation or billing data
- `admin_overrides`: Admin-controlled overrides (e.g., `mock_now_rfc3339` for time mocking)
- `accept_encoding`: Negotiated content encoding from HTTP headers
- `buffer_full_output`: Flag to buffer complete output before delivery (e.g., for v1 API compatibility)
- `polytope_mars`: Trusted datacube and options configuration for Polytope FE workers (see below)
- `metkit_ranges`: Per-key compact MARS ranges (e.g. `"1/to/100"`,
  `"0/to/240/by/6"`) with metkit-canonicalized endpoints, written by
  `transform::metkit_expansion` for FE workers that want to do their own
  availability-aware range expansion instead of consuming a fully
  enumerated list. See "The `metkit_ranges` metadata key" below and
  `docs/2026-08-19-metkit-range-preservation-plan.md` for the full design
  rationale.

Additional keys may be added by specific transform actions or middleware components. Always preserve existing keys unless explicitly overwriting a single key by design.

## The `set_metadata` Transform Action

The `set_metadata` action allows routing configuration to write or overwrite a single top-level key in `job.metadata`. It is **config-only** — the value is supplied entirely by the routing YAML and never interpolates or merges data from the client request.

### Configuration

```yaml
type: set_metadata
key: <string>
value: <any JSON value>
```

- `key`: The metadata key to write or overwrite.
- `value`: The value to set. Can be any JSON type (object, array, string, number, boolean, null).

### Behavior

- Writes or **overwrites** only the specified `key` in `job.metadata`.
- **Never** replaces the entire metadata map.
- Preserves all other existing metadata keys.
- If metadata is not an object (edge case), it is replaced with an empty object before writing the key.
- **Does not** read, merge, or interpolate any fields from the client request.

### Example: Setting Polytope MARS Options

```yaml
broker:
  transforms:
    - id: attach-climate-dt-fe-options
      type: set_metadata
      key: polytope_mars
      value:
        datacube: climate-dt
        options:
          axis_config:
            class_time_step_type_to_steps: ...
          pre_path:
            - climate-dt
          use_catalogue: catalogue1
          engine_options:
            datacube_version: 1
```

When this transform runs, `job.metadata["polytope_mars"]` is set to the configured object. The value is sourced **only** from the trusted routing configuration, never from client request fields such as `request.polytope_mars`, `request.metadata`, `request.pre_path`, or `request.use_catalogue`.

## The `metkit_ranges` metadata key

Unlike `set_metadata`, which is deliberately restricted to static,
config-only values (see "Security Considerations" below), `metkit_ranges` is
written automatically by `transform::metkit_expansion`
(`frontend/src/metkit_expansion.rs`) and its value **is** computed from the
request. This is a deliberate, narrow exception to the "config-only
transform actions" principle, and is safe specifically because:

- The value written is **not new information the client didn't already
  supply and have validated**. `job.request` is, as always, fully expanded
  by the same action into the flat/enumerated form every other check
  (`check_schedule`, `check_match`, `has_area`/`has_grid`, quotas, etc.)
  continues to validate against, completely unchanged. `metkit_ranges` is
  only ever a *compact, metkit-canonicalized re-expression* of a range the
  client already submitted for that same key -- it cannot smuggle in a
  value that wasn't already going to be part of the (still fully validated)
  expanded request.
- It is best-effort and purely additive: if a field isn't recognised as a
  single well-formed range, or the canonicalization probe doesn't come back
  as expected, that key is simply omitted from `metkit_ranges` -- the
  request is still served correctly via the ordinary fully-expanded
  `job.request`, just without the compact form for that key. Nothing here
  can cause a job to be rejected that would otherwise have succeeded.
- It is scoped to a single reserved key (not merged into `polytope_mars` or
  any other existing reserved key), so it's easy to audit and cannot be
  confused with, or override, genuinely static routing configuration.

### Shape

```json
{
  "metkit_ranges": {
    "step": "0/to/240/by/6",
    "levelist": "1/to/100"
  }
}
```

Only keys that were (a) submitted as a single, well-formed MARS range
(`a/to/b` or `a/to/b/by/c`) and (b) successfully round-tripped through a
metkit canonicalization probe are present. Absence of a key here is not an
error -- it just means the caller (e.g. an FE worker) should treat that
key as fully enumerated in `job.request` as usual, or reject if it
specifically requires a compact range for that key.

### Consumers

`workers/polytope-fe-worker/polytope.py` reads this key (alongside the
existing `polytope_mars` block) and overlays the compact range string onto
the retrieval request dict before calling `polytope_mars.extract()`, so the
FE/datacube layer can do FDB-availability-aware expansion for that axis
instead of being handed every enumerated value.

## Usage in Workers

Workers receive `job.metadata` as part of the work payload. The FE worker, for example, reads `metadata["polytope_mars"]` to overlay trusted datacube and options onto the per-request Polytope configuration, ensuring that dataset-specific FDB paths, catalogue selection, and engine options are controlled by routing policy rather than user input.

## Security Considerations

1. **Never merge request fields into metadata**. Client-supplied keys must remain in `job.request` only.
2. **Preserve trusted keys**. When adding new metadata, do not overwrite `cost`, `admin_overrides`, or other reserved keys unless that is the explicit intent.
3. **Config-only values, with one narrow, documented exception**. Transform actions like `set_metadata` must only write static values from the routing configuration, never dynamically constructed values derived from the request. The sole exception is `metkit_ranges` (see "The `metkit_ranges` metadata key" above), which is deliberately scoped to a single reserved key, is purely a compact re-expression of data already present and fully validated elsewhere in `job.request`, and can never cause a job to succeed or fail differently than it otherwise would. Any future request-derived metadata write should be held to that same bar and documented here, not treated as a precedent for looser writes in general.
4. **Routing controls structure**. Critical options such as `pre_path`, `use_catalogue`, `datacube`, and `engine_options` must be supplied via metadata from trusted routing rules, not read from the client request JSON.

By maintaining this separation, the server can safely route requests to dataset-specific configurations and enforce access policies without risking request-controlled structural options or privilege escalation.
