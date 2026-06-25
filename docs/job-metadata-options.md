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

## Usage in Workers

Workers receive `job.metadata` as part of the work payload. The FE worker, for example, reads `metadata["polytope_mars"]` to overlay trusted datacube and options onto the per-request Polytope configuration, ensuring that dataset-specific FDB paths, catalogue selection, and engine options are controlled by routing policy rather than user input.

## Security Considerations

1. **Never merge request fields into metadata**. Client-supplied keys must remain in `job.request` only.
2. **Preserve trusted keys**. When adding new metadata, do not overwrite `cost`, `admin_overrides`, or other reserved keys unless that is the explicit intent.
3. **Config-only values**. Transform actions like `set_metadata` must only write static values from the routing configuration, never dynamically constructed values derived from the request.
4. **Routing controls structure**. Critical options such as `pre_path`, `use_catalogue`, `datacube`, and `engine_options` must be supplied via metadata from trusted routing rules, not read from the client request JSON.

By maintaining this separation, the server can safely route requests to dataset-specific configurations and enforce access policies without risking request-controlled structural options or privilege escalation.
