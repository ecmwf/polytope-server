# frontend

This crate contains the Polytope HTTP frontend only.

- Binary: `polytope-server`
- Source: `frontend/src/`
- Depends on: `bits`, `bits-ecmwf`, Axum stack
- Does not depend on any worker crate

Run locally:

```bash
cargo run -p polytope-server -- config.yaml
```

The frontend submits jobs to bits and streams successful responses back to end users over HTTP.
