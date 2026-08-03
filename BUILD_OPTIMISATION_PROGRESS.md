# Image-build optimisation progress

This file tracks the phased work started on branch `agent/build-cache-versioning`.

## Baseline

- GitHub Actions job 91828185613 (2026-08-03) built the frontend in **5m46s**.
- Its cold `cargo chef cook` step took **210s**; compiling `cargo-chef` took
  about **52s**; the final application compile/link took **30s**.
- The PR changed `mars-worker`, but CI skipped Mars because its release tag
  already existed and instead built an unrelated un-published frontend tag.

## Phases

- [x] **1 — PR selection correctness:** dynamically select affected image(s),
  including conservative shared-dependency fan-out; do not skip a PR image build
  because a release image tag already exists.
- [x] **2 — Cold-build tooling:** replaced in-Dockerfile `cargo install
  cargo-chef` with the digest-pinned cargo-chef Rust base image
  `0.1.77-rust-1.94.1-slim-bookworm`.
- [x] **3 — Secret safety:** replaced GitHub-token build arguments and persistent
  Git configuration with BuildKit secrets. Cargo's temporary Git configuration
  exists only inside each secret-mounted `RUN` instruction.
  The legacy Kaniko profile is not compatible with BuildKit secret mounts and
  is deferred for removal while BuildKit becomes the supported path.
- [x] **4 — Shared BuildKit cache:** added the Skaffold `buildx-cache` profile.
  It imports the per-image ECCR cache by default and exports it only when
  `BUILDKIT_CACHE_MODE=read-write`; PR CI uses that write mode. The ordinary
  Docker driver was probed and cannot export registry caches, so the profile
  deliberately uses a custom Buildx wrapper.
- [x] **5 — Release integration:** release builds use the same Skaffold Buildx
  cache and record each published image's digest and source revision in the job
  summary. A durable cross-image release bundle remains Phase 6 work.
- [ ] **6 — Versioning migration:** design/implement a product bundle manifest
  and digest-pinned deployment handoff across `polytope-server`,
  `polytope-chart`, and `polytope-config`. This requires coordinated changes in
  the other repositories and is intentionally not mixed into container-cache
  commits.

## Guardrails

- Cache misses must be visible; a skipped image is never considered a benchmark.
- A cache exporter must never include a credential-bearing filesystem layer.
- The production deployment identity remains an image digest; tags are human
  metadata and cache lookup inputs only.
