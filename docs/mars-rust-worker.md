# MARS Rust worker

The MARS worker uses `mars-client-rs` from ECMWF's `mars-client-cpp` repository.
Its standalone `workers/mars-worker/Cargo.lock` pins the Rust bindings and native
stack. Build and test it separately from the root workspace:

```sh
cargo test --locked --manifest-path workers/mars-worker/Cargo.toml
cargo clippy --locked --manifest-path workers/mars-worker/Cargo.toml --all-targets
cargo fmt --manifest-path workers/mars-worker/Cargo.toml -- --check
```

Native builds require a C++ toolchain, CMake, Python 3, bison/flex, libclang,
and the development libraries listed in `workers/mars-worker/Dockerfile`.

## Requests and stream lifecycle

JSON requests are validated before entering the blocking executor. Inside the
serialized native retrieval, the worker creates a typed `MarsRequest`, sets its
values programmatically, and calls `retrieve_with_credentials`. Credentials remain
request-scoped. The callback relay and its per-request generation are unchanged.

`MARS_STREAM_QUEUE_BYTE_LIMIT` bounds the native producer queue (default 32 MiB).
The async bridge also bounds its channel. A disconnected consumer closes the
native stream; closing joins the producer before the callback generation ends.

A `Restarted` event asks the consumer to rewind its output and replay bytes.
Polytope cannot rewind bytes already delivered, so it fails the request and asks
the client to retry. This protocol event does not restart the worker. Transport
exceptions and other fatal native errors retain their restart policy.

## Native libraries and packaging

Cargo's `-sys` crates build the libraries used by the CXX bridges. The worker
image packages those exact installed libraries and their `share`/`etc` resources
under `/opt/mars-client`; it does not consume `cpp-mars-libs`. The standalone
bundle-image recipe under `docker/cpp-libs/mars` remains a separate legacy build,
not the dependency source for this worker.

The manifest aligns ecCodes and metkit sources across the wrapper ecosystem.
The lockfile pins eckit to a revision compatible with the MIR release selected
by `mir-sys`. Update this stack together and rebuild/test the worker; updating
only a floating upstream branch can introduce incompatible C++ APIs.

The upstream wrapper's native build enables FDB and ODB support but disables
MARS NetCDF output. Do not treat this image as a validated NetCDF-output worker.

## Logging

The native wrappers emit `log` records instead of calling a raw C callback.
The worker routes native-library targets into its existing per-request log
chunks and forwards Rust dependency records to tracing. Request completion,
size limits and periodic flushes preserve the existing `mars.logs` events.

## Build and verify a development image

```sh
FIXED_TAG=<commit>-mars-rust-<source-hash> skaffold build --push=true \
  -b eccr.ecmwf.int/polytope/mars-worker
```

Use `-p podman` on hosts where Docker is a Podman shim. Deploy the image to all
MARS-backed pools using the configuration repository's Helmfile workflow, then
run its Bologna verification suite. Preserve the running chart version and
unrelated frontend/worker images when validating only the MARS migration.
