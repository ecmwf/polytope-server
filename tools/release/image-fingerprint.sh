#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0
#
# Print a content fingerprint for one application image at HEAD.
#
#   tools/release/image-fingerprint.sh <artifact> [--verbose]
#
# The fingerprint is a hash over everything that determines the image contents:
#
#   1. the resolved dependency closure of the image's root crate — exact
#      versions for registry crates, exact revisions for git crates
#      (dev-dependencies excluded, features as the Dockerfile builds them),
#   2. the git tree hash of every workspace/path crate in that closure
#      (covers source, Cargo.toml, and non-Rust files such as requirements.txt),
#   3. the image's Dockerfile and skaffold.yaml (base images, build args).
#
# Two commits with the same fingerprint for an artifact would produce the same
# image, so the release workflow can carry the previous image forward instead of
# rebuilding it. The tree is expected to be clean: uncommitted changes are not
# visible to git rev-parse and would be silently ignored.
#
# Requires: cargo (no native libs — `cargo tree` only resolves, it does not
# build), git. Git dependencies must be fetchable (GH_TOKEN / insteadOf).
set -euo pipefail

artifact=${1:?usage: image-fingerprint.sh <artifact> [--verbose]}
verbose=${2:-}

case "$artifact" in
  frontend)           package=polytope-server;    manifest=frontend/Cargo.toml;                   dockerfile=frontend/Dockerfile;                   features=metkit,telemetry ;;
  polytope-fe-worker) package=polytope-fe-worker; manifest=workers/polytope-fe-worker/Cargo.toml; dockerfile=workers/polytope-fe-worker/Dockerfile; features= ;;
  fdb-worker)         package=fdb-worker;         manifest=workers/fdb-worker/Cargo.toml;         dockerfile=workers/fdb-worker/Dockerfile;         features= ;;
  mars-worker)        package=mars-worker;        manifest=workers/mars-worker/Cargo.toml;        dockerfile=workers/mars-worker/Dockerfile;        features= ;;
  test-worker)        package=test-worker;        manifest=workers/test-worker/Cargo.toml;        dockerfile=workers/test-worker/Dockerfile;        features= ;;
  polytope-loadgen)   package=loadgen;            manifest=loadgen/Cargo.toml;                    dockerfile=loadgen/Dockerfile;                    features= ;;
  *) echo "unknown artifact '$artifact'" >&2; exit 2 ;;
esac

root=$(git rev-parse --show-toplevel)
cd "$root"

# `{p}` prints "name vX.Y.Z" for registry crates, "name vX.Y.Z (<abs path>)"
# for path crates and "name vX.Y.Z (<git url>#<rev>)" for git crates. Strip
# the "(*)" de-dup markers and "(proc-macro)" annotations, and make path
# crates relative so the result does not depend on the checkout location.
closure=$(cargo tree --manifest-path "$manifest" -p "$package" \
            -e normal,build --locked --prefix none --format '{p}' \
            ${features:+--features "$features"} \
          | sed -e 's| (\*)$||' -e 's| (proc-macro)||' -e "s|($root/|(|" \
          | sort -u)

path_dirs=$(printf '%s\n' "$closure" | sed -n 's|.* (\([^)]*\))$|\1|p' | grep -v '://' | sort -u)
tree_hashes=$(for rel in $path_dirs; do
  printf '%s %s\n' "$rel" "$(git rev-parse "HEAD:$rel")"
done)

build_env=$(git rev-parse "HEAD:$dockerfile" "HEAD:skaffold.yaml")

if [[ "$verbose" == "--verbose" ]]; then
  {
    echo "## closure"; echo "$closure"
    echo "## trees";   echo "$tree_hashes"
    echo "## build";   echo "$build_env"
  } >&2
fi

printf '%s\n%s\n%s\n' "$closure" "$tree_hashes" "$build_env" | sha256sum | cut -c1-16
