#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0
#
# Print content fingerprints for the application images at HEAD.
#
#   tools/release/image-fingerprint.sh <artifact> [--verbose]
#   tools/release/image-fingerprint.sh --all          # "<artifact> <fingerprint>" per line
#
# The fingerprint is a hash over everything that determines the image contents:
#
#   1. the resolved dependency closure of the image's root crate — exact
#      versions for registry crates, exact revisions for git crates
#      (dev-dependencies excluded, package and features exactly as the
#      Dockerfile builds them),
#   2. the git tree hash of every workspace/path crate in that closure
#      (covers source, Cargo.toml, and non-Rust files such as requirements.txt),
#   3. the image's Dockerfile and the shared build inputs: skaffold.yaml, the
#      root Cargo.toml (profiles, workspace-level settings), .dockerignore and
#      the Buildx wrapper.
#
# Two commits with the same fingerprint for an artifact would produce the same
# image, so the release workflow can carry the previous image forward instead of
# rebuilding it. Mutable external inputs (base images tagged rather than pinned
# by digest, apt packages, unpinned Python requirements) are NOT covered; touch
# the image's Dockerfile to force a rebuild.
#
# The tree is expected to be clean: uncommitted changes are not visible to
# git rev-parse and would be silently ignored.
#
# Requires: cargo (no native libs — `cargo tree` only resolves, it does not
# build), git. Git dependencies must be fetchable (GH_TOKEN / insteadOf).
set -euo pipefail
export LC_ALL=C

ARTIFACTS=(frontend polytope-fe-worker fdb-worker mars-worker test-worker polytope-loadgen)

# Echo "<manifest> <dockerfile>" for an artifact.
artifact_paths() {
  case "$1" in
    frontend)           echo "frontend/Cargo.toml frontend/Dockerfile" ;;
    polytope-fe-worker) echo "workers/polytope-fe-worker/Cargo.toml workers/polytope-fe-worker/Dockerfile" ;;
    fdb-worker)         echo "workers/fdb-worker/Cargo.toml workers/fdb-worker/Dockerfile" ;;
    mars-worker)        echo "workers/mars-worker/Cargo.toml workers/mars-worker/Dockerfile" ;;
    test-worker)        echo "workers/test-worker/Cargo.toml workers/test-worker/Dockerfile" ;;
    polytope-loadgen)   echo "loadgen/Cargo.toml loadgen/Dockerfile" ;;
    *) echo "unknown artifact '$1'" >&2; return 2 ;;
  esac
}

# Echo the single distinct value of `ARG <name>=<value>` in a Dockerfile, or
# nothing if the ARG is absent. Fails if the Dockerfile disagrees with itself.
dockerfile_arg() {
  local dockerfile=$1 name=$2 values
  values=$(sed -n "s|^ARG ${name}=\"\{0,1\}\([^\"]*\)\"\{0,1\}\$|\1|p" "$dockerfile" | sort -u)
  if [[ $(printf '%s\n' "$values" | grep -c .) -gt 1 ]]; then
    echo "$dockerfile declares several values for ARG $name:" >&2
    printf '%s\n' "$values" >&2
    return 1
  fi
  printf '%s' "$values"
}

# Sets FP. Deliberately not a command substitution: bash disables errexit inside
# $(...), so a failing `cargo tree` there would yield a fingerprint of an empty
# closure instead of aborting.
fingerprint() {
  local artifact=$1 verbose=${2:-}
  local paths manifest dockerfile package features
  paths=$(artifact_paths "$artifact")
  read -r manifest dockerfile <<<"$paths"

  package=$(dockerfile_arg "$dockerfile" PACKAGE_NAME)
  features=$(dockerfile_arg "$dockerfile" FEATURES)
  [[ -n "$package" ]] || { echo "$dockerfile has no ARG PACKAGE_NAME" >&2; return 1; }

  # `{p}` prints "name vX.Y.Z" for registry crates, "name vX.Y.Z (<abs path>)"
  # for path crates and "name vX.Y.Z (<git url>#<rev>)" for git crates. Strip
  # the "(*)" de-dup markers and "(proc-macro)" annotations, and make path
  # crates relative so the result does not depend on the checkout location.
  local closure
  closure=$(cargo tree --manifest-path "$manifest" -p "$package" \
              -e normal,build --locked --prefix none --format '{p}' \
              ${features:+--features "$features"} \
            | sed -e 's| (\*)$||' -e 's| (proc-macro)||' -e "s|($root/|(|" \
            | sort -u)

  local path_dirs tree_hashes="" rel hash
  path_dirs=$(printf '%s\n' "$closure" | sed -n 's|.* (\([^)]*\))$|\1|p' | grep -v '://' | sort -u)
  for rel in $path_dirs; do
    hash=$(git rev-parse "HEAD:$rel")
    tree_hashes+="$rel $hash"$'\n'
  done

  local build_env
  build_env=$(git rev-parse \
    "HEAD:$dockerfile" HEAD:skaffold.yaml HEAD:Cargo.toml HEAD:.dockerignore \
    HEAD:.github/scripts/buildx-build.sh)

  if [[ "$verbose" == "--verbose" ]]; then
    {
      echo "## package";  echo "$package (features: ${features:-none})"
      echo "## closure";  echo "$closure"
      echo "## trees";    echo "$tree_hashes"
      echo "## build";    echo "$build_env"
    } >&2
  fi

  FP=$(printf '%s\n%s\n%s\n' "$closure" "$tree_hashes" "$build_env" | sha256sum | cut -c1-16)
}

root=$(git rev-parse --show-toplevel)
cd "$root"

case "${1:-}" in
  "")
    echo "usage: image-fingerprint.sh <artifact> [--verbose] | --all" >&2
    exit 2
    ;;
  --all)
    for artifact in "${ARTIFACTS[@]}"; do
      fingerprint "$artifact"
      echo "$artifact $FP"
    done
    ;;
  *)
    fingerprint "$1" "${2:-}"
    echo "$FP"
    ;;
esac
