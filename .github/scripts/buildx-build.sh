#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

# Build a Skaffold custom artifact with Buildx and the shared ECCR cache.
#
# Skaffold's Docker builder invokes `docker build`, whose default docker driver
# cannot export registry cache on GitHub-hosted runners. This wrapper is used by
# the opt-in buildx-cache profile and honours Skaffold's custom-builder contract.
set -euo pipefail

: "${IMAGE:?Skaffold must set IMAGE}"
: "${BUILD_CONTEXT:?Skaffold must set BUILD_CONTEXT}"
: "${GH_TOKEN:?GH_TOKEN is required for private Cargo Git dependencies}"

image_without_tag="${IMAGE%%@*}"
image_without_tag="${image_without_tag%:*}"
image_name="${image_without_tag##*/}"

case "$image_name" in
  frontend) dockerfile="frontend/Dockerfile" ;;
  polytope-fe-worker) dockerfile="workers/polytope-fe-worker/Dockerfile" ;;
  fdb-worker) dockerfile="workers/fdb-worker/Dockerfile" ;;
  mars-worker) dockerfile="workers/mars-worker/Dockerfile" ;;
  test-worker) dockerfile="workers/test-worker/Dockerfile" ;;
  polytope-loadgen) dockerfile="loadgen/Dockerfile" ;;
  *) echo "Unsupported Skaffold image: $IMAGE" >&2; exit 2 ;;
esac

cache_repo="${BUILDKIT_CACHE_REPOSITORY:-eccr.ecmwf.int/polytope/build-cache}"
cache_ref="${cache_repo}:${image_name}-linux-amd64-v1"
platforms="${PLATFORMS:-linux/amd64}"

args=(
  docker buildx build
  --file "$BUILD_CONTEXT/$dockerfile"
  --target release
  --tag "$IMAGE"
  --platform "$platforms"
  --secret id=GIT_AUTH_TOKEN,env=GH_TOKEN
  --cache-from "type=registry,ref=$cache_ref"
)

if [[ "${VERSION:-}" != "" ]]; then
  args+=(--build-arg "VERSION=$VERSION")
fi
if [[ "${REVISION:-}" != "" ]]; then
  args+=(--build-arg "REVISION=$REVISION")
fi

case "${BUILDKIT_CACHE_MODE:-read}" in
  read) ;;
  read-write)
    args+=(--cache-to "type=registry,ref=$cache_ref,mode=max")
    ;;
  *)
    echo "BUILDKIT_CACHE_MODE must be read or read-write" >&2
    exit 2
    ;;
esac

if [[ "${PUSH_IMAGE:-false}" == "true" ]]; then
  args+=(--push)
else
  # The repository currently builds one platform; loading a multi-platform
  # result into the Docker daemon would not be supported.
  if [[ "$platforms" == *,* ]]; then
    echo "Cannot --load a multi-platform Buildx result; use --push." >&2
    exit 2
  fi
  args+=(--load)
fi

printf 'Running:' >&2
printf ' %q' "${args[@]}" >&2
printf ' %q\n' "$BUILD_CONTEXT" >&2
"${args[@]}" "$BUILD_CONTEXT"
