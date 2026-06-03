#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DEFAULT_CONFIG="${SCRIPT_DIR}/skaffold.yaml"

sanitize_tag_component() {
  local value="$1"
  local max_len="$2"
  value="${value//\//-}"
  value="${value//:/-}"
  value="${value//@/-}"
  value="${value//+/-}"
  printf '%s' "${value:0:max_len}"
}

resolve_default_repo() {
  if [ -n "${SKAFFOLD_DEFAULT_REPO:-}" ]; then
    printf '%s' "${SKAFFOLD_DEFAULT_REPO}"
    return
  fi

  if [ -f "${REPO_DIR}/skaffold.env" ]; then
    bash -c 'set -a; source "$1" >/dev/null 2>&1; printf "%s" "${SKAFFOLD_DEFAULT_REPO:-}"' _ "${REPO_DIR}/skaffold.env"
  fi
}

DEPS_FILE="${DEPS_FILE:-${SCRIPT_DIR}/deps.env}"
if [ ! -f "${DEPS_FILE}" ]; then
  echo "Dependency file not found: ${DEPS_FILE}" >&2
  exit 1
fi

# shellcheck source=/dev/null
source "${DEPS_FILE}"

# Skaffold build args use lower-case names. Keep explicit lower-case env vars
# as the strongest override, otherwise bridge from the shared upper-case pins.
export ecbuild_version="${ecbuild_version:-${ECBUILD_VERSION}}"
export libaec_version="${libaec_version:-${LIBAEC_VERSION}}"
export eckit_version="${eckit_version:-${ECKIT_VERSION}}"
export eccodes_version="${eccodes_version:-${ECCODES_VERSION}}"
export metkit_version="${metkit_version:-${METKIT_VERSION}}"
export fdb_version="${fdb_version:-${FDB_VERSION}}"
export gribjump_version="${gribjump_version:-${GRIBJUMP_VERSION}}"

for arg in "$@"; do
  case "$arg" in
    -f|--filename|-f=*|--filename=*)
      echo "docker/gribjump/skaffold.sh is GribJump-only and always uses ${DEFAULT_CONFIG}; run skaffold directly for other configs." >&2
      exit 2
      ;;
  esac
done

default_repo="$(resolve_default_repo)"
gribjump_tag="${PREFIX:-}gj-$(sanitize_tag_component "${gribjump_version}" 16)"
gribjump_image="gribjump-source-worker-python:${gribjump_tag}"
if [ -n "${default_repo}" ]; then
  gribjump_image="${default_repo}/${gribjump_image}"
fi

printf 'GribJump helper image: %s\n' "${gribjump_image}" >&2

cd "${REPO_DIR}"
exec skaffold -f "${DEFAULT_CONFIG}" "$@"
