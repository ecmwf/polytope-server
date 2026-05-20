#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DEFAULT_CONFIG="${SCRIPT_DIR}/skaffold.yaml"

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

cd "${REPO_DIR}"
exec skaffold -f "${DEFAULT_CONFIG}" "$@"
