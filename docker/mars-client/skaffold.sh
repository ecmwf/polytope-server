#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DEFAULT_CONFIG="${SCRIPT_DIR}/skaffold.yaml"

for arg in "$@"; do
  case "$arg" in
    -f|--filename|-f=*|--filename=*)
      echo "docker/mars-client/skaffold.sh is MARS-only and always uses ${DEFAULT_CONFIG}; run skaffold directly for other configs." >&2
      exit 2
      ;;
  esac
done

if [ -z "${GITHUB_TOKEN:-}" ]; then
  if ! command -v gh >/dev/null 2>&1; then
    echo "GITHUB_TOKEN is unset and gh is not available to fetch one." >&2
    exit 1
  fi
  GITHUB_TOKEN="$(gh auth token)"
  export GITHUB_TOKEN
  if [ -z "${GITHUB_TOKEN}" ]; then
    echo "gh auth token returned an empty token." >&2
    exit 1
  fi
fi

cd "${REPO_DIR}"
exec skaffold -f "${DEFAULT_CONFIG}" "$@"
