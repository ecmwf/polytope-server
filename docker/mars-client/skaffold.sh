#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DEFAULT_CONFIG="${SCRIPT_DIR}/skaffold.yaml"

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

has_profile=false
for arg in "$@"; do
  case "${arg}" in
    -p|--profile|-p=*|--profile=*)
      has_profile=true
      break
      ;;
  esac
done

cd "${REPO_DIR}"

if [ "${has_profile}" = true ]; then
  exec skaffold -f "${DEFAULT_CONFIG}" "$@"
fi

skaffold -f "${DEFAULT_CONFIG}" "$@" -p mars-c
skaffold -f "${DEFAULT_CONFIG}" "$@" -p mars-cpp
