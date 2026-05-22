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

append_profiles() {
  local raw="$1"
  local part
  IFS=',' read -r -a profile_parts <<< "${raw}"
  for part in "${profile_parts[@]}"; do
    [ -n "${part}" ] && selected_profiles+=("${part}")
  done
}

print_image_ref() {
  local image_name="$1"
  local image_tag="$2"
  if [ -n "${default_repo}" ]; then
    printf '%s/%s:%s\n' "${default_repo}" "${image_name}" "${image_tag}"
  else
    printf '%s:%s\n' "${image_name}" "${image_tag}"
  fi
}

selected_profiles=()
expect_profile_arg=false

for arg in "$@"; do
  if [ "${expect_profile_arg}" = true ]; then
    append_profiles "${arg}"
    expect_profile_arg=false
    continue
  fi

  case "$arg" in
    -f|--filename|-f=*|--filename=*)
      echo "docker/mars-client/skaffold.sh is MARS-only and always uses ${DEFAULT_CONFIG}; run skaffold directly for other configs." >&2
      exit 2
      ;;
    -p|--profile)
      expect_profile_arg=true
      ;;
    -p=*|--profile=*)
      append_profiles "${arg#*=}"
      ;;
  esac
done

if [ "${expect_profile_arg}" = true ]; then
  echo "Missing profile name after -p/--profile." >&2
  exit 2
fi

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

default_repo="$(resolve_default_repo)"
mars_c_ref="${mars_client_c_bundle_ref:-6.34.4.11}"
mars_cpp_ref="${mars_client_cpp_bundle_ref:-7.1.9.1}"

if printf '%s\n' "${selected_profiles[@]:-}" | grep -qx 'mars-c'; then
  mars_c_tag="${PREFIX:-}mc-$(sanitize_tag_component "${mars_c_ref}" 40)"
  printf 'MARS C helper image: %s\n' "$(print_image_ref "mars-base-c" "${mars_c_tag}")" >&2
fi

if printf '%s\n' "${selected_profiles[@]:-}" | grep -qx 'mars-cpp'; then
  mars_cpp_tag="${PREFIX:-}mcpp-$(sanitize_tag_component "${mars_cpp_ref}" 40)"
  printf 'MARS C++ helper image: %s\n' "$(print_image_ref "mars-base-cpp" "${mars_cpp_tag}")" >&2
fi

if [ "${#selected_profiles[@]}" -eq 0 ]; then
  combined_tag="${PREFIX:-}mc-$(sanitize_tag_component "${mars_c_ref}" 24)-mcpp-$(sanitize_tag_component "${mars_cpp_ref}" 24)"
  printf 'MARS helper images:\n  %s\n  %s\n' \
    "$(print_image_ref "mars-base-c" "${combined_tag}")" \
    "$(print_image_ref "mars-base-cpp" "${combined_tag}")" >&2
fi

cd "${REPO_DIR}"
exec skaffold -f "${DEFAULT_CONFIG}" "$@"
