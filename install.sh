#!/usr/bin/env bash
set -euo pipefail

REPO_SLUG="${JET_REPO_SLUG:-nodfans/jet}"
REF="${JET_REF:-main}"
ARCHIVE_URL="${JET_ARCHIVE_URL:-https://codeload.github.com/${REPO_SLUG}/tar.gz/${REF}}"
WORK_ROOT=""
PASS_ARGS=()

usage() {
  cat <<EOF
Usage: bash install.sh [installer options...]

Downloads the Jet source archive from GitHub and runs the repo installer.

Defaults:
  repo: ${REPO_SLUG}
  ref:  ${REF}

Environment:
  JET_REPO_SLUG    Override GitHub repo slug
  JET_REF          Override branch, tag, or commit-ish
  JET_ARCHIVE_URL  Override full archive URL

Passed through to the repo installer:
  --copy
  --install-dir DIR
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    *)
      PASS_ARGS+=("$1")
      shift
      ;;
  esac
done

if command -v curl >/dev/null 2>&1; then
  FETCH_CMD=(curl -fsSL "${ARCHIVE_URL}")
elif command -v wget >/dev/null 2>&1; then
  FETCH_CMD=(wget -qO- "${ARCHIVE_URL}")
else
  echo "curl or wget is required" >&2
  exit 1
fi

if ! command -v tar >/dev/null 2>&1; then
  echo "tar is required" >&2
  exit 1
fi

WORK_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/jet-install.XXXXXX")"
trap 'rm -rf "${WORK_ROOT}"' EXIT

echo "Downloading ${REPO_SLUG}@${REF}..."
"${FETCH_CMD[@]}" | tar -xzf - -C "${WORK_ROOT}"

REPO_ROOT="$(find "${WORK_ROOT}" -mindepth 1 -maxdepth 1 -type d | head -n 1)"
if [[ -z "${REPO_ROOT}" || ! -f "${REPO_ROOT}/scripts/install.sh" ]]; then
  echo "downloaded archive does not contain scripts/install.sh" >&2
  exit 1
fi

bash "${REPO_ROOT}/scripts/install.sh" "${PASS_ARGS[@]}"
