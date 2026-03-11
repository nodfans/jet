#!/usr/bin/env bash
set -euo pipefail

LISTEN_ADDR="${JET_SERVER_LISTEN:-0.0.0.0:4220}"
REPOS_ROOT="${JET_SERVER_REPOS_ROOT:-$PWD}"
AUTH_CONFIG="${JET_SERVER_AUTH_CONFIG:-}"

cmd=("$(pwd)/target/release/jet-server" "--listen" "$LISTEN_ADDR" "--repos-root" "$REPOS_ROOT")

if [[ -n "$AUTH_CONFIG" ]]; then
  cmd+=("--auth-config" "$AUTH_CONFIG")
fi

exec "${cmd[@]}"
