#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JET_BIN="${ROOT_DIR}/target/release/jet"
JET_SERVER_BIN="${ROOT_DIR}/target/release/jet-server"
DATASET_DIR="${1:-${JET_DATASET_DIR:-/Users/joma/Downloads/pan-mixed-bench-dataset}}"
WORKING_SET_PATHS=("assets/shared/psd" "audio/clips/group_00")

if [[ ! -d "${DATASET_DIR}/base" || ! -d "${DATASET_DIR}/overlay_mutation" ]]; then
  echo "dataset must contain base/ and overlay_mutation/: ${DATASET_DIR}" >&2
  exit 1
fi

cleanup_stale_runs() {
  rm -rf /tmp/jet-product-remote-large.*
}

cleanup_stale_runs
WORK_ROOT="$(mktemp -d /tmp/jet-product-remote-large.XXXXXX)"
SERVER_PID=""

cleanup() {
  if [[ -n "${SERVER_PID}" ]]; then
    kill "${SERVER_PID}" >/dev/null 2>&1 || true
    wait "${SERVER_PID}" >/dev/null 2>&1 || true
  fi
  if [[ "${KEEP_WORK_ROOT:-0}" != "1" ]]; then
    rm -rf "${WORK_ROOT}"
  fi
}
trap cleanup EXIT

wait_for_server() {
  local host="$1"
  local port="$2"
  python3 - <<PY
import socket
import sys
import time

host = "${host}"
port = int("${port}")
deadline = time.time() + 10
last_error = None
while time.time() < deadline:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(0.25)
    try:
        sock.connect((host, port))
    except OSError as exc:
        last_error = exc
        time.sleep(0.1)
    else:
        sock.close()
        sys.exit(0)
    finally:
        sock.close()
print(f"remote server did not become ready on {host}:{port}: {last_error}", file=sys.stderr)
sys.exit(1)
PY
}

measure_ms() {
  local cmd="$1"
  local t
  t="$(mktemp)"
  if ! /usr/bin/time -p bash -lc "$cmd" >/dev/null 2>"$t"; then
    cat "$t" >&2
    rm -f "$t"
    return 1
  fi
  local sec
  sec="$(awk '/^real /{print $2}' "$t")"
  rm -f "$t"
  python3 - <<PY
print(int(float("$sec") * 1000))
PY
}

cargo build --release --manifest-path "${ROOT_DIR}/Cargo.toml" -p jet-cli -p jet-server >/dev/null

REPOS_ROOT="${WORK_ROOT}/repos"
SOURCE_REPO="${REPOS_ROOT}/game"
CLONE_ROOT="${WORK_ROOT}/clone"
mkdir -p "${SOURCE_REPO}" "${CLONE_ROOT}"
cp -R "${DATASET_DIR}/base/." "${SOURCE_REPO}/"

(
  cd "${SOURCE_REPO}"
  "${JET_BIN}" init >/dev/null
  "${JET_BIN}" add . >/dev/null
  "${JET_BIN}" commit -m "initial" -a smoke >/dev/null
)
head1="$(cat "${SOURCE_REPO}/.jet/refs/HEAD")"

PORT="${JET_SERVER_PORT:-50152}"
"${JET_SERVER_BIN}" --listen "127.0.0.1:${PORT}" --repos-root "${REPOS_ROOT}" >/dev/null 2>&1 &
SERVER_PID="$!"
wait_for_server "127.0.0.1" "${PORT}"

remote="http://127.0.0.1:${PORT}/game"
working_set_args="${WORKING_SET_PATHS[*]}"

clone_ms="$(measure_ms "cd '${WORK_ROOT}' && '${JET_BIN}' clone --all '${remote}' '${CLONE_ROOT}'")"
status1_ms="$(measure_ms "cd '${CLONE_ROOT}' && '${JET_BIN}' status")"
hydrate_ms="$(measure_ms "cd '${CLONE_ROOT}' && '${JET_BIN}' hydrate ${working_set_args}")"

cp -R "${DATASET_DIR}/overlay_mutation/." "${SOURCE_REPO}/"
(
  cd "${SOURCE_REPO}"
  "${JET_BIN}" add . >/dev/null
  "${JET_BIN}" commit -m "update" -a smoke >/dev/null
)
head2="$(cat "${SOURCE_REPO}/.jet/refs/HEAD")"

pull_ms="$(measure_ms "cd '${CLONE_ROOT}' && '${JET_BIN}' pull '${remote}'")"
open_old_ms="$(measure_ms "cd '${CLONE_ROOT}' && JET_AUTO_HYDRATE_ON_OPEN=0 '${JET_BIN}' open '${head1}'")"
restore_old_ms="$(measure_ms "cd '${CLONE_ROOT}' && '${JET_BIN}' hydrate ${working_set_args}")"
reopen_head_ms="$(measure_ms "cd '${CLONE_ROOT}' && JET_AUTO_HYDRATE_ON_OPEN=0 '${JET_BIN}' open '${head2}'")"

printf 'remote large flow push\n' > "${CLONE_ROOT}/clone-note.txt"
(
  cd "${CLONE_ROOT}"
  "${JET_BIN}" add clone-note.txt >/dev/null
  "${JET_BIN}" commit -m "clone-note" -a smoke >/dev/null
)
push_ms="$(measure_ms "cd '${CLONE_ROOT}' && '${JET_BIN}' push '${remote}'")"

status_output="$(cd "${CLONE_ROOT}" && "${JET_BIN}" status)"
stats_output="$(cd "${CLONE_ROOT}" && "${JET_BIN}" stats)"
object_count="$(find "${CLONE_ROOT}/.jet/segments" -type f 2>/dev/null | wc -l | tr -d ' ')"
manifest_bytes="$(wc -c < "${CLONE_ROOT}/.jet/workspace-manifest.bin" 2>/dev/null || echo 0)"
materialized_bytes="$(wc -c < "${CLONE_ROOT}/.jet/materialized-index.bin" 2>/dev/null || echo 0)"
workspace_bytes="$(wc -c < "${CLONE_ROOT}/.jet/workspace.bin" 2>/dev/null || echo 0)"

echo "FLOW remote-large"
echo "dataset=${DATASET_DIR}"
echo "repos_root=${REPOS_ROOT}"
echo "clone_ms=${clone_ms}"
echo "status1_ms=${status1_ms}"
echo "hydrate_ms=${hydrate_ms}"
echo "pull_ms=${pull_ms}"
echo "open_old_ms=${open_old_ms}"
echo "restore_old_ms=${restore_old_ms}"
echo "reopen_head_ms=${reopen_head_ms}"
echo "push_ms=${push_ms}"
echo "segment_files=${object_count}"
echo "workspace_manifest_bytes=${manifest_bytes}"
echo "materialized_index_bytes=${materialized_bytes}"
echo "workspace_state_bytes=${workspace_bytes}"
echo "status_summary=$(echo "${status_output}" | tr '\n' ';' | sed 's/;$/ /')"
echo "stats_summary=$(echo "${stats_output}" | tr '\n' ';' | sed 's/;$/ /')"
