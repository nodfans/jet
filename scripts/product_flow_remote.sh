#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JET_BIN="${ROOT_DIR}/target/release/jet"
JET_SERVER_BIN="${ROOT_DIR}/target/release/jet-server"

cleanup_stale_runs() {
  rm -rf /tmp/jet-product-remote.*
}

cleanup_stale_runs
WORK_ROOT="$(mktemp -d /tmp/jet-product-remote.XXXXXX)"
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

dump_clone_debug() {
  if [[ ! -d "${CLONE_ROOT}" ]]; then
    return
  fi
  echo "REMOTE_FLOW_DEBUG_ROOT=${WORK_ROOT}" >&2
  echo "REMOTE_FLOW_DEBUG_STATUS_BEGIN" >&2
  (
    cd "${CLONE_ROOT}"
    "${JET_BIN}" status
  ) >&2 || true
  echo "REMOTE_FLOW_DEBUG_STATUS_END" >&2
  echo "REMOTE_FLOW_DEBUG_LOG_BEGIN" >&2
  (
    cd "${CLONE_ROOT}"
    "${JET_BIN}" log
  ) >&2 || true
  echo "REMOTE_FLOW_DEBUG_LOG_END" >&2
  echo "REMOTE_FLOW_DEBUG_CODE_DIR_BEGIN" >&2
  ls -la "${CLONE_ROOT}/code" >&2 || true
  echo "REMOTE_FLOW_DEBUG_CODE_DIR_END" >&2
}

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
mkdir -p "${SOURCE_REPO}/code" "${SOURCE_REPO}/assets/cold" "${SOURCE_REPO}/assets/hot"
printf 'fn main() {}\n' > "${SOURCE_REPO}/code/main.rs"
python3 - <<PY
from pathlib import Path
Path("${SOURCE_REPO}/assets/cold/texture.bin").write_bytes(b"y" * (1024 * 1024))
Path("${SOURCE_REPO}/assets/hot/config.json").write_text('{"quality":"high"}\n')
PY

"${JET_BIN}" init >/dev/null 2>&1 || true
(
  cd "${SOURCE_REPO}"
  "${JET_BIN}" init >/dev/null
  "${JET_BIN}" add code assets >/dev/null
  "${JET_BIN}" commit -m "initial" -a smoke >/dev/null
)
head1="$(cat "${SOURCE_REPO}/.jet/refs/HEAD")"

PORT="${JET_SERVER_PORT:-50151}"
"${JET_SERVER_BIN}" --listen "127.0.0.1:${PORT}" --repos-root "${REPOS_ROOT}" >/dev/null 2>&1 &
SERVER_PID="$!"
wait_for_server "127.0.0.1" "${PORT}"

remote="http://127.0.0.1:${PORT}/game"

clone_ms="$(measure_ms "cd '${WORK_ROOT}' && '${JET_BIN}' clone '${remote}' '${CLONE_ROOT}'")"
status1_ms="$(measure_ms "cd '${CLONE_ROOT}' && '${JET_BIN}' status")"
open1_ms="$(measure_ms "cd '${CLONE_ROOT}' && '${JET_BIN}' open '${head1}'")"
hydrate_ms="$(measure_ms "cd '${CLONE_ROOT}' && '${JET_BIN}' hydrate assets/cold")"
lock_ms="$(measure_ms "cd '${CLONE_ROOT}' && '${JET_BIN}' lock '${remote}' assets/cold/texture.bin -o smoke")"
locks_ms="$(measure_ms "cd '${CLONE_ROOT}' && '${JET_BIN}' locks '${remote}' assets")"
unlock_ms="$(measure_ms "cd '${CLONE_ROOT}' && '${JET_BIN}' unlock '${remote}' assets/cold/texture.bin -o smoke")"

printf '\n// remote update\n' >> "${SOURCE_REPO}/code/main.rs"
(
  cd "${SOURCE_REPO}"
  "${JET_BIN}" add code >/dev/null
  "${JET_BIN}" commit -m "source-update" -a smoke >/dev/null
)

pull_ms="$(measure_ms "cd '${CLONE_ROOT}' && '${JET_BIN}' pull '${remote}'")"
reopen_head_ms="$(measure_ms "cd '${CLONE_ROOT}' && '${JET_BIN}' open HEAD")"

if [[ ! -f "${CLONE_ROOT}/code/main.rs" ]]; then
  dump_clone_debug
  echo "REMOTE_FLOW_ERROR=pull/open HEAD did not materialize code/main.rs" >&2
  exit 2
fi

printf '\n// clone update\n' >> "${CLONE_ROOT}/code/main.rs"
(
  cd "${CLONE_ROOT}"
  "${JET_BIN}" add code >/dev/null
  "${JET_BIN}" commit -m "clone-update" -a smoke >/dev/null
)
push_ms="$(measure_ms "cd '${CLONE_ROOT}' && '${JET_BIN}' push '${remote}'")"

status_output="$(cd "${CLONE_ROOT}" && "${JET_BIN}" status)"
stats_output="$(cd "${CLONE_ROOT}" && "${JET_BIN}" stats)"

echo "FLOW remote"
echo "repos_root=${REPOS_ROOT}"
echo "clone_ms=${clone_ms}"
echo "status1_ms=${status1_ms}"
echo "open1_ms=${open1_ms}"
echo "hydrate_ms=${hydrate_ms}"
echo "lock_ms=${lock_ms}"
echo "locks_ms=${locks_ms}"
echo "unlock_ms=${unlock_ms}"
echo "pull_ms=${pull_ms}"
echo "reopen_head_ms=${reopen_head_ms}"
echo "push_ms=${push_ms}"
echo "status_summary=$(echo "${status_output}" | tr '\n' ';' | sed 's/;$/ /')"
echo "stats_summary=$(echo "${stats_output}" | tr '\n' ';' | sed 's/;$/ /')"
