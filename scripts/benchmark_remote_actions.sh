#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JET_BIN="${ROOT_DIR}/target/release/jet"
JET_SERVER_BIN="${ROOT_DIR}/target/release/jet-server"
WORK_ROOT="$(mktemp -d "${RUNNER_TEMP:-/tmp}/jet-actions-remote.XXXXXX")"
SERVER_PID=""

REPO_NAME="${JET_REPO_NAME:-game}"
FILE_COUNT="${JET_FILE_COUNT:-4}"
FILE_SIZE_MB="${JET_FILE_SIZE_MB:-64}"
PORT="${JET_SERVER_PORT:-4220}"

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
deadline = time.time() + 15
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

write_payload() {
  local path="$1"
  local size_mb="$2"
  python3 - "$path" "$size_mb" <<'PY'
from pathlib import Path
import os
import sys

path = Path(sys.argv[1])
size_mb = int(sys.argv[2])
path.parent.mkdir(parents=True, exist_ok=True)
chunk = os.urandom(1024 * 1024)
with path.open("wb") as fh:
    for _ in range(size_mb):
        fh.write(chunk)
PY
}

cargo build --release --manifest-path "${ROOT_DIR}/Cargo.toml" -p jet-cli -p jet-server >/dev/null

REPOS_ROOT="${WORK_ROOT}/repos"
SOURCE_REPO="${REPOS_ROOT}/${REPO_NAME}"
CLONE_ALL_ROOT="${WORK_ROOT}/clone-all"
CLONE_PARTIAL_ROOT="${WORK_ROOT}/clone-partial"
mkdir -p "${SOURCE_REPO}" "${CLONE_ALL_ROOT}" "${CLONE_PARTIAL_ROOT}"
mkdir -p "${SOURCE_REPO}/code"

for index in $(seq 1 "${FILE_COUNT}"); do
  write_payload "${SOURCE_REPO}/assets/blob-$(printf '%02d' "${index}").bin" "${FILE_SIZE_MB}"
done
printf 'fn main() { println!("bench"); }\n' > "${SOURCE_REPO}/code/main.rs"

(
  cd "${SOURCE_REPO}"
  "${JET_BIN}" init >/dev/null
  "${JET_BIN}" add . >/dev/null
  "${JET_BIN}" commit -m "initial" -a actions >/dev/null
)

"${JET_SERVER_BIN}" --listen "127.0.0.1:${PORT}" --repos-root "${REPOS_ROOT}" >/dev/null 2>&1 &
SERVER_PID="$!"
wait_for_server "127.0.0.1" "${PORT}"

remote="http://127.0.0.1:${PORT}/${REPO_NAME}"

clone_all_ms="$(measure_ms "cd '${WORK_ROOT}' && '${JET_BIN}' clone --all '${remote}' '${CLONE_ALL_ROOT}'")"
clone_partial_ms="$(measure_ms "cd '${WORK_ROOT}' && '${JET_BIN}' clone --partial '${remote}' '${CLONE_PARTIAL_ROOT}'")"

printf '\n// source update\n' >> "${SOURCE_REPO}/code/main.rs"
write_payload "${SOURCE_REPO}/assets/blob-01.bin" "${FILE_SIZE_MB}"
(
  cd "${SOURCE_REPO}"
  "${JET_BIN}" add . >/dev/null
  "${JET_BIN}" commit -m "source-update" -a actions >/dev/null
)

pull_ms="$(measure_ms "cd '${CLONE_ALL_ROOT}' && '${JET_BIN}' pull")"

printf '\n// clone update\n' >> "${CLONE_ALL_ROOT}/code/main.rs"
(
  cd "${CLONE_ALL_ROOT}"
  "${JET_BIN}" add code/main.rs >/dev/null
  "${JET_BIN}" commit -m "clone-update" -a actions >/dev/null
)

push_ms="$(measure_ms "cd '${CLONE_ALL_ROOT}' && '${JET_BIN}' push")"

status_all="$(cd "${CLONE_ALL_ROOT}" && "${JET_BIN}" status | tr '\n' ';' | sed 's/;$/ /')"
status_partial="$(cd "${CLONE_PARTIAL_ROOT}" && "${JET_BIN}" status | tr '\n' ';' | sed 's/;$/ /')"
total_bytes="$((FILE_COUNT * FILE_SIZE_MB * 1024 * 1024))"

echo "FLOW remote-actions"
echo "repo=${REPO_NAME}"
echo "file_count=${FILE_COUNT}"
echo "file_size_mb=${FILE_SIZE_MB}"
echo "dataset_bytes=${total_bytes}"
echo "clone_all_ms=${clone_all_ms}"
echo "clone_partial_ms=${clone_partial_ms}"
echo "pull_ms=${pull_ms}"
echo "push_ms=${push_ms}"
echo "clone_all_status=$(echo "${status_all}")"
echo "clone_partial_status=$(echo "${status_partial}")"
