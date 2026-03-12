#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JET_BIN="${ROOT_DIR}/target/release/jet"
JET_SERVER_BIN="${ROOT_DIR}/target/release/jet-server"
WORK_ROOT="$(mktemp -d "${RUNNER_TEMP:-/tmp}/jet-actions-remote.XXXXXX")"
SERVER_PID=""
TRANSCRIPT_PATH="${WORK_ROOT}/remote-benchmark-transcript.txt"

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

measure_and_capture() {
  local __resultvar="$1"
  local label="$2"
  local workdir="$3"
  shift 3

  local started finished ms output
  output="$(mktemp)"
  started="$(python3 - <<'PY'
import time
print(time.time())
PY
)"

  {
    echo "\$ (cd ${workdir} && $*)"
  } >>"${TRANSCRIPT_PATH}"

  if ! (
    cd "${workdir}"
    "$@"
  ) >"${output}" 2>&1; then
    cat "${output}" >>"${TRANSCRIPT_PATH}"
    rm -f "${output}"
    return 1
  fi

  finished="$(python3 - <<'PY'
import time
print(time.time())
PY
)"
  ms="$(python3 - <<PY
print(int((float("${finished}") - float("${started}")) * 1000))
PY
)"

  cat "${output}" >>"${TRANSCRIPT_PATH}"
  echo >>"${TRANSCRIPT_PATH}"
  rm -f "${output}"
  printf -v "${__resultvar}" '%s' "${ms}"
}

capture_only() {
  local label="$1"
  local workdir="$2"
  shift 2

  {
    echo "\$ (cd ${workdir} && $*)"
    (
      cd "${workdir}"
      "$@"
    )
    echo
  } >>"${TRANSCRIPT_PATH}" 2>&1
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

measure_and_capture init_ms "init" "${SOURCE_REPO}" "${JET_BIN}" init
capture_only "status-after-init" "${SOURCE_REPO}" "${JET_BIN}" status
capture_only "log-before-first-commit" "${SOURCE_REPO}" "${JET_BIN}" log || true
measure_and_capture add1_ms "add1" "${SOURCE_REPO}" "${JET_BIN}" add .
measure_and_capture commit1_ms "commit1" "${SOURCE_REPO}" "${JET_BIN}" commit -m "initial" -a actions
capture_only "status-after-commit1" "${SOURCE_REPO}" "${JET_BIN}" status
capture_only "log-after-commit1" "${SOURCE_REPO}" "${JET_BIN}" log

"${JET_SERVER_BIN}" --listen "127.0.0.1:${PORT}" --repos-root "${REPOS_ROOT}" >/dev/null 2>&1 &
SERVER_PID="$!"
wait_for_server "127.0.0.1" "${PORT}"

remote="http://127.0.0.1:${PORT}/${REPO_NAME}"

measure_and_capture clone_all_ms "clone-all" "${WORK_ROOT}" "${JET_BIN}" clone --all "${remote}" "${CLONE_ALL_ROOT}"
capture_only "clone-all-status" "${CLONE_ALL_ROOT}" "${JET_BIN}" status
capture_only "clone-all-log" "${CLONE_ALL_ROOT}" "${JET_BIN}" log
measure_and_capture clone_partial_ms "clone-partial" "${WORK_ROOT}" "${JET_BIN}" clone --partial "${remote}" "${CLONE_PARTIAL_ROOT}"
capture_only "clone-partial-status" "${CLONE_PARTIAL_ROOT}" "${JET_BIN}" status
capture_only "clone-partial-log" "${CLONE_PARTIAL_ROOT}" "${JET_BIN}" log

printf '\n// source update\n' >> "${SOURCE_REPO}/code/main.rs"
write_payload "${SOURCE_REPO}/assets/blob-01.bin" "${FILE_SIZE_MB}"
measure_and_capture add2_ms "add2" "${SOURCE_REPO}" "${JET_BIN}" add .
measure_and_capture commit2_ms "commit2" "${SOURCE_REPO}" "${JET_BIN}" commit -m "source-update" -a actions
capture_only "source-log-after-update" "${SOURCE_REPO}" "${JET_BIN}" log

measure_and_capture pull_ms "pull" "${CLONE_ALL_ROOT}" "${JET_BIN}" pull
capture_only "clone-all-status-after-pull" "${CLONE_ALL_ROOT}" "${JET_BIN}" status
capture_only "clone-all-log-after-pull" "${CLONE_ALL_ROOT}" "${JET_BIN}" log

printf '\n// clone update\n' >> "${CLONE_ALL_ROOT}/code/main.rs"
measure_and_capture add3_ms "add3-small" "${CLONE_ALL_ROOT}" "${JET_BIN}" add code/main.rs
measure_and_capture commit3_ms "commit3-small" "${CLONE_ALL_ROOT}" "${JET_BIN}" commit -m "clone-update" -a actions
measure_and_capture push_small_ms "push-small" "${CLONE_ALL_ROOT}" "${JET_BIN}" push

write_payload "${CLONE_ALL_ROOT}/assets/blob-01.bin" "${FILE_SIZE_MB}"
measure_and_capture add4_ms "add4-large" "${CLONE_ALL_ROOT}" "${JET_BIN}" add assets/blob-01.bin
measure_and_capture commit4_ms "commit4-large" "${CLONE_ALL_ROOT}" "${JET_BIN}" commit -m "large-file-update" -a actions
measure_and_capture push_large_ms "push-large" "${CLONE_ALL_ROOT}" "${JET_BIN}" push
capture_only "clone-all-status-after-push-large" "${CLONE_ALL_ROOT}" "${JET_BIN}" status
capture_only "clone-all-log-after-push-large" "${CLONE_ALL_ROOT}" "${JET_BIN}" log

status_all="$(cd "${CLONE_ALL_ROOT}" && "${JET_BIN}" status | tr '\n' ';' | sed 's/;$/ /')"
status_partial="$(cd "${CLONE_PARTIAL_ROOT}" && "${JET_BIN}" status | tr '\n' ';' | sed 's/;$/ /')"
total_bytes="$((FILE_COUNT * FILE_SIZE_MB * 1024 * 1024))"

echo "FLOW remote-actions"
echo "repo=${REPO_NAME}"
echo "file_count=${FILE_COUNT}"
echo "file_size_mb=${FILE_SIZE_MB}"
echo "dataset_bytes=${total_bytes}"
echo "init_ms=${init_ms}"
echo "add1_ms=${add1_ms}"
echo "commit1_ms=${commit1_ms}"
echo "clone_all_ms=${clone_all_ms}"
echo "clone_partial_ms=${clone_partial_ms}"
echo "add2_ms=${add2_ms}"
echo "commit2_ms=${commit2_ms}"
echo "pull_ms=${pull_ms}"
echo "add3_ms=${add3_ms}"
echo "commit3_ms=${commit3_ms}"
echo "push_small_ms=${push_small_ms}"
echo "add4_ms=${add4_ms}"
echo "commit4_ms=${commit4_ms}"
echo "push_large_ms=${push_large_ms}"
echo "clone_all_status=$(echo "${status_all}")"
echo "clone_partial_status=$(echo "${status_partial}")"
cp "${TRANSCRIPT_PATH}" "${ROOT_DIR}/remote-benchmark-transcript.txt"
