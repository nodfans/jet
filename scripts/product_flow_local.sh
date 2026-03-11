#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JET_BIN="${ROOT_DIR}/target/release/jet"

cleanup_stale_runs() {
  rm -rf /tmp/jet-product-local.*
}

cleanup_stale_runs
WORK_ROOT="$(mktemp -d /tmp/jet-product-local.XXXXXX)"
trap 'rm -rf "${WORK_ROOT}"' EXIT

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

cargo build --release --manifest-path "${ROOT_DIR}/Cargo.toml" -p jet-cli >/dev/null

REPO_ROOT="${WORK_ROOT}/repo"
mkdir -p "${REPO_ROOT}/code" "${REPO_ROOT}/assets/cold"
printf 'fn main() {}\n' > "${REPO_ROOT}/code/main.rs"
python3 - <<PY
from pathlib import Path
Path("${REPO_ROOT}/assets/cold/texture.bin").write_bytes(b"x" * (1024 * 1024))
PY

init_ms="$(measure_ms "cd '${REPO_ROOT}' && '${JET_BIN}' init")"
add1_ms="$(measure_ms "cd '${REPO_ROOT}' && '${JET_BIN}' add code assets")"
commit1_ms="$(measure_ms "cd '${REPO_ROOT}' && '${JET_BIN}' commit -m 'initial' -a smoke")"
head1="$(cat "${REPO_ROOT}/.jet/refs/HEAD")"

printf '\n// local edit\n' >> "${REPO_ROOT}/code/main.rs"
add2_ms="$(measure_ms "cd '${REPO_ROOT}' && '${JET_BIN}' add code")"
commit2_ms="$(measure_ms "cd '${REPO_ROOT}' && '${JET_BIN}' commit -m 'edit' -a smoke")"

cat > "${REPO_ROOT}/.jet/workspace.local.toml" <<'EOF'
[view]
include = ["code/...", "assets/cold/..."]
exclude = ["assets/tmp/..."]
EOF

open_ms="$(measure_ms "cd '${REPO_ROOT}' && JET_AUTO_HYDRATE_ON_OPEN=0 '${JET_BIN}' open '${head1}'")"
status_ms="$(measure_ms "cd '${REPO_ROOT}' && '${JET_BIN}' status")"
hydrate_ms="$(measure_ms "cd '${REPO_ROOT}' && '${JET_BIN}' hydrate assets/cold")"
dehydrate_ms="$(measure_ms "cd '${REPO_ROOT}' && '${JET_BIN}' dehydrate assets/cold")"
log_ms="$(measure_ms "cd '${REPO_ROOT}' && '${JET_BIN}' log")"
stats_ms="$(measure_ms "cd '${REPO_ROOT}' && '${JET_BIN}' stats")"
fsck_quick_ms="$(measure_ms "cd '${REPO_ROOT}' && '${JET_BIN}' fsck")"
fsck_deep_ms="$(measure_ms "cd '${REPO_ROOT}' && '${JET_BIN}' fsck --deep")"

status_output="$(cd "${REPO_ROOT}" && "${JET_BIN}" status)"
stats_output="$(cd "${REPO_ROOT}" && "${JET_BIN}" stats)"

echo "FLOW local"
echo "repo_root=${REPO_ROOT}"
echo "init_ms=${init_ms}"
echo "add1_ms=${add1_ms}"
echo "commit1_ms=${commit1_ms}"
echo "add2_ms=${add2_ms}"
echo "commit2_ms=${commit2_ms}"
echo "open_ms=${open_ms}"
echo "status_ms=${status_ms}"
echo "hydrate_ms=${hydrate_ms}"
echo "dehydrate_ms=${dehydrate_ms}"
echo "log_ms=${log_ms}"
echo "stats_ms=${stats_ms}"
echo "fsck_quick_ms=${fsck_quick_ms}"
echo "fsck_deep_ms=${fsck_deep_ms}"
echo "status_summary=$(echo "${status_output}" | tr '\n' ';' | sed 's/;$/ /')"
echo "stats_summary=$(echo "${stats_output}" | tr '\n' ';' | sed 's/;$/ /')"
