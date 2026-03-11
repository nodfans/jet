#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JET_BIN="${ROOT_DIR}/target/release/jet"
DATASET_DIR="${1:-${JET_DATASET_DIR:-/Users/joma/Downloads/pan-mixed-bench-dataset}}"
RUNS="${2:-${BENCH_RUNS:-1}}"

if [[ ! -d "${DATASET_DIR}/base" || ! -d "${DATASET_DIR}/overlay_mutation" ]]; then
  echo "dataset must contain base/ and overlay_mutation/: ${DATASET_DIR}" >&2
  exit 1
fi

if ! [[ "${RUNS}" =~ ^[1-9][0-9]*$ ]]; then
  echo "runs must be a positive integer: ${RUNS}" >&2
  exit 1
fi

cleanup_stale_runs() {
  rm -rf /tmp/jet-local-large.*
}

cleanup_stale_runs
WORK_ROOT="$(mktemp -d /tmp/jet-local-large.XXXXXX)"
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

median_ms() {
  python3 - "$@" <<'PY'
import sys
values = sorted(int(v) for v in sys.argv[1:])
print(values[len(values) // 2])
PY
}

cargo build --release --manifest-path "${ROOT_DIR}/Cargo.toml" -p jet-cli >/dev/null

base_copy_runs=()
init_runs=()
add1_runs=()
commit1_runs=()
overlay_copy_runs=()
add2_runs=()
commit2_runs=()
open_runs=()
status_runs=()
hydrate_runs=()
dehydrate_runs=()
log_runs=()
stats_runs=()
fsck_quick_runs=()
fsck_deep_runs=()
last_repo_root=""
last_status_output=""
last_stats_output=""

for run in $(seq 1 "${RUNS}"); do
  REPO_ROOT="${WORK_ROOT}/repo-${run}"
  mkdir -p "${REPO_ROOT}"

  echo "RUN ${run}/${RUNS} phase=copy_base"
  base_copy_ms="$(measure_ms "cp -R '${DATASET_DIR}/base/.' '${REPO_ROOT}/'")"
  echo "RUN ${run}/${RUNS} base_copy_ms=${base_copy_ms}"

  init_ms="$(measure_ms "cd '${REPO_ROOT}' && '${JET_BIN}' init")"
  add1_ms="$(measure_ms "cd '${REPO_ROOT}' && '${JET_BIN}' add .")"
  commit1_ms="$(measure_ms "cd '${REPO_ROOT}' && '${JET_BIN}' commit -m 'initial' -a smoke")"
  head1="$(cat "${REPO_ROOT}/.jet/refs/HEAD")"

  echo "RUN ${run}/${RUNS} phase=copy_overlay"
  overlay_copy_ms="$(measure_ms "cp -R '${DATASET_DIR}/overlay_mutation/.' '${REPO_ROOT}/'")"
  echo "RUN ${run}/${RUNS} overlay_copy_ms=${overlay_copy_ms}"

  add2_ms="$(measure_ms "cd '${REPO_ROOT}' && '${JET_BIN}' add .")"
  commit2_ms="$(measure_ms "cd '${REPO_ROOT}' && '${JET_BIN}' commit -m 'update' -a smoke")"

  open_ms="$(measure_ms "cd '${REPO_ROOT}' && JET_AUTO_HYDRATE_ON_OPEN=0 '${JET_BIN}' open '${head1}'")"
  status_ms="$(measure_ms "cd '${REPO_ROOT}' && '${JET_BIN}' status")"
  hydrate_ms="$(measure_ms "cd '${REPO_ROOT}' && '${JET_BIN}' hydrate assets/shared/psd audio/clips/group_00")"
  dehydrate_ms="$(measure_ms "cd '${REPO_ROOT}' && '${JET_BIN}' dehydrate assets/shared/psd audio/clips/group_00")"
  log_ms="$(measure_ms "cd '${REPO_ROOT}' && '${JET_BIN}' log")"
  stats_ms="$(measure_ms "cd '${REPO_ROOT}' && '${JET_BIN}' stats")"
  fsck_quick_ms="$(measure_ms "cd '${REPO_ROOT}' && '${JET_BIN}' fsck")"
  fsck_deep_ms="$(measure_ms "cd '${REPO_ROOT}' && '${JET_BIN}' fsck --deep")"

  echo "RUN ${run}/${RUNS} init_ms=${init_ms} add1_ms=${add1_ms} commit1_ms=${commit1_ms} add2_ms=${add2_ms} commit2_ms=${commit2_ms} open_ms=${open_ms} status_ms=${status_ms} hydrate_ms=${hydrate_ms} dehydrate_ms=${dehydrate_ms} log_ms=${log_ms} stats_ms=${stats_ms} fsck_quick_ms=${fsck_quick_ms} fsck_deep_ms=${fsck_deep_ms}"

  base_copy_runs+=("${base_copy_ms}")
  init_runs+=("${init_ms}")
  add1_runs+=("${add1_ms}")
  commit1_runs+=("${commit1_ms}")
  overlay_copy_runs+=("${overlay_copy_ms}")
  add2_runs+=("${add2_ms}")
  commit2_runs+=("${commit2_ms}")
  open_runs+=("${open_ms}")
  status_runs+=("${status_ms}")
  hydrate_runs+=("${hydrate_ms}")
  dehydrate_runs+=("${dehydrate_ms}")
  log_runs+=("${log_ms}")
  stats_runs+=("${stats_ms}")
  fsck_quick_runs+=("${fsck_quick_ms}")
  fsck_deep_runs+=("${fsck_deep_ms}")

  last_repo_root="${REPO_ROOT}"
  last_status_output="$(cd "${REPO_ROOT}" && "${JET_BIN}" status)"
  last_stats_output="$(cd "${REPO_ROOT}" && "${JET_BIN}" stats)"
done

base_copy_ms="$(median_ms "${base_copy_runs[@]}")"
init_ms="$(median_ms "${init_runs[@]}")"
add1_ms="$(median_ms "${add1_runs[@]}")"
commit1_ms="$(median_ms "${commit1_runs[@]}")"
overlay_copy_ms="$(median_ms "${overlay_copy_runs[@]}")"
add2_ms="$(median_ms "${add2_runs[@]}")"
commit2_ms="$(median_ms "${commit2_runs[@]}")"
open_ms="$(median_ms "${open_runs[@]}")"
status_ms="$(median_ms "${status_runs[@]}")"
hydrate_ms="$(median_ms "${hydrate_runs[@]}")"
dehydrate_ms="$(median_ms "${dehydrate_runs[@]}")"
log_ms="$(median_ms "${log_runs[@]}")"
stats_ms="$(median_ms "${stats_runs[@]}")"
fsck_quick_ms="$(median_ms "${fsck_quick_runs[@]}")"
fsck_deep_ms="$(median_ms "${fsck_deep_runs[@]}")"

echo "FLOW local-large"
echo "dataset=${DATASET_DIR}"
echo "runs=${RUNS}"
echo "repo_root=${last_repo_root}"
echo "base_copy_ms=${base_copy_ms}"
echo "init_ms=${init_ms}"
echo "add1_ms=${add1_ms}"
echo "commit1_ms=${commit1_ms}"
echo "overlay_copy_ms=${overlay_copy_ms}"
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
echo "base_copy_runs_ms=${base_copy_runs[*]}"
echo "add1_runs_ms=${add1_runs[*]}"
echo "overlay_copy_runs_ms=${overlay_copy_runs[*]}"
echo "add2_runs_ms=${add2_runs[*]}"
echo "status_summary=$(echo "${last_status_output}" | tr '\n' ';' | sed 's/;$/ /')"
echo "stats_summary=$(echo "${last_stats_output}" | tr '\n' ';' | sed 's/;$/ /')"
