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

if ! command -v git >/dev/null 2>&1; then
  echo "git is required" >&2
  exit 1
fi

if ! git lfs version >/dev/null 2>&1; then
  echo "git lfs is required" >&2
  exit 1
fi

cleanup_stale_runs() {
  find /tmp -maxdepth 1 -type d \( -name 'jet-vs-lfs.*' -o -name 'jet-vs-lfs-fast.*' \) -exec rm -rf {} +
}

cleanup_stale_runs
WORK_ROOT="$(mktemp -d /tmp/jet-vs-lfs.XXXXXX)"
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

echo "COMPARE local_vs_lfs"
echo "dataset=${DATASET_DIR}"
echo "work_root=${WORK_ROOT}"
echo "runs=${RUNS}"

cargo build --release --manifest-path "${ROOT_DIR}/Cargo.toml" -p jet-cli >/dev/null

jet_base_copy_runs=()
jet_add1_runs=()
jet_add2_runs=()
jet_commit1_runs=()
jet_commit2_runs=()
jet_open_runs=()
jet_restore_runs=()
jet_store1_runs=()
jet_store2_runs=()
jet_incremental_runs=()
jet_overlay_copy_runs=()
lfs_base_copy_runs=()
lfs_add1_runs=()
lfs_add2_runs=()
lfs_commit1_runs=()
lfs_commit2_runs=()
lfs_checkout_runs=()
lfs_store1_runs=()
lfs_store2_runs=()
lfs_incremental_runs=()
lfs_overlay_copy_runs=()

for run in $(seq 1 "${RUNS}"); do
  JET_REPO="${WORK_ROOT}/jet-${run}"
  mkdir -p "${JET_REPO}"
  echo "JET run=${run}/${RUNS} phase=copy_base"
  jet_base_copy_ms="$(measure_ms "cp -R '${DATASET_DIR}/base/.' '${JET_REPO}/'")"
  echo "JET run=${run}/${RUNS} base_copy_ms=${jet_base_copy_ms}"

  echo "JET run=${run}/${RUNS} phase=init"
  jet_init_ms="$(measure_ms "cd '${JET_REPO}' && '${JET_BIN}' init")"
  echo "JET run=${run}/${RUNS} init_ms=${jet_init_ms}"

  echo "JET run=${run}/${RUNS} phase=add1"
  jet_add1_ms="$(measure_ms "cd '${JET_REPO}' && '${JET_BIN}' add .")"
  echo "JET run=${run}/${RUNS} add1_ms=${jet_add1_ms}"

  echo "JET run=${run}/${RUNS} phase=commit1"
  jet_commit1_ms="$(measure_ms "cd '${JET_REPO}' && '${JET_BIN}' commit -m 'initial' -a smoke")"
  jet_head1="$(cat "${JET_REPO}/.jet/refs/HEAD")"
  jet_store1_kb="$(du -sk "${JET_REPO}/.jet" | awk '{print $1}')"
  echo "JET run=${run}/${RUNS} commit1_ms=${jet_commit1_ms}"
  echo "JET run=${run}/${RUNS} store1_kb=${jet_store1_kb}"

  echo "JET run=${run}/${RUNS} phase=copy_overlay"
  jet_overlay_copy_ms="$(measure_ms "cp -R '${DATASET_DIR}/overlay_mutation/.' '${JET_REPO}/'")"
  echo "JET run=${run}/${RUNS} overlay_copy_ms=${jet_overlay_copy_ms}"

  echo "JET run=${run}/${RUNS} phase=add2"
  jet_add2_ms="$(measure_ms "cd '${JET_REPO}' && '${JET_BIN}' add .")"
  echo "JET run=${run}/${RUNS} add2_ms=${jet_add2_ms}"

  echo "JET run=${run}/${RUNS} phase=commit2"
  jet_commit2_ms="$(measure_ms "cd '${JET_REPO}' && '${JET_BIN}' commit -m 'update' -a smoke")"
  jet_store2_kb="$(du -sk "${JET_REPO}/.jet" | awk '{print $1}')"
  echo "JET run=${run}/${RUNS} commit2_ms=${jet_commit2_ms}"
  echo "JET run=${run}/${RUNS} store2_kb=${jet_store2_kb}"
  echo "JET run=${run}/${RUNS} incremental_kb=$((jet_store2_kb - jet_store1_kb))"

  echo "JET run=${run}/${RUNS} phase=open_restore"
  jet_open_ms="$(measure_ms "cd '${JET_REPO}' && JET_AUTO_HYDRATE_ON_OPEN=0 '${JET_BIN}' open '${jet_head1}'")"
  jet_restore_ms="$(measure_ms "cd '${JET_REPO}' && '${JET_BIN}' hydrate assets/shared/psd audio/clips/group_00")"
  echo "JET run=${run}/${RUNS} open_ms=${jet_open_ms}"
  echo "JET run=${run}/${RUNS} restore_ms=${jet_restore_ms}"

  LFS_REPO="${WORK_ROOT}/lfs-${run}"
  mkdir -p "${LFS_REPO}"
  echo "LFS run=${run}/${RUNS} phase=copy_base"
  lfs_base_copy_ms="$(measure_ms "cp -R '${DATASET_DIR}/base/.' '${LFS_REPO}/'")"
  echo "LFS run=${run}/${RUNS} base_copy_ms=${lfs_base_copy_ms}"

  echo "LFS run=${run}/${RUNS} phase=init"
  ( cd "${LFS_REPO}" && \
    git init -q && \
    git config user.email smoke@example.com && \
    git config user.name smoke && \
    git lfs install --local >/dev/null && \
    git lfs track '*.bin' '*.zip' '*.wav' '*.psd' '*.uasset' '*.png' '*.jpg' '*.tga' >/dev/null )
  echo "LFS run=${run}/${RUNS} init_ms=0"

  echo "LFS run=${run}/${RUNS} phase=add1"
  lfs_add1_ms="$(measure_ms "cd '${LFS_REPO}' && git add .")"
  echo "LFS run=${run}/${RUNS} add1_ms=${lfs_add1_ms}"

  echo "LFS run=${run}/${RUNS} phase=commit1"
  lfs_commit1_ms="$(measure_ms "cd '${LFS_REPO}' && git commit -q -m 'initial'")"
  lfs_head1="$(cd "${LFS_REPO}" && git rev-parse HEAD)"
  lfs_store1_kb="$(du -sk "${LFS_REPO}/.git" | awk '{print $1}')"
  echo "LFS run=${run}/${RUNS} commit1_ms=${lfs_commit1_ms}"
  echo "LFS run=${run}/${RUNS} store1_kb=${lfs_store1_kb}"

  echo "LFS run=${run}/${RUNS} phase=copy_overlay"
  lfs_overlay_copy_ms="$(measure_ms "cp -R '${DATASET_DIR}/overlay_mutation/.' '${LFS_REPO}/'")"
  echo "LFS run=${run}/${RUNS} overlay_copy_ms=${lfs_overlay_copy_ms}"

  echo "LFS run=${run}/${RUNS} phase=add2"
  lfs_add2_ms="$(measure_ms "cd '${LFS_REPO}' && git add .")"
  echo "LFS run=${run}/${RUNS} add2_ms=${lfs_add2_ms}"

  echo "LFS run=${run}/${RUNS} phase=commit2"
  lfs_commit2_ms="$(measure_ms "cd '${LFS_REPO}' && git commit -q -m 'update'")"
  lfs_store2_kb="$(du -sk "${LFS_REPO}/.git" | awk '{print $1}')"
  echo "LFS run=${run}/${RUNS} commit2_ms=${lfs_commit2_ms}"
  echo "LFS run=${run}/${RUNS} store2_kb=${lfs_store2_kb}"
  echo "LFS run=${run}/${RUNS} incremental_kb=$((lfs_store2_kb - lfs_store1_kb))"

  echo "LFS run=${run}/${RUNS} phase=checkout"
  lfs_checkout_ms="$(measure_ms "cd '${LFS_REPO}' && git checkout -q '${lfs_head1}'")"
  echo "LFS run=${run}/${RUNS} checkout_ms=${lfs_checkout_ms}"

  jet_base_copy_runs+=("${jet_base_copy_ms}")
  jet_add1_runs+=("${jet_add1_ms}")
  jet_add2_runs+=("${jet_add2_ms}")
  jet_commit1_runs+=("${jet_commit1_ms}")
  jet_commit2_runs+=("${jet_commit2_ms}")
  jet_open_runs+=("${jet_open_ms}")
  jet_restore_runs+=("${jet_restore_ms}")
  jet_store1_runs+=("${jet_store1_kb}")
  jet_store2_runs+=("${jet_store2_kb}")
  jet_incremental_runs+=("$((jet_store2_kb - jet_store1_kb))")
  jet_overlay_copy_runs+=("${jet_overlay_copy_ms}")
  lfs_base_copy_runs+=("${lfs_base_copy_ms}")
  lfs_add1_runs+=("${lfs_add1_ms}")
  lfs_add2_runs+=("${lfs_add2_ms}")
  lfs_commit1_runs+=("${lfs_commit1_ms}")
  lfs_commit2_runs+=("${lfs_commit2_ms}")
  lfs_checkout_runs+=("${lfs_checkout_ms}")
  lfs_store1_runs+=("${lfs_store1_kb}")
  lfs_store2_runs+=("${lfs_store2_kb}")
  lfs_incremental_runs+=("$((lfs_store2_kb - lfs_store1_kb))")
  lfs_overlay_copy_runs+=("${lfs_overlay_copy_ms}")
done

jet_base_copy_ms="$(median_ms "${jet_base_copy_runs[@]}")"
jet_add1_ms="$(median_ms "${jet_add1_runs[@]}")"
jet_add2_ms="$(median_ms "${jet_add2_runs[@]}")"
jet_commit1_ms="$(median_ms "${jet_commit1_runs[@]}")"
jet_commit2_ms="$(median_ms "${jet_commit2_runs[@]}")"
jet_open_ms="$(median_ms "${jet_open_runs[@]}")"
jet_restore_ms="$(median_ms "${jet_restore_runs[@]}")"
jet_store1_kb="$(median_ms "${jet_store1_runs[@]}")"
jet_store2_kb="$(median_ms "${jet_store2_runs[@]}")"
jet_overlay_copy_ms="$(median_ms "${jet_overlay_copy_runs[@]}")"
jet_incremental_kb="$(median_ms "${jet_incremental_runs[@]}")"
lfs_base_copy_ms="$(median_ms "${lfs_base_copy_runs[@]}")"
lfs_add1_ms="$(median_ms "${lfs_add1_runs[@]}")"
lfs_add2_ms="$(median_ms "${lfs_add2_runs[@]}")"
lfs_commit1_ms="$(median_ms "${lfs_commit1_runs[@]}")"
lfs_commit2_ms="$(median_ms "${lfs_commit2_runs[@]}")"
lfs_checkout_ms="$(median_ms "${lfs_checkout_runs[@]}")"
lfs_store1_kb="$(median_ms "${lfs_store1_runs[@]}")"
lfs_store2_kb="$(median_ms "${lfs_store2_runs[@]}")"
lfs_overlay_copy_ms="$(median_ms "${lfs_overlay_copy_runs[@]}")"
lfs_incremental_kb="$(median_ms "${lfs_incremental_runs[@]}")"

echo "RESULT jet_base_copy_ms=${jet_base_copy_ms}"
echo "RESULT jet_add1_ms=${jet_add1_ms}"
echo "RESULT jet_commit1_ms=${jet_commit1_ms}"
echo "RESULT jet_overlay_copy_ms=${jet_overlay_copy_ms}"
echo "RESULT jet_add2_ms=${jet_add2_ms}"
echo "RESULT jet_commit2_ms=${jet_commit2_ms}"
echo "RESULT jet_open_ms=${jet_open_ms}"
echo "RESULT jet_restore_ms=${jet_restore_ms}"
echo "RESULT jet_store1_kb=${jet_store1_kb}"
echo "RESULT jet_store2_kb=${jet_store2_kb}"
echo "RESULT jet_incremental_kb=${jet_incremental_kb}"
echo "RESULT lfs_base_copy_ms=${lfs_base_copy_ms}"
echo "RESULT lfs_commit1_ms=${lfs_commit1_ms}"
echo "RESULT lfs_add1_ms=${lfs_add1_ms}"
echo "RESULT lfs_overlay_copy_ms=${lfs_overlay_copy_ms}"
echo "RESULT lfs_add2_ms=${lfs_add2_ms}"
echo "RESULT lfs_commit2_ms=${lfs_commit2_ms}"
echo "RESULT lfs_checkout_ms=${lfs_checkout_ms}"
echo "RESULT lfs_store1_kb=${lfs_store1_kb}"
echo "RESULT lfs_store2_kb=${lfs_store2_kb}"
echo "RESULT lfs_incremental_kb=${lfs_incremental_kb}"
echo "RESULT jet_add1_runs_ms=${jet_add1_runs[*]}"
echo "RESULT lfs_add1_runs_ms=${lfs_add1_runs[*]}"
