# Jet Product Flow Testing

These scripts test the smallest end-to-end product experience rather than isolated storage benchmarks.

## Local flow

Runs:

- `jet init`
- `jet add`
- `jet commit`
- `jet open`
- `jet status`
- `jet hydrate`
- `jet dehydrate`
- `jet log`
- `jet stats`
- `jet fsck`

Command:

```bash
./scripts/product_flow_local.sh
```

Output includes per-command latency plus compact `status` and `stats` summaries.

## Local large-dataset flow

Runs the same local command chain against a dataset with `base/` and
`overlay_mutation/`.

Command:

```bash
./scripts/product_flow_local_large.sh /absolute/path/to/dataset
```

If no path is passed, it defaults to
`/Users/joma/Downloads/pan-mixed-bench-dataset`.
Pass a second argument, or set `BENCH_RUNS`, to repeat the run and report the
median. The script now prints dataset copy timing separately from Jet command
timing so `cp -R` cache noise does not get mistaken for `jet add` regressions.

## Remote flow

Runs:

- `jet-server`
- `jet clone`
- `jet status`
- `jet open`
- `jet hydrate`
- `jet lock`
- `jet locks`
- `jet unlock`
- `jet pull`
- `jet push`

Command:

```bash
./scripts/product_flow_remote.sh
```

Output includes per-command latency and a summary of the final workspace state.
The large remote flow also prints a few lightweight local-state sizes so it is
easier to distinguish transport cost from local workspace bookkeeping:

- `segment_files`
- `workspace_manifest_bytes`
- `materialized_index_bytes`
- `workspace_state_bytes`

## Remote large-dataset flow

Runs the remote flow against a dataset with `base/` and `overlay_mutation/`.
It measures:

- `jet clone`
- `jet hydrate` for a fixed working set
- remote source update + `jet pull`
- `jet open` old commit
- restore the same working set on the old commit
- reopen `HEAD`
- local `jet push`

Command:

```bash
./scripts/product_flow_remote_large.sh /absolute/path/to/dataset
```

If no path is passed, it defaults to
`/Users/joma/Downloads/pan-mixed-bench-dataset`.

## Jet vs LFS local compare

Runs the same local dataset through Jet and Git LFS, printing each phase as it
completes so long LFS steps do not hide earlier results.

Command:

```bash
./scripts/compare_local_lfs.sh /absolute/path/to/dataset
```

Pass a second argument, or set `BENCH_RUNS`, to repeat the comparison and
report median phase timings. The script also prints separate base/overlay copy
times for Jet and LFS.

## Cleanup policy

The product-flow scripts delete stale `/tmp/jet-product-local.*`,
`/tmp/jet-product-remote.*`, `/tmp/jet-product-remote-large.*`, and
`/tmp/jet-local-large.*` directories before each run.
