# Phase 173 D2 Shipped-Path Attribution QA

Source: use the commit that adds this assignment and
`scripts/run-phase173-shipped-path-attribution-gate.sh`.

## Purpose

Measure the shipped WALStore foreground and checkpoint path with fixed work,
then reconcile product counters against independent operating-system evidence.
This gate adds diagnostic instrumentation only. It does not implement, select,
or admit an architecture candidate.

The current gate covers the local WALStore engine/checkpoint path. Frontend and
replication comparisons remain D3 diagnostic controls and must not be inferred
from this engine result.

## Run

Run on the Linux storage host. The store directory must resolve to the local
block device being evaluated, not the OS root filesystem or a network share:

```bash
cd /path/to/seaweed_block
export SW_BLOCK_ARTIFACT_DIR=/path/to/results/phase173-d2
export SW_BLOCK_PHASE173_STORE_DIR=/path/on/evaluated/filesystem/phase173-d2-stores
bash scripts/run-phase173-shipped-path-attribution-gate.sh "$PWD"
```

The account must have non-interactive sudo for `strace`, `perf stat`, and
signals sent to those root-owned tools. The gate does not need Kubernetes.

## Required Evidence

- `phase173_shipped_path_attribution_status=ok`
- `scope=walstore_engine_checkpoint_path`
- `fixed_work_runs_reconciled=4`
- `product_counter_reconciliation=true`
- `strace_scope_exact=true`
- `strace_product_counter_reconciliation=true`
- `perf_scope_exact=true`
- `perf_required_events_present=true`
- `profile_scope_exact=true`
- `iostat_device_observed=true`
- `checkpoint_frontiers_equal=true`
- `complete_drain=true`
- `store_residue_count=0`
- `architecture_candidate_selected=false`
- `optimization_code_present=false`

Inspect the summary's operation, byte, and nanosecond values. Do not report
percentages alone. `wal_encode_ns` contains its nested copy and checksum work;
do not add those three values as if they were independent stages.

The exact strace run must show:

```text
pread64 = flusher header reads + flusher record reads + correctness reads
pwrite64 = WAL WriteAt calls + extent writes + checkpoint writes
fsync/fdatasync = extent syncs + checkpoint syncs + the one final Sync
```

The test blocks immediately after warmup. The gate attaches `strace` or `perf`,
starts the measured foreground work, waits through final Sync, complete drain,
and correctness reads, detaches the tool, and only then permits `Close`. Store
create/recover/warmup and final-close metadata I/O are therefore outside the
exact attribution window.

## Verdict

- `PASS`: all product counters reconcile across the four independent runs,
  exact strace counts match the traced run, required perf events and profiles
  are present, iostat names the evaluated device, and cleanup is complete.
- `FAIL`: logical work, bytes, syscall counts, frontiers, profiles, perf, or
  cleanup do not reconcile.
- `HOLD`: required Linux tools, sudo access, or a dedicated local block-device
  store are unavailable. Do not substitute inferred or whole-process evidence.
