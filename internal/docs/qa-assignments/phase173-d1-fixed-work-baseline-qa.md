# Phase 173 D1 Fixed-Work Baseline QA

Source: use the commit that adds this assignment and
`scripts/run-phase173-fixed-work-baseline-gate.sh`.

## Purpose

Prove that architecture admission no longer depends on Go benchmark
auto-calibration. The gate executes a fixed logical workload through the
shipped WALStore path and measures foreground writes separately from the one
final `Sync` and complete flusher drain.

This is a measurement gate, not an architecture or performance promotion.

## Run

Run on the Linux storage host and place the store directory on the filesystem
being evaluated:

```bash
cd /path/to/seaweed_block
export SW_BLOCK_ARTIFACT_DIR=/path/to/results/phase173-d1
export SW_BLOCK_PHASE173_STORE_DIR=/path/on/evaluated/filesystem/phase173-d1-stores
bash scripts/run-phase173-fixed-work-baseline-gate.sh "$PWD"
```

Do not lower run counts or change the `1.25x` threshold after seeing results.

## Required Evidence

- `phase173_fixed_work_baseline_status=ok`
- `fixed_work_result_count=64`
- `fixed_work_counter_reconciliation=true`
- `fixed_work_complete_drain=true`
- `fixed_work_correctness_samples=true`
- `four_writer_stability_gate=pass`
- `architecture_candidate_admission_allowed=true`
- every `*_writers_4_set_{1,2}_max_min_ratio` and combined ratio is at most
  `1.25`
- environment evidence identifies kernel, CPU/affinity/scheduler, filesystem,
  mount options, backing device, free space, load, and available thermal data
- `store_source` is the dedicated local block device under evaluation, not the
  OS/root filesystem by convenience and never CIFS/NFS/tmpfs/overlay
- the store directory contains no `phase173-*.store` residue after the gate

Each JSON result must show one final sync, `dirty_entries=0`, and equal
checkpoint/head/synced frontiers. WAL encode operations must equal logical
blocks, commit-lock operations must equal fixed API operations, and physical
WAL append calls must reconcile independently with `WriteAt` calls. A batch
may encode 16 records while coalescing them into one physical append. A WAL
wrap may add one separately counted padding `WriteAt`; `wal_wraps` and
`wal_padding_bytes` make that physical I/O explicit.

## Verdict

- `PASS`: all required evidence is present and both independent five-run
  four-writer sets remain within the predeclared range. Each fixed-work sample
  writes about 64 MiB so foreground execution crosses multiple 100 ms flusher
  periods; 1/2/8 writers are diagnostic matrix points, while four writers use
  the two independent five-run admission sets. The second set reverses shape
  and writer order; the gate syncs between samples and records each set's
  starting device/load evidence.
- `FAIL`: a correctness, counter, drain, or residue invariant fails.
- `HOLD`: the stability range exceeds `1.25x`; fix the harness or lab before
  D2/D3 and do not implement an architecture candidate.
