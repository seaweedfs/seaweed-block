# Phase 171 D1 WALStore Checkpoint Baseline QA

Validate the complete default `walstore` checkpoint pipeline before any
coalesced extent-write candidate is implemented.

## Source

Run from the exact assigned commit in a clean worktree. Record the commit,
dirty state, Linux kernel, Go version, and artifact checksum.

## Command

```bash
bash scripts/run-phase171-checkpoint-correctness-gate.sh
bash scripts/run-phase171-walstore-checkpoint-baseline-gate.sh
```

Do not reduce the five repetitions, one-second benchmark time, writer matrix,
normal 100 ms background flusher cadence, final Sync, or final drain.

## Required Evidence

The correctness gate must be green first:

```text
phase171_checkpoint_correctness_status=ok
focused_storage_tests=pass
sigkill_crash_windows=pass
storage_race=pass
storage_regression=pass
storage_vet=pass
```

The baseline gate must then report:

```text
phase171_walstore_checkpoint_baseline_status=ok
storage_regression=pass
all_samples_checkpoint_coverage_complete=true
all_samples_failed_cycles_zero=true
d2_bounded_extent_candidate_admitted=<true|false>
next_recommendation=<implement_disabled_bounded_extent_writeback|stop_before_bounded_extent_writeback>
```

Inspect the full summary and logs. Confirm:

- sequential 4 KiB, scattered 4 KiB, and contiguous 16-block batch workloads
  each ran with 1/2/4/8 writers for five repetitions;
- foreground, final Sync, and final drain durations are separate;
- every sample has one final explicit Sync, zero dirty entries, checkpoint
  equal to head, and zero failed flusher/checkpoint operations;
- product counters and `strace` agree directionally on WAL reads, extent
  writes, and fsyncs;
- contiguous-run evidence reports the initial snapshot upper bound separately
  from the successfully written-entry set; D2 uses only the latter's bounded
  minimum, run count, singleton runs, coalescible-entry fraction, and maximum
  run;
- D2 admission is based on stable opportunity in both sequential and
  scattered workloads, not only the explicit batch control.

If the gate stops D2, record it as a valid evidence-based rejection. Do not
weaken thresholds or disable normal checkpoint work.

## Deliverable

Write:

```text
internal/docs/qa-assignments/phase171-d1-walstore-checkpoint-baseline-qa-signoff.md
```

Include the exact commit, summary, profile/strace observations, artifact path
and SHA-256, and cleanup status.
