# Phase 172 D4 Materialization Lifecycle QA

Validate concurrency, recovery, lifecycle, and replication equivalence with
shared-record materialization actually enabled in the candidate fixtures.

## Source And Command

Use the exact assigned commit from a clean Linux worktree:

```bash
bash scripts/run-phase172-materialization-lifecycle-gate.sh
```

Do not reduce the 20 candidate repetitions, 20 CGO race repetitions, or 10
existing-equivalence repetitions.

## Required Evidence

```text
phase172_materialization_lifecycle_status=ok
candidate_repeat_20=pass
candidate_race_repeat_20=pass
existing_equivalence_repeat_10=pass
large_snapshot_concurrent_write_batch=pass
direct_base_overlap=pass
recycle_floor_partial_batch_recovery=pass
close_final_flush_and_failure_recovery=pass
overflow_batch_recovery_fails_closed=pass
legacy_wrapped_retained_window_reconstructed=pass
checkpoint_crash_windows=pass
checkpoint_sigkill_windows=pass
sync_close_lifecycle=pass
scan_lbas_concurrent_live_write=pass
recycle_pin_contract=pass
storage_regression=pass
recovery_regression=pass
replication_regression=pass
replication_component_regression=pass
lifecycle_vet=pass
rf1_local_storage_contract=pass
rf3_sync_quorum_component_contract=pass
checkpoint_tail_dirty_consistency=pass
candidate_disk_format_unchanged=true
recovery_branch_added=false
external_selector_added=false
d5_performance_gate_eligible=true
```

Review the large-snapshot fixture rather than accepting only the terminal key:
the first candidate cycle must process 288 entries from 48 physical records,
retain 10 newer ordinary/batch overwrites, and checkpoint only the old frontier.
The second cycle must publish those 10 entries and finish with no dirty state.

The recycle-floor fixture is a regression for a D4 live finding. A checkpoint
at LSN 8 inside a physical batch spanning LSN 1 through 16 must retain the WAL,
reopen at frontier 16, replay only LSN 9 through 16 into the dirty map, then
materialize that suffix with one physical read and seven reuse hits. A malformed
batch whose `Reserved * blockSize` would overflow must not publish any recovery
frontier.

The legacy wrapped-window fixture must force persisted byte head/tail to zero
while retaining an older high-offset record and newer offset-zero batch.
Recovery must find both, reconstruct `tail=highOffset` and
`head=WALSize+lowEnd`, and append the next record at `lowEnd` without
overwriting either retained record.

Confirm that direct BASE remains authoritative, Close failure keeps checkpoint
and physical tail unchanged, and the existing SIGKILL, recovery, catch-up,
rebuild, and RF3 sync-quorum suites remain green. No new disk format, recovery
branch, or external selector is allowed.

## Deliverable

Write:

```text
internal/docs/qa-assignments/phase172-d4-materialization-lifecycle-qa-signoff.md
```

Include exact commit, all terminal evidence, artifact SHA-256, findings, and
cleanup. Do not patch product or gate code during QA.
