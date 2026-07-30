# Phase 171 D1 Checkpoint Pipeline QA Sign-off

Verdict: PASS for D1 correctness and baseline gates; D2 admission rejected by
design.

## Source

```text
git_sha=2c54a61559a9bfdb11d2a0e8234348a81ff4fe7d
git_dirty=false
go_version=go1.25.0 linux/amd64
cgo_enabled=1
```

## Correctness

```text
transport_race_repetitions=100
transport_race_status=pass
pillar3_nonrace_repetitions=1000
pillar3_nonrace_status=pass
focused_repetitions=20
focused_status=pass
sigkill_repetitions=20
sigkill_status=pass
race_repetitions=10
race_status=pass
storage_recovery_transport_replication_regression=pass
go_vet_storage=pass
checkpoint_metadata_durable_before_tail_reuse=true
stale_or_corrupt_dirty_record_fails_closed=true
close_lifecycle_fence=true
direct_base_ownership_restart_safe=true
phase171_checkpoint_correctness_status=ok
```

The prior Pillar3 flake was a product boundary defect: the recovery adapter did
not seal the resident WalShipper timer before `BarrierReq`. The fix serializes
foreground append, autonomous drain, barrier cut, and steady-context handoff
under the shipper owner. No race finding, convergence failure, or teardown
hang remained.

## Admission

```text
phase171_walstore_checkpoint_baseline_status=ok
sequential_writers_4_mibps_median=44.54
scattered_writers_4_mibps_median=42.72
batch_writers_4_mibps_median=54.81
sequential_writers_4_extent_write_ops_per_entry_median=1.000
sequential_writers_4_extent_written_min_write_ops_per_entry_median=0.3522
scattered_writers_4_extent_write_ops_per_entry_median=1.000
scattered_writers_4_extent_written_min_write_ops_per_entry_median=0.5547
sequential_opportunity_pass_count=1
scattered_opportunity_pass_count=5
scattered_coalescible_pass_count=5
d2_bounded_extent_candidate_admitted=false
next_recommendation=stop_before_bounded_extent_writeback
```

The scoped qualitative syscall control used
`BenchmarkPhase167WALStoreContention/writers_4` with 67,539 logical writes:

```text
pread64_calls=155282
pwrite64_calls=155304
fsync_calls=44
```

The gate passed as a mechanism and rejected the candidate as a decision.
Sequential bounded-write opportunity met the required threshold in only one
of five samples. D2-D6 were not run and the threshold was not changed after
observing the result.

## Evidence

QA archive SHA256:
`5D6B3E83798E88992A17A719405BD793BBB8A3107761541F543B8611262D5BFE`.

Final cleanup found no block residue or QA process. The shared worktree was not
modified by QA.
