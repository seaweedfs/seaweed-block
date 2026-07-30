# Phase 172 D4 Materialization Lifecycle QA Sign-off

Verdict: **PASS**

The committed Phase 172 D4 gate passed on the exact assigned commit in a clean,
isolated Linux worktree. No product, gate, or lab blocker was found.

## Source And Environment

```text
commit=1cf3cbcc311aae4bb145b699c5d333e9c66160ca
branch_at_assignment=phase172-wal-materialization-pipeline
git_dirty=false
host=m02
goos=linux
goarch=amd64
go_version=go1.25.0
kernel=Linux 6.17.0-23-generic x86_64
cgo_enabled=1
cc=gcc
gate_sha256=2a775f879c8dcaaf6c228e423bd791a479ec9a62bd55199804c0285912cc4115
```

The exact command was:

```bash
SW_BLOCK_ARTIFACT_DIR=/mnt/smb/work/share/g15d-k8s/20260730-qa-phase172-d4-1cf3cbc-a \
  bash scripts/run-phase172-materialization-lifecycle-gate.sh \
  /tmp/20260730-qa-phase172-d4-1cf3cbc-a-worktree
```

The gate exited `0`. The committed `go test -race` command ran with CGO
enabled. Log auditing found no `SKIP`, `no tests to run`, race warning, or test
failure.

## Repetition Audit

```text
candidate_parent_pass_count=120
candidate_repeat=6 tests x 20
candidate_race_parent_pass_count=120
candidate_race_repeat=6 tests x 20
existing_equivalence_parent_pass_count=70
existing_equivalence_repeat=7 tests x 10
forbidden_skip_no_tests_race_fail_count=0
sigkill_process_kill_source_count=1
```

`TestWALStoreCheckpointSIGKILLCrashWindows` ran 10 times from its Linux-only
test file and invokes `process.Kill()`.

## Lifecycle Evidence

The passing candidate fixtures enforce these exact values:

```text
large_initial_logical_entries=288
large_initial_physical_records=48
large_first_checkpoint=288
large_retained_newer_entries=10
large_final_dirty_entries=0
large_total_cycles=2
large_total_snapshot_entries=298
large_total_unique_records=51
large_total_reads=51
large_total_reuse_hits=247
large_superseded_entries=10

partial_batch_checkpoint=8
partial_batch_physical_tail_under_pin=0
partial_batch_recovered_frontier=16
partial_batch_replayed_suffix_entries=8
partial_batch_post_recovery_append_at_reconstructed_head=true
partial_batch_physical_reads_with_post_recovery_append=2
partial_batch_reuse_hits=7

integrity_error_type=ErrWALIntegrityFault
malformed_cases=reserved_multiplication_overflow,lsn_range_overflow,length_mismatch
malformed_recovery_frontier=0
malformed_dirty_entries=0

legacy_persisted_head=0
legacy_persisted_tail=0
legacy_recovered_frontier=6
legacy_reconstructed_tail=high_record_offset
legacy_reconstructed_head=wal_size_plus_low_record_end
legacy_append_at_low_record_end=true
legacy_final_tail_equals_head=true
```

The large fixture creates 16 multi-block records plus 32 ordinary records,
then retains ten concurrent overwrites after the first cycle. The second cycle
publishes those ten entries and leaves no dirty state.

The partial-batch fixture checkpoints at LSN 8 inside a physical LSN 1..16
batch, reopens at frontier 16, retains only LSN 9..16, appends after the
reconstructed byte head, and materializes the retained batch with one physical
read and seven reuse hits. The reported total of two reads includes the new
ordinary post-recovery append.

Malformed persisted batches fail closed with the typed
`ErrWALIntegrityFault`; recovery publishes no frontier or dirty entry. The
legacy wrap fixture reconstructs the retained high/low physical window,
appends at the low record end without overwriting retained data, and finishes
with physical `tail=head`.

Direct BASE authority, checkpoint-write failure retention, final Close flush,
checkpoint crash windows, Linux SIGKILL windows, concurrent ScanLBAs, recycle
pinning, recovery, replication, replication component, and RF3 sync-quorum
equivalence all remained green.

## Full Gate Summary

```text
phase172_materialization_lifecycle_status=running
git_sha=1cf3cbcc311aae4bb145b699c5d333e9c66160ca
git_dirty=false
go_version=go_version_go1.25.0_linux/amd64
kernel=Linux_6.17.0-23-generic_#23~24.04.1-Ubuntu_SMP_PREEMPT_DYNAMIC_Tue_Apr_14_16:11:48_UTC_2_x86_64_GNU/Linux
TestWALStoreSharedMaterializationLargeConcurrentLifecycle=pass
TestWALStoreSharedMaterializationCannotOverwriteDirectBase=pass
TestWALStoreSharedMaterializationRespectsRecycleFloorAcrossRestart=pass
TestWALStoreSharedMaterializationCloseLifecycle=pass
TestWALStoreRecoverRejectsOverflowingBatchGeometry=pass
TestWALStoreRecoverReconstructsLegacyWrappedRetainedWindow=pass
candidate_repeat_20=pass
large_snapshot_concurrent_write_batch=pass
direct_base_overlap=pass
recycle_floor_partial_batch_recovery=pass
close_final_flush_and_failure_recovery=pass
overflow_batch_recovery_fails_closed=pass
legacy_wrapped_retained_window_reconstructed=pass
candidate_race_repeat_20=pass
TestCheckpointPublicationCrashWindowsRemainRecoverable=pass
TestWALStoreCheckpointSIGKILLCrashWindows=pass
TestWALStoreCloseWaitsForInflightSync=pass
TestWALStoreCloseReturnsFinalMetadataFailure=pass
TestRunningFlusherCannotOverwriteDirectBase=pass
TestWalstoreRecovery_ScanLBAs_ConcurrentLiveWrite_Safe=pass
TestWALStore_RecycleGate_SourceActive_ClampsAtFloor=pass
existing_equivalence_repeat_10=pass
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
phase172_materialization_lifecycle_status=ok
```

## Artifact And Cleanup

```text
artifact=/mnt/smb/work/share/g15d-k8s/20260730-qa-phase172-d4-1cf3cbc-a.tar.gz
artifact_sha256=081aca79d69372ff1c610e44d8463872d5d414bf175360fe5f4874cc41a35e47
artifact_sha256_recheck=ok
m02_temp_worktree_removed=true
m02_temp_bundle_removed=true
local_temp_bundle_removed=true
cluster_swblock_crd_residue=0
cluster_swblock_workload_residue=0
shared_windows_tree_touched=false
```

The artifact contains the full gate summary, candidate/race/equivalence logs,
all regression and vet logs, the post-gate repetition audit, and the exact
source assertions used for the lifecycle evidence review.

Phase 172 D4 may close. `d5_performance_gate_eligible=true`.
