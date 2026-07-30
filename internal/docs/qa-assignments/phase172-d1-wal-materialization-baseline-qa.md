# Phase 172 D1 WAL Materialization Baseline QA

Validate exact in-memory WAL record geometry and reproduce the shipped
flusher's duplicate-read shape before D2 changes materialization behavior.

## Source

Run the exact assigned commit from a clean Linux worktree. Record the full
commit, dirty state, kernel, Go version, artifact path, and archive SHA-256.
Do not use a shared development tree.

## Command

```bash
bash scripts/run-phase172-wal-materialization-baseline-gate.sh
```

Do not reduce the five repetitions, one-second benchmark time, normal
checkpoint behavior, final Sync, final drain, 20 geometry repetitions, or
1024-record scoped syscall probe.

## Required Evidence

The gate must report:

```text
phase172_wal_materialization_baseline_status=ok
storage_regression=pass
record_geometry_repeat_20=pass
legacy_trim_recovery_fixture=pass
ring_wrap_geometry_fixture=pass
all_samples_checkpoint_coverage_complete=true
all_samples_failed_cycles_zero=true
sequential_duplicate_read_pass_count=<0..5>
scattered_duplicate_read_pass_count=<0..5>
sequential_wrap_pass_count=<0..5>
strace_available=true
scoped_strace_matches_product_counter=true
d2_single_read_candidate_admitted=<true|false>
next_recommendation=<implement_disabled_single_read_materialization|stop_before_single_read_materialization>
```

Inspect all five samples for sequential 4 KiB, scattered 4 KiB, explicit
16-block batch, and disabled-by-default multi-block opt-in workloads. Confirm:

- ordinary append, independent batch, multi-block append, replicated
  `ApplyEntry`, crash/reopen recovery, legacy trim recovery, and ring wrap all
  carry exact encoded `RecordSize`;
- no disk-format or default materialization selector changed;
- sequential and scattered samples each report one header read plus one full
  record read per validated dirty record;
- at least four of five samples in each ordinary workload satisfy the
  predeclared `[0.95,1.05]`, `[0.95,1.05]`, and `>=1.90` thresholds;
- explicit batch records remain independent, while multi-block snapshots show
  fewer unique records than logical entries and nonzero reuse opportunity;
- actual materialization reuse hits remain zero on the unchanged D1 path;
- all cycles finish, checkpoint equals head, and validation/read/write/sync
  failure counters stay zero;
- `strace -P <exact-store-file>` reports the same `pread64` call count as the
  product's `MaterializationReadOps`; whole-process syscall counts do not
  satisfy this gate;
- D2 admission follows the gate output without changing thresholds after the
  run.

If the gate reports `d2_single_read_candidate_admitted=false`, treat that as a
valid D1 result and stop before D2.

## Additional Linux Checks

```bash
go test -race ./core/storage \
  -run 'Test(DirtyMapSnapshotCarriesRecordGeometry|WALStoreDirtyRecordGeometryAppendPaths|WALStoreRecoverReconstructsRecordGeometry|WALStoreRecoverReconstructsLegacyTrimRecordGeometry|WALStoreRecordGeometrySurvivesRingWrap|FlusherInstrumentationCountsWALRecordMaterializationShape|FlusherInstrumentationExposesCurrentDuplicateReadsForSharedRecord)$' \
  -count=20
go vet ./core/storage
```

## Deliverable

Write:

```text
internal/docs/qa-assignments/phase172-d1-wal-materialization-baseline-qa-signoff.md
```

Include exact summary keys, the scoped product/`strace` read counts, all four
workload medians, artifact checksum, review findings, and cleanup status.
