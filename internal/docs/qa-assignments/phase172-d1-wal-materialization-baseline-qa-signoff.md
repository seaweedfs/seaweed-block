# Phase 172 D1 WAL Materialization Baseline QA Sign-off

## Verdict

**PASS**

Exact commit `6dd89e71298b5fa5a46d80c4c27efce7d5bba02f` passed the
unchanged Phase 172 D1 Linux gate, the assigned race test at `-count=20`,
and `go vet ./core/storage`.

The baseline admits D2:

```text
d2_single_read_candidate_admitted=true
next_recommendation=implement_disabled_single_read_materialization
```

## Source And Environment

```text
tested_sha=6dd89e71298b5fa5a46d80c4c27efce7d5bba02f
tested_branch=detached_exact_commit
source_dirty=false
lab_host=M02
go_version=go version go1.25.0 linux/amd64
kernel=Linux 6.17.0-23-generic #23~24.04.1-Ubuntu SMP PREEMPT_DYNAMIC Tue Apr 14 16:11:48 UTC 2 x86_64 GNU/Linux
strace_version=strace -- version 6.8
```

The source was transferred as a Git bundle and checked out detached in the
isolated Linux worktree
`/tmp/phase172-d1-qa-6dd89e7-20260730-050249`. The shared development
worktree was not used or modified.

## Commands

```bash
bash scripts/run-phase172-wal-materialization-baseline-gate.sh

go test -race ./core/storage \
  -run 'Test(DirtyMapSnapshotCarriesRecordGeometry|WALStoreDirtyRecordGeometryAppendPaths|WALStoreRecoverReconstructsRecordGeometry|WALStoreRecoverReconstructsLegacyTrimRecordGeometry|WALStoreRecordGeometrySurvivesRingWrap|FlusherInstrumentationCountsWALRecordMaterializationShape|FlusherInstrumentationExposesCurrentDuplicateReadsForSharedRecord)$' \
  -count=20

go vet ./core/storage
```

The gate ran with its committed defaults: five repetitions, one second per
benchmark, normal checkpoint behavior, final Sync and drain, 20 geometry
repetitions, and 1024 records in the scoped syscall probe.

## Required Gates

| Gate | Evidence | Result |
|---|---|---|
| Storage regression | `storage_regression=pass` | PASS |
| Geometry repeat | `record_geometry_repeat_20=pass` | PASS |
| Legacy range trim | Three blocks recovered, flushed, checkpointed; `legacy_trim_recovery_fixture=pass` | PASS |
| Ring wrap | Wrap, crash, reopen, and recovered identity; `ring_wrap_geometry_fixture=pass` | PASS |
| Checkpoint/failures | 20/20 complete checkpoints; 160/160 failure metrics zero | PASS |
| Duplicate-read baseline | Sequential `5/5`; scattered `5/5` | PASS |
| Current reuse behavior | 20/20 workload samples reported zero actual reuse hits | PASS |
| Exact-path syscall probe | Product `2048`; `strace -P` `2048` | PASS |
| Race | `ok ... core/storage 3.807s`, `-count=20` | PASS |
| Vet | Exit 0, no output | PASS |
| D2 admission | `d2_single_read_candidate_admitted=true` | PASS |

The append/recovery fixtures cover ordinary append, independent explicit
batch records, disabled-by-default multi-block records, replicated
`ApplyEntry`, crash/reopen recovery, a three-block legacy trim, and ring
wrap followed by crash/reopen. The exact `RecordSize` and shared record
identity assertions passed.

The commit changes no WAL/superblock format file and adds no default
materialization selector. The D1 path still performs one header read and one
full-record read per validated record.

## Scoped Strace

The probe used `strace -f -c -e trace=pread64 -P <exact-store-file>` rather
than whole-process syscall counts:

```text
phase172_probe_validated_records=1024
phase172_probe_header_read_ops=1024
phase172_probe_record_read_ops=1024
phase172_probe_materialization_read_ops=2048

% time     seconds  usecs/call     calls    errors syscall
100.00    0.004185           2      2048           pread64
```

Therefore:

```text
scoped_product_materialization_reads=2048
scoped_strace_pread64_calls=2048
scoped_strace_matches_product_counter=true
```

## Workload Medians

| Workload | MiB/s | p99 ns | Unique records/snapshot entry | Reuse opportunities/snapshot entry | Header reads/validated | Record reads/validated | Materialization reads/validated | Read bytes/entry | Actual reuse hits/validated | WAL wraps |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Sequential 4 KiB | 66.11 | 2212380 | 1.000 | 0 | 1.000 | 1.000 | 2.000 | 4172 | 0 | 6.000 |
| Scattered 4 KiB | 63.86 | 2208120 | 1.000 | 0 | 1.000 | 1.000 | 2.000 | 4172 | 0 | 5.000 |
| Explicit 16x4 KiB batch | 77.75 | 5770525 | 1.000 | 0 | 1.000 | 1.000 | 2.000 | 3912 | 0 | 9.000 |
| Multi-block 16x4 KiB opt-in | 53.41 | 5791757 | 0.06250 | 0.9375 | 1.000 | 1.000 | 2.000 | 58565 | 0 | 9.000 |

The explicit batch remained independent. The opt-in multi-block workload had
one unique record per 16 logical entries and nonzero reuse opportunity, while
the unchanged D1 implementation recorded zero actual reuse hits.

## Independent Metric Audit

An independent pass over all raw benchmark rows reported:

```text
benchmark_rows=20
zero_failure_metric_checks=160
checkpoint_checks=20
balanced_cycle_checks=20
zero_actual_reuse_checks=20
duplicate_materialization_checks=20
ordinary_read_shape_checks=10
independent_batch_shape_checks=5
multiblock_reuse_opportunity_checks=5
qa_independent_metric_audit=pass
```

## Full Gate Summary

```text
phase172_wal_materialization_baseline_status=running
git_sha=6dd89e71298b5fa5a46d80c4c27efce7d5bba02f
git_dirty=false
go_version=go_version_go1.25.0_linux/amd64
kernel=Linux_6.17.0-23-generic_#23~24.04.1-Ubuntu_SMP_PREEMPT_DYNAMIC_Tue_Apr_14_16:11:48_UTC_2_x86_64_GNU/Linux
benchmark_time=1s
repetitions=5
writers=4
workloads=sequential_4k,scattered_4k,explicit_16x4k_batch,multiblock_16x4k_opt_in
d2_admission=4_of_5_sequential_and_scattered_samples_each_header_and_record_reads_per_validated_record_in_0.95_to_1.05_and_combined_at_least_1.90
strace_scope=exact_store_file_path
storage_regression=pass
record_geometry_repeat_20=pass
legacy_trim_recovery_fixture=pass
ring_wrap_geometry_fixture=pass
repetition_1_order=sequential scattered batch multiblock
repetition_2_order=multiblock batch scattered sequential
repetition_3_order=scattered sequential multiblock batch
repetition_4_order=batch multiblock sequential scattered
repetition_5_order=sequential batch scattered multiblock
all_samples_checkpoint_coverage_complete=true
all_samples_failed_cycles_zero=true
sequential_mibps_samples=55.66,61.63,67.32,120.64,66.11
sequential_mibps_median=66.11
sequential_p99_ns_samples=2265736,2214159,2212380,1187207,2210448
sequential_p99_ns_median=2212380
sequential_unique_records_per_snapshot_entry_samples=1.000,1.000,1.000,1.000,1.000
sequential_unique_records_per_snapshot_entry_median=1.000
sequential_reuse_opportunities_per_snapshot_entry_samples=0,0,0,0,0
sequential_reuse_opportunities_per_snapshot_entry_median=0
sequential_header_reads_per_validated_record_samples=1.000,1.000,1.000,1.000,1.000
sequential_header_reads_per_validated_record_median=1.000
sequential_record_reads_per_validated_record_samples=1.000,1.000,1.000,1.000,1.000
sequential_record_reads_per_validated_record_median=1.000
sequential_materialization_reads_per_validated_record_samples=2.000,2.000,2.000,2.000,2.000
sequential_materialization_reads_per_validated_record_median=2.000
sequential_materialization_read_bytes_per_entry_samples=4172,4172,4172,4172,4172
sequential_materialization_read_bytes_per_entry_median=4172
sequential_record_reuse_hits_per_validated_record_samples=0,0,0,0,0
sequential_record_reuse_hits_per_validated_record_median=0
sequential_wal_wraps_samples=7.000,6.000,6.000,6.000,6.000
sequential_wal_wraps_median=6.000
scattered_mibps_samples=63.86,66.13,81.54,56.55,43.65
scattered_mibps_median=63.86
scattered_p99_ns_samples=2220691,2208120,2201888,2207386,2337337
scattered_p99_ns_median=2208120
scattered_unique_records_per_snapshot_entry_samples=1.000,1.000,1.000,1.000,1.000
scattered_unique_records_per_snapshot_entry_median=1.000
scattered_reuse_opportunities_per_snapshot_entry_samples=0,0,0,0,0
scattered_reuse_opportunities_per_snapshot_entry_median=0
scattered_header_reads_per_validated_record_samples=1.000,1.000,1.000,1.000,1.000
scattered_header_reads_per_validated_record_median=1.000
scattered_record_reads_per_validated_record_samples=1.000,1.000,1.000,1.000,1.000
scattered_record_reads_per_validated_record_median=1.000
scattered_materialization_reads_per_validated_record_samples=2.000,2.000,2.000,2.000,2.000
scattered_materialization_reads_per_validated_record_median=2.000
scattered_materialization_read_bytes_per_entry_samples=4172,4172,4172,4172,4172
scattered_materialization_read_bytes_per_entry_median=4172
scattered_record_reuse_hits_per_validated_record_samples=0,0,0,0,0
scattered_record_reuse_hits_per_validated_record_median=0
scattered_wal_wraps_samples=5.000,6.000,5.000,5.000,6.000
scattered_wal_wraps_median=5.000
batch_mibps_samples=77.75,88.87,116.70,64.87,66.22
batch_mibps_median=77.75
batch_p99_ns_samples=5816134,5741796,5765574,5770525,5782230
batch_p99_ns_median=5770525
batch_unique_records_per_snapshot_entry_samples=1.000,1.000,1.000,1.000,1.000
batch_unique_records_per_snapshot_entry_median=1.000
batch_reuse_opportunities_per_snapshot_entry_samples=0,0,0,0,0
batch_reuse_opportunities_per_snapshot_entry_median=0
batch_header_reads_per_validated_record_samples=1.000,1.000,1.000,1.000,1.000
batch_header_reads_per_validated_record_median=1.000
batch_record_reads_per_validated_record_samples=1.000,1.000,1.000,1.000,1.000
batch_record_reads_per_validated_record_median=1.000
batch_materialization_reads_per_validated_record_samples=2.000,2.000,2.000,2.000,2.000
batch_materialization_reads_per_validated_record_median=2.000
batch_materialization_read_bytes_per_entry_samples=4122,4112,3658,3730,3912
batch_materialization_read_bytes_per_entry_median=3912
batch_record_reuse_hits_per_validated_record_samples=0,0,0,0,0
batch_record_reuse_hits_per_validated_record_median=0
batch_wal_wraps_samples=9.000,9.000,9.000,9.000,9.000
batch_wal_wraps_median=9.000
multiblock_mibps_samples=54.74,58.98,45.92,49.20,53.41
multiblock_mibps_median=53.41
multiblock_p99_ns_samples=5892553,5731452,5786637,5803821,5791757
multiblock_p99_ns_median=5791757
multiblock_unique_records_per_snapshot_entry_samples=0.06250,0.06250,0.06250,0.06250,0.06250
multiblock_unique_records_per_snapshot_entry_median=0.06250
multiblock_reuse_opportunities_per_snapshot_entry_samples=0.9375,0.9375,0.9375,0.9375,0.9375
multiblock_reuse_opportunities_per_snapshot_entry_median=0.9375
multiblock_header_reads_per_validated_record_samples=1.000,1.000,1.000,1.000,1.000
multiblock_header_reads_per_validated_record_median=1.000
multiblock_record_reads_per_validated_record_samples=1.000,1.000,1.000,1.000,1.000
multiblock_record_reads_per_validated_record_median=1.000
multiblock_materialization_reads_per_validated_record_samples=2.000,2.000,2.000,2.000,2.000
multiblock_materialization_reads_per_validated_record_median=2.000
multiblock_materialization_read_bytes_per_entry_samples=58395,58565,56312,64007,65147
multiblock_materialization_read_bytes_per_entry_median=58565
multiblock_record_reuse_hits_per_validated_record_samples=0,0,0,0,0
multiblock_record_reuse_hits_per_validated_record_median=0
multiblock_wal_wraps_samples=9.000,9.000,9.000,8.000,9.000
multiblock_wal_wraps_median=9.000
sequential_duplicate_read_pass_count=5
scattered_duplicate_read_pass_count=5
sequential_wrap_pass_count=5
strace_available=true
scoped_product_materialization_reads=2048
scoped_strace_pread64_calls=2048
scoped_strace_matches_product_counter=true
d2_single_read_candidate_admitted=true
next_recommendation=implement_disabled_single_read_materialization
phase172_wal_materialization_baseline_status=ok
```

## Artifacts And Cleanup

```text
artifact_path=/mnt/smb/work/share/g15d-k8s/phase172-d1-6dd89e7-20260730-050249/phase172-d1-6dd89e7-20260730-050249-artifacts.tar.gz
archive_sha256=c04b2c1d8868d05cb3412b23e4508f63c3c98ba4ecb4c16570a465dc526b9277
archive_file_count=105
durable_archive_integrity=pass
archive_filename_cr=false
lingering_test_process=false
temp_cleanup=pass
lab_resource_cleanup=pass
```

The archive contains the full gate summary, all 20 benchmark logs and metric
value files, storage and geometry logs, race and vet logs, exact-path strace
and probe output, script checksum, source provenance, and per-file checksums.

No product or gate finding was found. One QA-created audit filename initially
contained a carriage return due to a PowerShell here-string; it was renamed
before the final archive, and the discarded archive checksum is not used.
