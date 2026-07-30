# Phase 172 D5 Materialization Performance QA Sign-off

Verdict: **PASS REJECT**

The exact-commit Linux gate completed with valid evidence and reported
`d5_materialization_candidate_admitted=false`. This is the planned honest
rejection outcome, not a product or test failure. D6 must not run.

## Source And Run

```text
commit=db9e701eb21b31d4252610adffc9d75d1ec4cb8b
branch=phase172-wal-materialization-pipeline
git_dirty=false
host=m02
go_version=go1.25.0
kernel=Linux 6.17.0-23-generic x86_64
benchmark_time=1s
repetitions=5
writers=1,2,4,8
modes=default,shared-record
flusher_interval=100ms
```

The exact command was:

```bash
SW_BLOCK_ARTIFACT_DIR=/mnt/smb/work/share/g15d-k8s/20260730T065900Z-phase172-d5-materialization-performance \
  bash scripts/run-phase172-materialization-performance-gate.sh \
  /tmp/seaweed-block-qa-phase172-d5-db9e701-20260730T065900Z
```

The gate covered sequential and scattered 4 KiB writes, explicit 16-block
`WriteBatch`, and opt-in 16-block physical WAL records. Default/candidate and
workload order rotated across five repetitions.

## Admission Decision

```text
ordinary_writers_1_default_mibps=82.77
ordinary_writers_1_candidate_mibps=86.89
ordinary_writers_1_candidate_vs_default_ratio=1.050
ordinary_writers_1_floor_met=true

ordinary_writers_4_default_mibps=78.74
ordinary_writers_4_candidate_mibps=84.09
ordinary_writers_4_candidate_vs_default_ratio=1.068
ordinary_writers_4_required_ratio=1.150
ordinary_writers_4_gain_met=false

candidate_ordinary_writers_4_samples_mibps=55.18,84.09,112.05,55.61,85.78
candidate_ordinary_writers_4_range_ratio=2.031
candidate_ordinary_writers_4_required_range_maximum=1.500
candidate_ordinary_writers_4_range_bounded=false

ordinary_writers_4_default_p99_ns=2180587
ordinary_writers_4_candidate_p99_ns=2177345
ordinary_writers_4_p99_candidate_vs_default_ratio=0.999
ordinary_writers_4_p99_bounded=true
```

The candidate reduced ordinary physical materialization reads by exactly 50%,
but the foreground benefit was only 6.8% at four writers and was not stable.
It therefore failed two pre-declared admission conditions. Thresholds were not
changed after observing the result.

## Path And Correctness Evidence

```text
ordinary_default_reads_per_validated_record=2.000
ordinary_candidate_reads_per_validated_record=1.000
ordinary_materialization_read_reduction_met=true

multiblock_default_reads_per_validated_record=2.000
multiblock_candidate_reads_per_validated_record=0.06250
multiblock_candidate_reuse_hits_per_validated_record=0.9375
multiblock_materialization_read_reduction_met=true
multiblock_candidate_reuse_present=true

ordinary_default_product_reads=2048
ordinary_default_strace_pread64=2048
ordinary_candidate_product_reads=1024
ordinary_candidate_strace_pread64=1024
multiblock_default_product_reads=2048
multiblock_default_strace_pread64=2048
multiblock_candidate_product_reads=64
multiblock_candidate_strace_pread64=64
scoped_strace_matches_product_counter=true
```

All 160 workload/mode/writer/repetition samples completed with:

```text
all_samples_checkpoint_coverage_complete=true
all_samples_failed_cycles_zero=true
all_samples_materialization_mode_verified=true
dirty_entries=0
checkpoint_equals_head=true
explicit_final_sync_per_sample=1
cpu_profiles_generated=true
memory_profiles_generated=true
d5_materialization_candidate_admitted=false
next_recommendation=remove_materialization_candidate
phase172_materialization_performance_status=ok
```

All validation, read, extent-write, extent-sync, checkpoint-write,
checkpoint-sync, and cycle-failure counters were zero.

## Artifact And Cleanup

```text
artifact=/mnt/smb/work/share/g15d-k8s/20260730T065900Z-phase172-d5-materialization-performance.tar.gz
artifact_sha256=f33544c3d2bffc3d54954898d9e9cd7e22ffe49897c5a168582cbef9e04b6077
artifact_sha256_recheck=ok
m02_isolated_worktree_removed=true
m02_transfer_bundle_removed=true
windows_temporary_bundle_removed=true
shared_windows_tree_touched=false
```

The artifact contains the complete summary, all 576 five-sample metric-series
files, 40 benchmark logs, exact-path strace reports, and CPU/memory profiles.

Phase 172 D5 closes as **PASS REJECT**. D6 is not eligible. Remove the
disabled materialization candidate and retain the independent recovery
correctness fixes found during D4.
