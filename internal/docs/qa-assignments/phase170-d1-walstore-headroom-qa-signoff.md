# Phase 170 D1 WALStore Headroom QA Sign-Off

Verdict: evidence mechanism PASS and staged commit owner STOP at exact commit
`b9e4fda6599b7c81cbf1800a89f3e9189e7cc034`.

## Environment

```text
environment=m02
go_version=go1.25.0 linux/amd64
source_archive_sha256=d4dd771b46de572cde944c41f77808b0edd2818c33b156cc28374eefde216144
qa_archive_sha256=ef8469ada1daa552fedb1a4d23d5c3763ebb59d258eee2c1becd8537ea38bbd9
benchmark_time=1s
repetitions=5
writers=1,2,4,8
```

The gate ran 40 benchmark rows with the normal background flusher enabled.
Every row completed one explicit final Sync, drained the flusher after timing,
reported `dirty_entries=0`, and reached
`checkpoint_lsn=head_lsn`. The storage regression suite passed.

## Decision Evidence

```text
ordinary_writers_1_mibps_median=87.97
ordinary_writers_4_mibps_median=64.09
ordinary_writers_8_mibps_median=50.12
batch_writers_1_mibps_median=137.48
batch_writers_4_mibps_median=47.22
batch_writers_8_mibps_median=50.51
ordinary_four_writer_scaling_ratio=0.729
batch_four_vs_ordinary_four_ratio=0.737
ordinary_writers_4_range_ratio=2.936
batch_writers_4_range_ratio=3.500
paired_batch_gain_pass_count=2
paired_batch_gain_stable=false
ordinary_writers_4_writeat_calls_per_entry_median=1.000
batch_writers_4_writeat_calls_per_entry_median=0.06250
writeat_shape_stable=true
batch_throughput_headroom=false
existing_format_headroom=false
d2_owner_admitted=false
next_recommendation=stop_before_staged_owner
phase170_walstore_headroom_status=ok
```

The explicit batch control reduced WAL `WriteAt` calls by about 16x, but that
mechanical coalescing did not produce stable concurrent throughput headroom.
The four-writer batch median was below ordinary Write, only two of five paired
samples passed the gain threshold, and both four-writer ranges exceeded the
`1.35x` stability limit.

The ordinary path also exposed the current contention shape:

```text
ordinary_writers_1_commit_lock_wait_ns_per_api_call_median=86.68
ordinary_writers_4_commit_lock_wait_ns_per_api_call_median=29264
ordinary_writers_4_p99_ns_per_api_call_median=2211028
cpu_top_flat_syscall6_percent=39.22
qualitative_strace_pwrite64_calls=30948
qualitative_strace_fsync_calls=18
```

This is evidence of contention and writeback amplification, but not evidence
that another append owner will fix it. The Phase 170 stop rule therefore
applies before D2 implementation.

## Artifacts

- Run root:
  `C:\work\qa-phase170-d1-b9e4fda-20260730-010058`
- Summary:
  `artifacts/source/results/phase170-walstore-headroom-gate/phase170-walstore-headroom-summary.txt`
- QA archive:
  `C:\work\qa-phase170-d1-b9e4fda-20260730-010058\qa-artifacts.tar.gz`

QA removed the scoped remote directory, found no residual process, and left
all three Kubernetes nodes Ready. The shared development worktree was not
modified.
