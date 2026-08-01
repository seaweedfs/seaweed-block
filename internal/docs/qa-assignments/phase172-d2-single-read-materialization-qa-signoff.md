# Phase 172 D2 Single-Read Materialization QA Sign-off

## Verdict

**PASS**

Exact commit `a46af56f850ab3faa03fcad92243bf1bad484a69` passed the
unchanged Phase 172 D2 correctness gate on Linux m02. The disabled candidate
uses one exact-file WAL read per validated record, while the shipped default
continues to use the existing header-plus-record two-read path.

```text
phase172_single_read_correctness_status=ok
d3_shared_record_reuse_eligible=true
```

No product, gate, artifact, or lab finding was found.

## Source And Environment

```text
tested_sha=a46af56f850ab3faa03fcad92243bf1bad484a69
tested_branch=qa/phase172-d2-a46af56
source_dirty=false
lab_host=m02
go_version=go version go1.25.0 linux/amd64
cgo_enabled=1
kernel=Linux 6.17.0-23-generic #23~24.04.1-Ubuntu SMP PREEMPT_DYNAMIC Tue Apr 14 16:11:48 UTC 2 x86_64 GNU/Linux
strace_version=strace -- version 6.8
```

The exact commit was transferred as a verified Git bundle and cloned into an
isolated clean worktree on m02. The shared development worktree and its
unrelated modified files were not used or changed.

## Command

The committed gate script was run without modification:

```bash
SW_BLOCK_ARTIFACT_DIR=/tmp/phase172-d2-a46af56-artifacts \
  bash scripts/run-phase172-single-read-correctness-gate.sh
```

The external artifact directory kept the source worktree clean while the gate
checked `git_dirty=false`.

## Exact Test Execution

All eight assigned top-level tests executed 20 times in the focused pass and
20 times under `go test -race`. No test or subtest was skipped or missing:

| Test | Focused | Race |
|---|---:|---:|
| `TestWALStoreSingleReadMaterializationDisabledByDefault` | 20 | 20 |
| `TestWALStoreSingleReadMaterializesOrdinaryAndMultiBlockRecords` | 20 | 20 |
| `TestWALStoreSingleReadMaterializesLegacyRangeTrim` | 20 | 20 |
| `TestWALStoreSingleReadFailsClosedOnInvalidRecord` | 20 | 20 |
| `TestWALStoreSingleReadRejectsInvalidMultiBlockSemantics` | 20 | 20 |
| `TestWALStoreSingleReadRejectsInvalidTrimSemantics` | 20 | 20 |
| `TestWALStoreSingleReadFailureAtEachPhysicalRecordKeepsWholeSnapshot` | 20 | 20 |
| `TestWALStoreSingleReadHandlesReverseGappedAndWrappedRecords` | 20 | 20 |

The focused log contains 36 distinct top-level test/subtest names, each with
20 executions. The audit reported:

```text
focused_skip_or_missing_count=0
race_skip_or_missing_count=0
focused_fail_count=0
race_fail_count=0
```

## Fail-Closed Review

The executed failure table covers:

- zero, short, long, and out-of-bounds record geometry;
- short physical reads, stale LSN, corrupt length or payload, invalid flags,
  unsupported type, record LBA mismatch, dirty length mismatch, and dirty data
  offset mismatch;
- multi-block reserved-count, base-LBA, data-offset, and dirty-length
  mismatches;
- legacy range-trim record-length, unaligned/out-of-range data-offset, and
  dirty-length mismatches;
- corruption at each of three physical record positions;
- reverse, gapped, and ring-wrapped record placement.

Every failure case uses the same terminal-state assertion:

```text
checkpoint_lsn=0
logical_wal_tail=1
physical_wal_tail=0
dirty_entries=original_snapshot_size
```

Thus no failed cycle publishes checkpoint progress, advances the WAL tail, or
deletes a dirty entry. Earlier partial extent writes are allowed, but the
checkpoint and dirty-map transition remains all-or-nothing.

The multi-block success test reports three logical materializations, three
physical record reads, and zero actual record-reuse hits. Shared-record reuse
is not claimed in D2.

## Exact-File Probes

The unchanged gate's candidate probe produced:

```text
phase172_probe_single_read=true
phase172_probe_validated_records=1024
phase172_probe_header_read_ops=0
phase172_probe_record_read_ops=1024
phase172_probe_materialization_read_ops=1024

100.00    0.006696           6      1024           pread64
```

An additional exact-file control probe with the candidate disabled confirmed
the shipped default remains the two-read path:

```text
phase172_probe_single_read=false
phase172_probe_validated_records=1024
phase172_probe_header_read_ops=1024
phase172_probe_record_read_ops=1024
phase172_probe_materialization_read_ops=2048

100.00    0.009566           4      2048           pread64
```

The candidate switch is an unexported package test helper. Its environment
variable is consumed only by a `_test.go` scoped probe. The commit changes no
`cmd/` or chart file and adds no CLI, Helm, configuration, or production
environment selector.

## Full Gate Summary

```text
phase172_single_read_correctness_status=running
git_sha=a46af56f850ab3faa03fcad92243bf1bad484a69
git_dirty=false
go_version=go_version_go1.25.0_linux/amd64
kernel=Linux_6.17.0-23-generic_#23~24.04.1-Ubuntu_SMP_PREEMPT_DYNAMIC_Tue_Apr_14_16:11:48_UTC_2_x86_64_GNU/Linux
TestWALStoreSingleReadMaterializationDisabledByDefault=pass
TestWALStoreSingleReadMaterializesOrdinaryAndMultiBlockRecords=pass
TestWALStoreSingleReadMaterializesLegacyRangeTrim=pass
TestWALStoreSingleReadFailsClosedOnInvalidRecord=pass
TestWALStoreSingleReadRejectsInvalidMultiBlockSemantics=pass
TestWALStoreSingleReadRejectsInvalidTrimSemantics=pass
TestWALStoreSingleReadFailureAtEachPhysicalRecordKeepsWholeSnapshot=pass
TestWALStoreSingleReadHandlesReverseGappedAndWrappedRecords=pass
focused_repeat_20=pass
default_two_read_path=pass
ordinary_single_read=pass
legacy_range_trim_single_read=pass
multiblock_single_read_without_reuse=pass
invalid_geometry_fails_closed=pass
short_read_fails_closed=pass
stale_corrupt_flags_unsupported_fail_closed=pass
ordinary_batch_trim_semantic_mismatches_fail_closed=pass
failed_cycle_retains_wal_tail=pass
physical_record_failure_positions=pass
reverse_gapped_wrap=pass
race_repeat_20=pass
storage_regression=pass
storage_vet=pass
scoped_probe_single_read=true
scoped_probe_validated_records=1024
scoped_probe_header_reads=0
scoped_probe_record_reads=1024
scoped_probe_product_materialization_reads=1024
scoped_probe_strace_pread64_calls=1024
scoped_strace_matches_product_counter=true
default_materialization_unchanged=true
external_selector_added=false
d3_shared_record_reuse_eligible=true
phase172_single_read_correctness_status=ok
```

## Artifacts And Cleanup

```text
artifact_path=V:\share\g15d-k8s\phase172-d2-a46af56-20260730-124744\phase172-d2-a46af56-artifacts.tar.gz
archive_sha256=b08c3c547c5df39205ea4067ea8cd85634f5ae93302cac8570f1a1cc56cd9219
durable_archive_integrity=pass
qa_matching_process_count=0
qa_source_git_dirty=false
qa_kubernetes_resources_touched=false
remote_temp_cleanup_status=ok
qa_cleanup_status=ok
```

The archive contains the full gate summary, focused and race logs, storage
regression and vet logs, candidate and default exact-file probe/strace output,
the compiled probe binary and stores, gate stdout/stderr, and the independent
execution audit.
