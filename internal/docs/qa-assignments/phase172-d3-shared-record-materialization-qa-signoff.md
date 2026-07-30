# Phase 172 D3 Shared-Record Materialization QA Sign-Off

## Verdict

**PASS**

- Exact commit: `7d099248993582eaeea66683089f45c55065320b`
- Branch: `phase172-wal-materialization-pipeline`
- Execution host: `M02` (`linux/amd64`, Go 1.25.0, `CGO_ENABLED=1`)
- Source: isolated clean Linux clone at `/tmp/phase172-d3-qa-7d09924`
- Gate command:

  ```bash
  SW_BLOCK_ARTIFACT_DIR=/tmp/phase172-d3-artifacts-7d09924 \
    bash scripts/run-phase172-shared-record-correctness-gate.sh
  ```

- Gate exit: `0`
- Committed gate SHA-256:
  `e91ee4e6d5d58f41d089ea85d9d3e9412c3b0b3d2a3bb62b9f2f50dd9c5c9339`

## Required Evidence

```text
phase172_shared_record_correctness_status=running
git_sha=7d099248993582eaeea66683089f45c55065320b
git_dirty=false
go_version=go_version_go1.25.0_linux/amd64
kernel=Linux_6.17.0-23-generic_#23~24.04.1-Ubuntu_SMP_PREEMPT_DYNAMIC_Tue_Apr_14_16:11:48_UTC_2_x86_64_GNU/Linux
TestWALStoreSingleReadMaterializationDisabledByDefault=pass
TestWALStoreSingleReadMaterializesOrdinaryAndMultiBlockRecords=pass
TestWALStoreSharedRecordMaterializationReadsEachRecordOnce=pass
TestWALStoreSharedRecordMaterializationReadsRangeTrimOnce=pass
TestWALStoreSharedRecordMaterializationConcurrentPartialOverwrite=pass
TestWALStoreSharedRecordMaterializationSurvivesLegalRingWrap=pass
TestWALStoreSharedRecordMaterializationFailsClosedOnMalformedRecord=pass
focused_repeat_20=pass
default_two_read_path_unchanged=pass
d2_single_read_path_unchanged=pass
ordinary_and_multiblock_shared_reuse=pass
legacy_range_trim_shared_reuse=pass
concurrent_partial_overwrite=pass
legal_ring_wrap_recovery_reuse=pass
malformed_shared_record_fails_closed=pass
race_repeat_20=pass
storage_regression=pass
storage_vet=pass
scoped_probe_shared_record=true
scoped_probe_snapshot_entries=1024
scoped_probe_unique_records=64
scoped_probe_reuse_candidates=960
scoped_probe_validated_records=1024
scoped_probe_header_reads=0
scoped_probe_record_reads=64
scoped_probe_product_materialization_reads=64
scoped_probe_reuse_hits=960
scoped_probe_strace_pread64_calls=64
scoped_strace_matches_product_counter=true
cache_scope=single_flush_cycle
cache_bound=one_decoded_record
external_selector_added=false
d4_equivalence_gate_eligible=true
phase172_shared_record_correctness_status=ok
```

The focused and CGO race logs contain no `SKIP` or `no tests to run`. Each of
the seven top-level race tests ran exactly 20 times. A race-binary inspection
on the same tree found `runtime.raceinit` and `runtime.racefuncenter`.

The exact-file `strace -P` summary reports 64 `pread64` calls. This matches
`scoped_probe_product_materialization_reads=64` for 1,024 logical blocks in 64
physical records. The path records 960 reuse hits and no header reads.

## Contract Checks

- Reuse compares both `WALOffset` and `RecordSize`; one decoded record is cached
  only inside one `flushOnceInternal` invocation.
- Every reuse calls `validateMaterializedDirtyEntry`, independently validating
  LSN, LBA, logical length, and data offset before extent publication.
- Malformed shared records, including `Reserved * blockSize` overflow, fail
  closed without changing checkpoint, logical/physical WAL tails, or dirty map.
- The legal wrap fixture crash-reopens offset zero, reads once, reuses twice,
  and advances the physical tail exactly to the recovered head.
- Concurrent partial overwrite publishes current old blocks, retains the newer
  same-LBA LSN dirty, preserves first-cycle tail, then publishes it next flush.
- Default two-read and D2 single-read paths remain unchanged; no external
  selector was added.

## Artifacts

- Directory: `/mnt/smb/work/share/g15d-k8s/20260730-061645-phase172-d3-shared-record-materialization`
- Archive: `/mnt/smb/work/share/g15d-k8s/20260730-061645-phase172-d3-shared-record-materialization.tar.gz`
- Archive SHA-256: `6418511c7ab72a2053dc6bf12e2e9f96f40f8a9b079fb30bf3127e8a1de9534a`
- Per-file hashes: `artifact-files.sha256` in the artifact directory/archive.

## Findings And Cleanup

- Product findings: none. Gate findings: none. Lab blockers: none.
- All k3s nodes were Ready, m02 disk use was 65%, and no deploy was required.
- SMB rejected timestamp preservation by `cp -a`; the partial QA-owned path was
  removed, then ordinary copying and SHA-256 verification succeeded.
- The QA temporary bundle, Linux clone, and local artifact staging directory
  were removed after sign-off capture. Only persistent SMB evidence remains.
  No shared development worktree or Kubernetes resource was modified.

Phase 172 D3 is eligible to close and D4 may proceed.
