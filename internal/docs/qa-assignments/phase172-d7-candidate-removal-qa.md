# Phase 172 D7 Candidate Removal QA

## Goal

Prove on an exact clean Linux commit that the rejected materialization
candidate and its measurement-only overhead are gone, the shipped default
header-plus-record path remains, and independent Phase 172 correctness fixes
remain covered. This gate does not rerun D6 or reconsider D5 thresholds.

## Command

Follow `QA-AGENT-RUNBOOK.md` and run:

```bash
SW_BLOCK_ARTIFACT_DIR=/mnt/smb/work/share/g15d-k8s/<run-id> \
  bash scripts/run-phase172-candidate-removal-gate.sh \
  <exact-clean-worktree>
```

Do not patch product or gate code.

## Required Evidence

```text
candidate_runtime_and_test_symbols=0
candidate_dirty_geometry_fields=0
candidate_instrumentation_fields=0
candidate_files_removed=true
default_flusher_header_read_path_present=true
default_flusher_record_read_path_present=true
retained_correctness_repeat_20=pass
retained_correctness_race_repeat_10=pass
legacy_range_trim=pass
partial_multiblock_suffix_replay=pass
malformed_batch_typed_fail_closed=pass
legacy_wrapped_byte_boundaries=pass
storage_regression=pass
recovery_regression=pass
replication_regression=pass
replication_component_regression=pass
candidate_removal_vet=pass
d6_mounted_gate_run=false
default_materialization_path_unchanged=true
independent_recovery_fixes_retained=true
phase172_candidate_removal_status=ok
```

`PASS` requires every value above and a zero exit. Any remaining selector,
candidate file, geometry/counter overhead, failed retained regression, race,
or vet error is a blocker.

Package the artifact and SHA-256, write a sign-off and patch to the SMB QA
directory, remove the isolated worktree, and leave the shared Windows tree
untouched.
