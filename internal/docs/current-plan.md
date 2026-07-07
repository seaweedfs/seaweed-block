# Current Plan: Phase 153 WAL Multi-Block Release Boundary

Status: planning.

Phase 152 closed the mounted restart/recovery compatibility gate:

```text
phase152_wal_multiblock_recovery_compatibility_status=ok
runtime_opt_in_name=durable-wal-multiblock-records
runtime_opt_in_enabled=true
recovery_test_disable_flusher_enabled=true
restart_persistence_mode=hostpath
candidate_max_h2c_bytes=65536
target_write_request_max_bytes=65536
backend_write_request_max_bytes=65536
wal_encode_ops=873
backend_storage_write_calls=873
backend_storage_write_blocks=13512
multiblock_record_shape_observed=true
writer_verified_before_restart=true
blockvolume_restart_mode=force_delete_pod
blockvolume_restarted=true
recovery_completed=true
recovered_lsn_after_restart=14545
wal_integrity_fault_observed=false
reader_verified_after_restart=true
ready_after_restart=true
cleanup_status=ok
phase152_decision=keep_opt_in
next_recommendation=phase153_wal_multiblock_release_boundary
```

## Goal

Turn the Phase 150-152 evidence into an explicit release boundary for the
multi-block WAL record opt-in. The release boundary must keep the current WAL
format default unchanged, document the opt-in accurately, and name any remaining
diagnostic or compatibility follow-ups before a user-facing release note.

## Required Evidence

```text
phase153_wal_multiblock_release_boundary_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
default_wal_format_unchanged=true
feature_gate_default=false
runtime_opt_in_name=durable-wal-multiblock-records
runtime_opt_in_documented=true
mounted_profile_gate_passed=true
mounted_recovery_gate_passed=true
release_note_non_claims_documented=true
remaining_followups_listed=true
phase153_decision=<document_opt_in|defer|blocked>
next_recommendation=<specific next phase>
```

## Boundaries

- Do not enable multi-block records by default.
- Do not claim throughput/SLO from the Phase 151/152 gates.
- Do not claim RoCE/NVMe-RDMA.
- Do not turn the recovery-test flusher-disable hook into a production feature.
- Do not hide the diagnostic `HeadLSN` follow-up; document it as a non-blocking
  status cleanup unless it proves to affect correctness.

## Candidate Work

1. Add a release-boundary gate script that reads Phase 150-152 evidence.
2. Update release/roadmap docs to describe the opt-in and non-claims.
3. Add a concise user-facing values example for enabling the opt-in in lab-only
   testing.
4. List remaining follow-ups, including the post-recovery `HeadLSN` diagnostic
   cleanup.

## Exit Criteria

Phase 153 can close when the project has an accurate release boundary for the
multi-block WAL opt-in: profile and recovery gates cited, defaults unchanged,
non-claims explicit, and follow-ups tracked.
