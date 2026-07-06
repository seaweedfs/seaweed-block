# Current Plan: Phase 152 WAL Multi-Block Recovery Compatibility Gate

Status: planning.

Phase 151 closed the mounted NVMe/TCP opt-in profile:

```text
phase151_wal_multiblock_mounted_nvme_profile_status=ok
runtime_opt_in_name=durable-wal-multiblock-records
runtime_opt_in_enabled=true
candidate_max_h2c_bytes=65536
target_write_request_max_bytes=65536
backend_write_request_max_bytes=65536
wal_encode_ops=9002
backend_storage_write_calls=9002
backend_storage_write_blocks=143570
multiblock_record_shape_observed=true
writer_verified=true
reader_verified=true
cleanup_status=ok
phase151_decision=keep_opt_in
next_recommendation=phase152_wal_multiblock_recovery_compatibility_gate
```

## Goal

Prove that actual multi-block WAL records can be recovered after a mounted
NVMe/TCP write path restart. This is the safety gate that should come before
any release claim or default change for the new WAL entry type.

## Required Evidence

```text
phase152_wal_multiblock_recovery_compatibility_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
default_wal_format_unchanged=true
feature_gate_default=false
runtime_opt_in_name=durable-wal-multiblock-records
runtime_opt_in_enabled=true
multiblock_record_shape_observed=true
writer_verified_before_restart=true
blockvolume_restarted=true
recovery_completed=true
wal_integrity_fault_observed=false
reader_verified_after_restart=true
ready_after_restart=true
cleanup_status=ok
phase152_decision=<keep_opt_in|defer|blocked>
next_recommendation=<specific next phase>
```

## Boundaries

- Do not enable multi-block records by default.
- Do not claim throughput/SLO from this phase.
- Do not claim RoCE/NVMe-RDMA.
- Do not hide recovery warnings behind a PASS; any WAL integrity fault, false
  Ready, or post-restart read mismatch blocks the phase.

## Candidate Work

1. Extend the Phase 151 mounted profile into a restart/recovery gate.
2. Write data through the mounted NVMe/TCP PVC with the opt-in enabled.
3. Restart the owning `blockvolume` pod without deleting durable state.
4. Wait for recovery and Ready status.
5. Verify the reader sees the pre-restart data and no WAL integrity fault was
   surfaced.

## Exit Criteria

Phase 152 can close when a mounted opt-in volume restarts and recovers with
data intact, no false Ready during recovery, and zero cleanup residue. If the
runtime cannot prove those facts, keep the feature default-off and file the
blocking evidence.
