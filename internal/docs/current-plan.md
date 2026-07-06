# Current Plan: Phase 149 WAL Multi-Block Record Profile Gate

Status: planning.

Phase 148 closed the local multi-block WAL record prototype:

```text
phase148_wal_multiblock_record_local_prototype_status=ok
default_wal_format_unchanged=true
feature_gate_default=false
multiblock_encode_decode=pass
multiblock_dirty_read=pass
multiblock_recovery_split=pass
multiblock_flusher_split=pass
single_block_compatibility=pass
current_recovery_compatibility=pass
phase148_decision=profile_next
next_recommendation=phase149_wal_multiblock_record_profile_gate
cleanup_status=ok
```

## Goal

Profile the multi-block WAL prototype under a controlled opt-in and decide
whether it is worth wiring into a mounted NVMe/TCP lab gate. The default path
must remain single-block WAL records.

## Required Evidence

```text
phase149_wal_multiblock_record_profile_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
default_wal_format_unchanged=true
feature_gate_default=false
single_block_compatibility=pass
current_recovery_compatibility=pass
profile_scope=<local_storage|mounted_nvme_tcp>
single_block_wal_encode_ops=<n>
multiblock_wal_encode_ops=<n>
record_count_reduction_visible=<true|false>
dirty_read_verified=true
recovery_verified=true
phase149_decision=<wire_runtime_opt_in|defer|blocked>
next_recommendation=<specific next phase>
cleanup_status=ok
```

## Boundaries

- Do not enable multi-block records by default.
- Do not claim throughput/SLO from this phase.
- Do not raise the default H2C size.
- If this phase stays local-storage only, do not imply mounted NVMe/TCP
  improvement.

## Candidate Work

1. Add a local storage profile comparing single-block and multi-block record
   counts for the same contiguous WriteBatch workload.
2. Keep correctness checks in the same gate: dirty read and recovery must pass.
3. If the record-count reduction is clear, recommend a runtime opt-in wiring
   phase. If not, defer the prototype.

## Exit Criteria

Phase 149 can close when it either justifies wiring a runtime opt-in for mounted
NVMe/TCP profiling, or defers multi-block records with concrete evidence.
