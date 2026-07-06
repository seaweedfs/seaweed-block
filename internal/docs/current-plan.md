# Current Plan: Phase 148 WAL Multi-Block Record Local Prototype

Status: planning.

Phase 147 closed the WAL multi-block record design gate:

```text
phase147_wal_multiblock_record_design_status=ok
current_wal_format_unchanged=true
current_recovery_compatibility=pass
candidate_design=multi_block_record
candidate_reduces_record_count=true
candidate_reduces_write_calls=false
durability_invariant_documented=true
recovery_invariant_documented=true
phase147_decision=prototype_next
next_recommendation=phase148_wal_multiblock_record_local_prototype
cleanup_status=ok
```

## Goal

Prototype a multi-block WAL record locally behind an explicit feature gate. The
prototype must prove encode/decode, dirty read, recovery split, and flusher split
semantics without changing the default WAL format or Kubernetes behavior.

## Required Evidence

```text
phase148_wal_multiblock_record_local_prototype_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
default_wal_format_unchanged=true
feature_gate_default=false
multiblock_encode_decode=pass
multiblock_dirty_read=pass
multiblock_recovery_split=pass
multiblock_flusher_split=pass
single_block_compatibility=pass
current_recovery_compatibility=pass
phase148_decision=<profile_next|defer|blocked>
next_recommendation=<specific next phase>
cleanup_status=ok
```

## Boundaries

- Do not enable the new record shape by default.
- Do not use the new record shape from Kubernetes/blockvolume in this phase.
- Do not raise the default H2C size.
- Do not claim throughput/SLO from this design gate.
- Keep old single-block WAL recovery compatibility green.

## Candidate Work

1. Add a disabled-by-default local option for multi-block WAL records.
2. Encode/decode one batch record as `firstLSN + blockIndex` and
   `firstLBA + blockIndex`.
3. Extend only the local test path enough to prove dirty reads, recovery split,
   and flusher split.
4. Keep current single-block format as default and compatibility path.

## Exit Criteria

Phase 148 can close when the prototype proves correctness locally or is
explicitly deferred with a concrete blocker. A later phase must run the mounted
NVMe/TCP profile before any user-facing performance statement.
