# Current Plan: Phase 147 WAL Multi-Block Record Design Gate

Status: planning.

Phase 146 closed the WAL materialization effectiveness profile:

```text
phase146_wal_record_materialization_effectiveness_status=ok
wal_record_materialization_change=writebatch_value_entries
candidate_max_h2c_bytes=65536
target_write_request_max_bytes=65536
backend_write_request_max_bytes=65536
wal_encode_duration_ms=281
wal_append_duration_ms=280
phase146_pair_improvement_pct=5.24
phase146_effectiveness=visible
phase146_decision=keep_change
next_recommendation=phase147_wal_multiblock_record_design_gate
cleanup_status=ok
```

## Goal

Decide the next deeper WAL optimization path without changing durability
semantics by accident. The candidate designs are:

- multi-block WAL records: encode multiple contiguous block writes into one WAL
  record;
- vectored write-at: keep the current record format but reduce materialization
  or syscall/copy shape with a platform-gated write path.

## Required Evidence

```text
phase147_wal_multiblock_record_design_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
current_wal_format_unchanged=true
current_recovery_compatibility=pass
candidate_design=<multi_block_record|vectored_writeat|defer>
candidate_reduces_record_count=<true|false>
candidate_reduces_write_calls=<true|false>
durability_invariant_documented=true
recovery_invariant_documented=true
phase147_decision=<prototype_next|defer|blocked>
next_recommendation=<specific next phase>
cleanup_status=ok
```

## Boundaries

- Do not change the WAL on-disk format in Phase 147.
- Do not add Linux-specific writev/pwritev behavior without an explicit
  fallback contract.
- Do not raise the default H2C size.
- Do not claim throughput/SLO from this design gate.

## Candidate Work

1. Document the current WAL record/recovery invariants and the exact point where
   contiguous writes could be grouped safely.
2. Add a local design gate that proves current WAL format/recovery tests still
   pass unchanged.
3. Produce an explicit prototype recommendation:
   `multi_block_record`, `vectored_writeat`, or `defer`.

## Exit Criteria

Phase 147 can close when the next WAL optimization is selected with durability
and recovery invariants written down, or when the work is explicitly deferred as
too risky for the current NVMe/TCP supported-lab track.
