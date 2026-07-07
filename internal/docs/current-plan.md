# Current Plan: Phase 154 Durable Status HeadLSN Diagnostic Cleanup

Status: planning.

Phase 153 closed the multi-block WAL release-boundary gate:

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
phase153_decision=document_opt_in
next_recommendation=phase154_durable_status_head_lsn_cleanup
```

## Goal

Clean up the diagnostic durable-status mismatch observed in Phase 152 after
multi-block WAL recovery. The recovery gate showed the mounted reader and
`DurableLSN=14545` were correct, but `/status/durable` displayed a much larger
`HeadLSN`. Phase 154 should clarify and fix the status semantics without
weakening recovery correctness or changing the default WAL format.

## Required Evidence

```text
phase154_durable_status_head_lsn_cleanup_status=ok
phase152_followup=head_lsn_diagnostic_cleanup
runtime_opt_in_name=durable-wal-multiblock-records
runtime_opt_in_enabled=true
recovered_lsn_remains_correct=true
durable_status_head_lsn_semantics_documented=true
head_lsn_after_recovery_is_bounded=true
no_recovery_semantics_change_without_test=true
default_wal_format_unchanged=true
cleanup_status=ok
phase154_decision=<fixed|documented_no_code_change|blocked>
next_recommendation=<specific next phase>
```

## Boundaries

- Do not enable multi-block records by default.
- Do not change WAL recovery behavior unless a test proves the current behavior
  is incorrect.
- Do not weaken the Phase 152 mounted recovery gate.
- Do not turn this into a performance phase.
- Keep the work scoped to durable status semantics and diagnostics unless a real
  correctness issue is found.

## Candidate Work

1. Reproduce or isolate the Phase 152 post-recovery status mismatch with the
   smallest local or mounted gate available.
2. Define what `HeadLSN`, `DurableLSN`, and recovery evidence should mean after
   replay.
3. Fix the status publisher or document the semantics if no code change is
   needed.
4. Add a regression that proves recovered LSN remains correct and diagnostic
   `HeadLSN` no longer shows an unrelated larger value.

## Exit Criteria

Phase 154 can close when the durable status after multi-block recovery is
coherent, documented, and regression-tested without changing defaults or making
new performance/RDMA claims.
