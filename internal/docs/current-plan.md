# Current Plan: Phase 156 WAL Multi-Block Release Smoke Decision

Status: planning.

Phase 155 closed the mounted durable-status confirmation:

```text
phase155_mounted_durable_status_head_lsn_confirmation_status=ok
runtime_opt_in_enabled=true
recovery_test_disable_flusher_enabled=true
restart_persistence_mode=hostpath
blockvolume_restart_mode=force_delete_pod
recovery_completed=true
recovered_lsn_after_restart=13511
durable_status_durable_lsn_after_restart=13511
durable_status_head_lsn_after_restart=13511
durable_status_head_lsn_equals_recovered_lsn=true
reader_verified_after_restart=true
ready_after_restart=true
cleanup_status=ok
phase155_decision=mounted_confirmed
next_recommendation=phase156_wal_multiblock_published_image_release_smoke_decision
```

## Goal

Decide the release boundary for the disabled-by-default multi-block WAL record
opt-in after Phases 150-155. The decision should either keep it source-gated
with no published-image claim, or define a narrow published-image smoke that
proves the same opt-in, recovery, and durable-status evidence on release
artifacts.

## Required Evidence

```text
phase156_wal_multiblock_release_smoke_decision_status=ok
runtime_opt_in_name=durable-wal-multiblock-records
default_wal_format_unchanged=true
source_gated_status=<kept|superseded_by_published_image_smoke>
published_image_smoke_required=<true|false>
published_image_smoke_scope=<none|explicit_opt_in_recovery_status>
recovery_test_disable_flusher_user_claim=false
performance_slo_claim_allowed=false
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
next_recommendation=<specific next phase>
```

## Boundaries

- Do not enable multi-block records by default.
- Do not turn Phase155 evidence into a public image claim unless the matching
  published artifacts are actually smoked.
- Do not claim performance, RoCE, NVMe/RDMA, broad compatibility, or production
  HA.
- Keep the recovery-test flusher-disable hook out of user guidance.

## Candidate Work

1. Review whether upcoming development wants a release now or continued source
   work.
2. If no release is being cut, document that the opt-in remains source-gated.
3. If a release is being cut, define the minimal published-image smoke:
   explicit opt-in, mounted recovery, `HeadLSN == recovered LSN`, reader/Ready,
   cleanup.
4. Update release docs with the chosen boundary.

## Exit Criteria

Phase 156 can close when the release boundary is explicit enough that README and
release docs cannot accidentally over-claim Phase155 source-gated evidence.
