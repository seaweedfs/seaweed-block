# Current Plan: Phase 155 Mounted Durable Status HeadLSN Confirmation

Status: planning.

Phase 154 closed the local durable-status `HeadLSN` diagnostic cleanup:

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
phase154_decision=fixed
next_recommendation=phase155_mounted_durable_status_head_lsn_confirmation
```

## Goal

Confirm the Phase 154 local fix on the mounted Kubernetes path that originally
exposed the issue. Rerun the multi-block WAL restart/recovery shape and assert
that live `/status/durable` reports a bounded `HeadLSN` that agrees with the
recovered frontier after restart.

## Required Evidence

```text
phase155_mounted_durable_status_head_lsn_confirmation_status=ok
phase152_followup=head_lsn_diagnostic_cleanup
runtime_opt_in_name=durable-wal-multiblock-records
runtime_opt_in_enabled=true
recovery_test_disable_flusher_enabled=true
restart_persistence_mode=hostpath
blockvolume_restart_mode=force_delete_pod
recovery_completed=true
recovered_lsn_remains_correct=true
durable_status_head_lsn_after_restart=<number>
durable_status_head_lsn_equals_recovered_lsn=true
reader_verified_after_restart=true
ready_after_restart=true
default_wal_format_unchanged=true
cleanup_status=ok
phase155_decision=<mounted_confirmed|blocked>
next_recommendation=<specific next phase>
```

## Boundaries

- Do not enable multi-block records by default.
- Do not weaken the Phase 152 mounted recovery gate.
- Do not turn this into a performance phase.
- Do not claim RoCE, NVMe/RDMA, or performance/SLO.
- Keep the gate scoped to live status confirmation after recovery.

## Candidate Work

1. Extend the Phase 152 runner/gate to capture `/status/durable` after restart.
2. Assert `DurableLSN`, `HeadLSN`, and recovery evidence agree after recovery.
3. Keep the mounted reader/Ready/cleanup checks from Phase 152.
4. Record whether this is sufficient to keep the opt-in source-gated or whether
   another release-smoke artifact check is required.

## Exit Criteria

Phase 155 can close when the live mounted recovery path confirms the Phase 154
status fix: no inflated `HeadLSN`, data still readable after restart, Ready
returns, and cleanup is clean.
