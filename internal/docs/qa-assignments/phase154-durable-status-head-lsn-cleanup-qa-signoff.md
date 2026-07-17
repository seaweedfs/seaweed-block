# Phase 154 QA Sign-Off: Durable Status HeadLSN Diagnostic Cleanup

Status: **PASS** on 2026-07-07.

Run bundle:
`results/phase154-durable-status-head-lsn-cleanup-gate`.

## Verdict

The Phase 152 diagnostic mismatch is fixed at the storage/status boundary.
`walstore` no longer initializes the in-memory `HeadLSN` boundary from the
superblock's WAL byte-position metadata, and recovery now sets `HeadLSN` from
the recovered LSN frontier. Durable provider status now reports
`DurableLSN == HeadLSN == recovered LSN` for the multi-block WAL recovery shape
that previously displayed a much larger `HeadLSN`.

This is a diagnostic/status fix. It does not enable multi-block WAL records by
default, does not change the WAL format, and does not create a performance,
RoCE, or NVMe/RDMA claim.

## Evidence

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

## Regression Coverage

- `core/storage`: multi-block WAL recovery after persisted WAL byte-position
  metadata asserts `Boundaries().H == recovered LSN`, not the WAL byte offset.
- `core/frontend/durable`: `DurableProvider.DurableStatuses()` asserts
  `DurableLSN`, `HeadLSN`, and recovery evidence agree after reopening and
  recovering a multi-block WAL store.
- Full package tests passed for `./core/storage` and `./core/frontend/durable`.

## Conclusion

Phase 154 closes the local diagnostic/status cleanup. Phase 155 later confirmed
the same `HeadLSN` boundary in the mounted K8s restart/recovery path.
