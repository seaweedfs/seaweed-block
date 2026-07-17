# Phase 154 Finished Plan: Durable Status HeadLSN Diagnostic Cleanup

Status: **closed 2026-07-07, local gate PASS**.

## Problem

Phase 152 proved that multi-block WAL records replay after a mounted
`blockvolume` restart. The user-visible durable status still showed an
incoherent diagnostic: `DurableLSN` and recovery evidence reported the recovered
frontier, while `HeadLSN` displayed a much larger value.

Root cause: `walstore` mixed two different notions named "head":

- superblock `WALHead`: monotonic byte position in the WAL ring;
- `LogicalStorage.Boundaries().H`: newest written LSN.

On reopen, `openInitialized` initialized the in-memory LSN boundary from the
superblock byte-position. If that byte-position was larger than the recovered
LSN, `Recover` left `HeadLSN` inflated even though data recovery was correct.

## Work

Phase 154:

- initializes in-memory `walHead` from the checkpoint LSN, not superblock WAL
  byte-position;
- initializes the retained LSN boundary from checkpoint LSN;
- sets `walHead` to the recovered frontier after `Recover`;
- adds storage and durable-provider regressions for the exact multi-block WAL
  recovery/status shape.

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

## Conclusion

The local diagnostic bug is fixed without changing the default WAL format or
the Phase 152 recovery contract. Phase 155 later confirmed the same status
boundary in the live mounted K8s path.
