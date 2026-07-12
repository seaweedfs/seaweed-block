# Phase 155 Finished Plan: Mounted Durable Status HeadLSN Confirmation

Status: **closed 2026-07-12, QA PASS**.

## Problem

Phase 154 fixed a local durable-status diagnostic bug where `HeadLSN` could show
the walstore WAL byte-position metadata instead of the recovered LSN frontier.
The remaining question was whether the same fix held on the mounted Kubernetes
path that originally exposed the mismatch.

## Work

Phase 155 added a wrapper gate around the existing Phase 152 mounted
multi-block WAL recovery shape. The gate:

- reruns the source-gated multi-block WAL opt-in with hostPath persistence;
- preserves the Phase 152 writer, force-delete restart, recovery, reader, Ready,
  and cleanup checks;
- reads the live `/status/durable` JSON captured after `blockvolume` restart;
- asserts `DurableLSN`, `HeadLSN`, and recovery evidence all match the recovered
  LSN.

No WAL format, Helm default, or recovery semantics changed in this phase.

## Evidence

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
default_wal_format_unchanged=true
cleanup_status=ok
phase155_decision=mounted_confirmed
```

## Conclusion

The local Phase 154 status fix is confirmed in the live mounted K8s path. The
multi-block WAL record optimization remains disabled by default and source-gated;
the next decision is whether to keep it as a lab-only opt-in or include it in a
future published-image release smoke.
