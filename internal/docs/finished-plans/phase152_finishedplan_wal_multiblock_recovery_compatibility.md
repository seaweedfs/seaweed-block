# Phase 152 Finished Plan: WAL Multi-Block Recovery Compatibility

Status: **closed 2026-07-06, live gate PASS**.

## Problem

Phase 151 proved that the multi-block WAL record opt-in reaches the mounted
NVMe/TCP write path and reduces record shape as intended. That was still not
enough to release or default the format: the important safety question was
whether synced multi-block WAL records can be replayed after a real
`blockvolume` restart.

## Work

Phase 152 added:

- a default-off walstore recovery-test hook that disables automatic checkpoint
  flushing before test writes;
- blockvolume and blockmaster launcher flags for that hook;
- Helm values and renderer wiring that omit the hook by default and render it
  only when explicitly requested;
- a mounted NVMe/TCP scenario that uses hostPath restart persistence, writes
  data, force-deletes the `blockvolume` pod, waits for recovery, and verifies
  post-restart reads.

## Evidence

```text
phase152_wal_multiblock_recovery_compatibility_status=ok
runtime_opt_in_enabled=true
recovery_test_disable_flusher_enabled=true
restart_persistence_mode=hostpath
multiblock_record_shape_observed=true
writer_verified_before_restart=true
blockvolume_restart_mode=force_delete_pod
blockvolume_restarted=true
recovery_completed=true
recovered_lsn_after_restart=14545
wal_integrity_fault_observed=false
reader_verified_after_restart=true
ready_after_restart=true
cleanup_status=ok
phase152_decision=keep_opt_in
next_recommendation=phase153_wal_multiblock_release_boundary
```

## Conclusion

The opt-in passes the mounted recovery compatibility gate. It remains
default-off, and the project still must not claim performance/SLO, RoCE, or
NVMe/RDMA acceleration from this work. The next step is a release-boundary
review that decides how to document and gate the opt-in without changing the
default WAL format.
