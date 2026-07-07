# Phase 152 QA Sign-Off: WAL Multi-Block Recovery Compatibility

Status: **PASS** on 2026-07-06.

Run bundle: `results/20260706-220244-712c`.

## Verdict

The mounted NVMe/TCP opt-in path now proves real WAL replay for multi-block WAL
records after a `blockvolume` process restart. The gate uses hostPath restart
persistence, disables walstore's automatic checkpoint flusher through an
explicit test-only opt-in, force-deletes the `blockvolume` pod, waits for
recovery, and verifies mounted data after restart.

## Evidence

```text
phase152_wal_multiblock_recovery_compatibility_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
default_wal_format_unchanged=true
feature_gate_default=false
runtime_opt_in_name=durable-wal-multiblock-records
runtime_opt_in_enabled=true
recovery_test_disable_flusher_enabled=true
seq_size_mib=4
restart_verify_mib=4
restart_persistence_mode=hostpath
candidate_max_h2c_bytes=65536
target_write_request_max_bytes=65536
backend_write_request_max_bytes=65536
wal_encode_ops=873
backend_storage_write_calls=873
backend_storage_write_blocks=13512
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

Blockvolume restart log includes:

```text
blockvolume: durable recovered: recovered LSN=14545
```

## Fixes Added By The Gate

- Added an explicit walstore recovery-test hook:
  `--durable-wal-recovery-test-disable-flusher`.
- Wired it through durable provider, `blockvolume`, `blockmaster` launcher,
  launcher renderer, and Helm values.
- Kept default Helm renders unchanged: no multi-block opt-in and no
  recovery-test flusher disable unless explicitly requested.
- Updated the Phase 152 scenario to use run-scoped hostPath state so the
  force-delete restart preserves the WAL file.

## Non-Blocking Note

After recovery, the diagnostic durable status reported `DurableLSN=14545` and
`Evidence="recovered LSN=14545"`, but `HeadLSN` displayed a much larger value.
The mounted reader, Ready status, and recovered LSN gate all passed. Track this
as a follow-up diagnostic/status cleanup; it does not block this compatibility
gate.

## Conclusion

The multi-block WAL opt-in can proceed to a release-boundary review. It remains
default-off and is not a throughput, SLO, RoCE, or NVMe/RDMA claim.
