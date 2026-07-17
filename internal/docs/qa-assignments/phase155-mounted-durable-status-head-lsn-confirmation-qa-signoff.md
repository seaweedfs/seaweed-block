# Phase 155 QA Sign-Off: Mounted Durable Status HeadLSN Confirmation

Status: **PASS** on 2026-07-12.

Run bundle:
`results/20260712-101758-5762`.

Remote artifact:
`/mnt/smb/work/share/g15d-k8s/20260712-101758-5762-phase155-head-lsn`.

## Verdict

The Phase 154 durable-status fix is confirmed on the mounted Kubernetes
restart/recovery path. The gate reran the Phase 152 multi-block WAL opt-in
shape with hostPath persistence, force-deleted the `blockvolume` pod, verified
data after recovery, and then asserted live `/status/durable` reports
`HeadLSN == DurableLSN == recovered LSN`.

This remains a source-gated opt-in confirmation. It does not enable multi-block
WAL records by default and does not create a performance, RoCE, or NVMe/RDMA
claim.

## Evidence

```text
phase155_mounted_durable_status_head_lsn_confirmation_status=ok
phase152_followup=head_lsn_diagnostic_cleanup
runtime_opt_in_name=durable-wal-multiblock-records
runtime_opt_in_enabled=true
recovery_test_disable_flusher_enabled=true
restart_persistence_mode=hostpath
blockvolume_restart_mode=force_delete_pod
recovery_completed=true
recovered_lsn_after_restart=13511
recovered_lsn_remains_correct=true
wal_integrity_fault_observed=false
durable_status_volume_id=pvc-b0e723aa-c4b1-4639-a00e-580a56f52f16
durable_status_durable_lsn_after_restart=13511
durable_status_head_lsn_after_restart=13511
durable_status_head_lsn_equals_recovered_lsn=true
durable_status_evidence_matches_recovered_lsn=true
durable_status_latched_after_restart=true
durable_status_operational_after_restart=true
durable_status_epoch_after_restart=1
reader_verified_after_restart=true
ready_after_restart=true
default_wal_format_unchanged=true
cleanup_status=ok
phase155_decision=mounted_confirmed
next_recommendation=phase156_wal_multiblock_published_image_release_smoke_decision
```

The nested Phase 152 summary also remained green:

```text
phase152_wal_multiblock_recovery_compatibility_status=ok
target_write_request_max_bytes=65536
backend_write_request_max_bytes=65536
wal_encode_ops=618
backend_storage_write_calls=618
backend_storage_write_blocks=9417
multiblock_record_shape_observed=true
mounted_helm_renders_recovery_test_disable_flusher=true
writer_verified_before_restart=true
blockvolume_restart_mode=force_delete_pod
blockvolume_restarted=true
recovery_completed=true
recovered_lsn_after_restart=13511
reader_verified_after_restart=true
ready_after_restart=true
cleanup_status=ok
```

## Runner Result

```text
=== nvme-tcp-wal-multiblock-head-lsn-confirmation-chain === PASS (6m36.157s)
47 actions: 47 passed, 0 failed
```

## Cleanup

The nested cleanup verifiers reported zero residue:

```text
cleanup_status=ok
k8s_residue_count=0
iscsi_residue_count=0
process_residue_count=0
multipath_residue_count=0
hostpath_residue_count=0
failure_count=0
```

Post-run lab checks showed no `sw-block` Helm release, no `sw-block` pods/PVCs,
no `swblock*` CRDs, and no Seaweed NVMe subsystem residue.

## Conclusion

Phase 155 closes the mounted follow-up from Phase 154. The live K8s status
surface now agrees with the recovered WAL frontier after mounted restart, while
the opt-in boundary remains unchanged.
