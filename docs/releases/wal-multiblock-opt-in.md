# WAL Multi-Block Record Opt-In Boundary

Status: source-gated opt-in. This is **not** a default format change.

## Summary

Seaweed Block has a disabled-by-default `walstore` optimization that can encode a
contiguous full-block write batch as one multi-block WAL record while preserving
per-block LSN semantics:

```text
NVMe/TCP write request
-> durable backend WriteBatch
-> one WAL record for N contiguous blocks
-> recovery splits the record back into per-block dirty-map entries
```

The opt-in exists to reduce WAL record materialization overhead in the current
TCP NVMe supported-lab write path. It is not a production storage-format
migration, not a performance/SLO claim, and not related to RoCE or NVMe/RDMA.

## Opt-In Shape

For source-gated lab testing only:

```yaml
blockmaster:
  durableWALMultiBlockRecords: true
```

This renders generated `blockvolume` workloads with:

```text
--durable-wal-multiblock-records
```

Do not enable this in a user release by default. The chart default remains:

```yaml
blockmaster:
  durableWALMultiBlockRecords: false
```

The separate `durableWALRecoveryTestDisableFlusher` value and
`--durable-wal-recovery-test-disable-flusher` flag are recovery-test scaffolding
only. They are used to preserve synced-but-uncheckpointed WAL records for a
restart/replay gate and are not a production or user-facing tuning knob.

## Validated Evidence

| Phase | Evidence | Result |
|---|---|---|
| 147 | WAL multi-block record design gate | PASS |
| 148 | local encode/decode, dirty-read, recovery-split, flusher-split tests | PASS |
| 149 | local profile reduces encode ops from `2048` to `128` | PASS |
| 150 | runtime and Helm opt-in wired default-off | PASS |
| 151 | mounted NVMe/TCP opt-in profile observes multi-block record shape | PASS |
| 152 | mounted hostPath restart/recovery replays multi-block WAL records | PASS |
| 153 | release-boundary documentation keeps the opt-in source-gated | PASS |
| 154 | local durable-status `HeadLSN` diagnostic cleanup | PASS |
| 155 | mounted durable-status `HeadLSN` confirmation after restart/recovery | PASS |

Phase 151 mounted profile evidence:

```text
phase151_wal_multiblock_mounted_nvme_profile_status=ok
runtime_opt_in_enabled=true
candidate_max_h2c_bytes=65536
target_write_request_max_bytes=65536
backend_write_request_max_bytes=65536
wal_encode_ops=9002
backend_storage_write_calls=9002
backend_storage_write_blocks=143570
multiblock_record_shape_observed=true
writer_verified=true
reader_verified=true
cleanup_status=ok
```

Phase 152 mounted recovery evidence:

```text
phase152_wal_multiblock_recovery_compatibility_status=ok
runtime_opt_in_enabled=true
recovery_test_disable_flusher_enabled=true
restart_persistence_mode=hostpath
blockvolume_restart_mode=force_delete_pod
recovery_completed=true
recovered_lsn_after_restart=14545
wal_integrity_fault_observed=false
reader_verified_after_restart=true
ready_after_restart=true
cleanup_status=ok
```

Phase 155 mounted durable-status confirmation:

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
```

## Explicit Non-Claims

- Not enabled by default.
- Not a public release-image claim until matching images are published and a
  release smoke includes this opt-in.
- No performance, throughput, latency, RTO, RPO, or SLO claim.
- No RoCE, NVMe/RDMA, GPU Direct, cuFile/cuObject, or NIXL claim.
- No broad host, distro, kernel, filesystem, or initiator compatibility claim.
- No WAL format migration promise for existing user data.
- No production HA claim.

## Remaining Follow-Ups

- Phase 154 fixed the diagnostic durable status where post-recovery `HeadLSN`
  could display the persisted WAL byte offset instead of the recovered LSN
  frontier. Phase 155 confirmed that fix in the mounted K8s
  restart/recovery path.
- Decide whether this opt-in should stay source-gated or be included in a future
  published-image release smoke.
- Keep the recovery-test flusher-disable hook out of user release guidance.
