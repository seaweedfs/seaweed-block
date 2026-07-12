# Phase 136 Finished Plan: WAL Append / Copy / Checksum Profile

Status: **closed 2026-07-04, live gate PASS**.

## Problem

Phase 135 proved that durable backend batching is active but did not materially
move the supported-lab NVMe/TCP write result. The remaining bottleneck was still
inside backend write work, but the product did not yet expose enough evidence
to separate WAL record encode/copy, checksum, append/write-at, and dirty-map
bookkeeping.

## Work

Phase 136 added low-overhead product counters around the `walstore` write path:

- WAL data copy ops/bytes/duration.
- WAL record encode ops/bytes/duration.
- WAL checksum ops/bytes/duration.
- WAL append/write-at ops/bytes/duration.
- Dirty-map update ops/duration.

The counters are exposed through `/status/durable` in `WriteProfile` and are
validated by local regression tests plus a live mounted NVMe/TCP gate.

## Gate

Phase 136 added:

- `scripts/run-phase136-wal-append-copy-checksum-profile-gate.sh`
- `testops/scenarios/nvme-tcp-wal-append-copy-checksum-profile-chain.yaml`

The gate reuses the Phase 126 512MiB profile shape, requires active backend
batching, asserts all WAL-internal counters are populated, and emits a concrete
next-phase recommendation.

## Evidence

```text
phase136_wal_append_copy_checksum_profile_status=ok
profile_size_mib=512
target_write_observed=true
target_write_bytes=588075008
backend_write_bytes=588075008
backend_storage_batch_calls=17952
backend_storage_batch_blocks=143552
backend_storage_batching_effective=true
wal_copy_ops=143573
wal_copy_bytes=588075008
wal_copy_duration_ms=593
wal_encode_ops=143573
wal_encode_bytes=593530782
wal_encode_duration_ms=753
wal_checksum_ops=143573
wal_checksum_bytes=592382198
wal_checksum_duration_ms=100
wal_append_ops=17981
wal_append_bytes=593543918
wal_append_duration_ms=338
dirty_map_update_ops=143573
dirty_map_update_duration_ms=67
post_phase136_bottleneck=wal_encode
next_recommendation=phase137_reduce_wal_record_encode_copy
cleanup_status=ok
```

Run bundle:

```text
results\20260704-160121-60c2
24 actions: 24 passed, 0 failed
```

## Conclusion

The backend-internal bottleneck is now named from live product evidence:
`wal_encode`, with data copy close behind. Phase 137 should reduce WAL record
encode/copy cost and keep the same mounted NVMe/TCP profiling gate. This phase
does not claim a throughput improvement, SLO, RoCE, or NVMe/RDMA.
