# Current Plan: Phase 138 WAL WriteAt Shape Profile

Status: planning.

Phase 137 reduced the WAL encode/copy seam and moved the backend-internal
bottleneck to append/write-at:

```text
phase137_reduce_wal_record_encode_copy_status=ok
target_write_observed=true
backend_write_bytes=588075008
backend_storage_batch_calls=17953
backend_storage_batch_blocks=143555
backend_storage_batching_effective=true
phase136_wal_encode_copy_duration_ms=1346
wal_encode_copy_duration_ms=363
wal_copy_duration_ms=93
wal_encode_duration_ms=270
wal_checksum_duration_ms=110
wal_append_duration_ms=375
dirty_map_update_duration_ms=74
post_phase137_bottleneck=wal_append
next_recommendation=phase138_wal_writeat_shape_profile
cleanup_status=ok
```

This means the next useful work is still not NVMe/RDMA. It is to split WAL
append/write-at cost into pwrite count, coalesced write size, wrap/padding
behavior, and syscall/write latency.

## Goal

```text
mounted NVMe/TCP write still reaches durable backend
-> backend batching remains active
-> WAL append/write-at shape is visible
-> pwrite count and bytes are visible
-> coalesced write size distribution is visible
-> wrap/padding behavior is visible
-> no WAL recovery semantics are weakened
-> next bottleneck is named from counters after the change
-> cleanup remains clean
```

## Required Evidence

```text
phase138_wal_writeat_shape_profile_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
target_write_observed=true
backend_storage_batching_effective=true
wal_append_ops=<count>
wal_append_bytes=<bytes>
wal_append_duration_ms=<duration>
wal_append_writeat_calls=<count>
wal_append_writeat_bytes=<bytes>
wal_append_writeat_max_bytes=<bytes>
wal_append_writeat_avg_bytes=<bytes>
wal_append_wrap_count=<count>
wal_append_padding_bytes=<bytes>
phase137_wal_append_duration_ms=375
post_phase138_bottleneck=<wal_append_syscall|wal_append_small_writes|wal_wrap_padding|wal_encode|wal_checksum|dirty_map|unknown>
next_recommendation=<specific next phase>
cleanup_status=ok
```

## Boundaries

- Do not change frontend protocol behavior, CSI semantics, authority behavior,
  failover, or reconnect logic.
- Do not add broad tracing frameworks or data-path logging.
- Do not claim performance improvement, SLO, RoCE, NVMe/RDMA, GPU Direct,
  cuFile/cuObject, or NIXL.
- Do not weaken WAL recovery semantics: every write still needs an LSN and a
  recoverable record.
- Keep this phase to append/write-at observation unless the instrumentation
  itself exposes a trivial bug.

## Candidate Work

1. Add product counters around WAL append write-at shape:
   write-at call count, total bytes, max bytes, average bytes.
2. Count circular-WAL wrap/padding events and padding bytes.
3. Keep the existing Phase 137 encode/copy counters so the new shape is
   comparable.
4. Add a Phase 138 wrapper/scenario that reruns the Phase 137 profile and
   classifies whether append cost is syscall count, small-write shape, wrap
   behavior, or something else.
5. Do not optimize pwrite shape in this phase unless the counter insertion
   exposes a one-line bug.

## Exit Criteria

Phase 138 can close when the live supported-lab gate names the append/write-at
shape from product-owned counters and cleanup is clean. If pwrite shape is not
the reason append dominates, close with the measured distribution and a concrete
next experiment.
