# Current Plan: Phase 139 WAL Append Batch Shape Coalescing

Status: planning.

Phase 138 split the WAL append/write-at shape:

```text
phase138_wal_writeat_shape_profile_status=ok
target_write_observed=true
backend_write_bytes=588075008
backend_storage_batch_calls=17953
backend_storage_batch_blocks=143555
backend_storage_batching_effective=true
wal_append_duration_ms=380
wal_append_writeat_calls=17979
wal_append_writeat_bytes=593543918
wal_append_writeat_max_bytes=33072
wal_append_writeat_avg_bytes=33013
wal_append_wrap_count=8
wal_append_padding_bytes=13136
post_phase138_bottleneck=wal_append_small_writes
next_recommendation=phase139_wal_append_batch_shape_coalescing
cleanup_status=ok
```

This means the next useful work is still not NVMe/RDMA. The WAL writer is
issuing many small pwrite calls around 33KB; the next phase should explain or
improve the batch/coalescing shape feeding `walWriter.appendBatch`.

## Goal

```text
mounted NVMe/TCP write still reaches durable backend
-> backend batching remains active
-> WAL append write-at average/max size improves or the upstream request shape is named
-> write-at calls per MiB decrease or are explained by frontend request shape
-> no WAL recovery semantics are weakened
-> next bottleneck is named from counters after the change
-> cleanup remains clean
```

## Required Evidence

```text
phase139_wal_append_batch_shape_coalescing_status=ok
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
phase138_wal_append_writeat_avg_bytes=33013
phase138_wal_append_writeat_max_bytes=33072
phase138_wal_append_writeat_calls=17979
phase139_shape_result=<improved|frontend_request_limited|blocked>
post_phase139_bottleneck=<wal_append_small_writes|wal_append_syscall|wal_encode|wal_checksum|dirty_map|unknown>
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
- Keep the scope inside WAL batch/coalescing shape and the upstream full-block
  write run shape.

## Candidate Work

1. Inspect the upstream write request size reaching `StorageBackend.writeBytes`
   and `WriteBatch`.
2. If the frontend/request shape only feeds 8 full blocks at a time, surface
   that as the limiting shape instead of guessing in `walWriter`.
3. If the backend can safely coalesce multiple adjacent batches without
   changing ack/recovery semantics, implement the smallest bounded coalescing
   step and gate it.
4. Add a Phase 139 wrapper/scenario that compares write-at average/max size and
   calls against Phase 138.
5. If coalescing is blocked by frontend request shape, close with an explicit
   `frontend_request_limited` result and a concrete next phase.

## Exit Criteria

Phase 139 can close when the live supported-lab gate either improves the
write-at shape or proves the current 33KB shape is imposed by frontend request
size. The close report must name the next bottleneck from product-owned
counters and keep cleanup clean.
