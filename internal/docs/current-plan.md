# Current Plan: Phase 137 Reduce WAL Record Encode / Copy Cost

Status: planning.

Phase 136 split the durable backend write path below the batch seam with
product-owned `/status/durable` counters:

```text
phase136_wal_append_copy_checksum_profile_status=ok
target_write_observed=true
backend_write_bytes=588075008
backend_storage_batch_calls=17952
backend_storage_batch_blocks=143552
backend_storage_batching_effective=true
wal_copy_duration_ms=593
wal_encode_duration_ms=753
wal_checksum_duration_ms=100
wal_append_duration_ms=338
dirty_map_update_duration_ms=67
post_phase136_bottleneck=wal_encode
next_recommendation=phase137_reduce_wal_record_encode_copy
cleanup_status=ok
```

This means the next useful work is still not NVMe/RDMA. It is to reduce the WAL
record encode/copy cost while preserving WAL recovery semantics and the current
one-record-per-block durability model.

## Goal

```text
mounted NVMe/TCP write still reaches durable backend
-> backend batching remains active
-> WAL record encode/copy cost is reduced or isolated further
-> no WAL recovery semantics are weakened
-> next bottleneck is named from counters after the change
-> cleanup remains clean
```

## Required Evidence

```text
phase137_reduce_wal_record_encode_copy_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
target_write_observed=true
backend_storage_batching_effective=true
wal_encode_ops=<count>
wal_encode_bytes=<bytes>
wal_encode_duration_ms=<duration>
wal_copy_ops=<count>
wal_copy_bytes=<bytes>
wal_copy_duration_ms=<duration>
phase136_wal_encode_duration_ms=753
phase136_wal_copy_duration_ms=593
post_phase137_bottleneck=<wal_encode|wal_copy|wal_append|wal_checksum|dirty_map|unknown>
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
- Keep the change inside WAL record construction/copy shape unless evidence
  proves the bottleneck moved.

## Candidate Work

1. Reduce avoidable record-buffer copying in the WAL encode path.
2. Preserve the record format and recovery behavior unless a separate migration
   plan is written.
3. Add a local regression proving encoded records still decode/recover.
4. Add a Phase 137 wrapper/scenario that reruns the Phase 136 profile and
   compares against the Phase 136 encode/copy baseline.
5. Classify the next bottleneck after the change:
   - encode/copy still dominates -> split encode allocation/copy further;
   - append dominates -> inspect pwrite/coalescing shape;
   - checksum dominates -> inspect checksum strategy;
   - unknown -> add the missing counter instead of guessing.

## Exit Criteria

Phase 137 can close when the live supported-lab gate either reduces the
Phase 136 WAL encode/copy cost or proves the cost is not reducible without a
larger WAL format change. The close report must name the next bottleneck from
product-owned counters and keep cleanup clean.
