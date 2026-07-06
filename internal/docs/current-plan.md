# Current Plan: Phase 140 Frontend Request Size Profile

Status: planning.

Phase 139 proved the WAL append small-write shape is imposed by the frontend
request size:

```text
phase139_wal_append_batch_shape_coalescing_status=ok
target_write_observed=true
backend_write_request_max_bytes=32768
backend_write_request_avg_bytes=32723
backend_full_block_batch_max=8
backend_full_block_batch_avg=7
backend_storage_batching_effective=true
wal_append_writeat_max_bytes=33072
wal_append_writeat_avg_bytes=33012
phase139_shape_result=frontend_request_limited
post_phase139_bottleneck=wal_append_small_writes
next_recommendation=phase140_frontend_request_size_profile
cleanup_status=ok
```

This means the next useful work is still not NVMe/RDMA. The WAL writer is
coalescing the 8-block batch it receives; the next phase should inspect why the
frontend/NVMe path is delivering 32KB requests and whether that request size is
configurable, host-driven, or product-limited.

## Goal

```text
mounted NVMe/TCP write still reaches durable backend
-> backend batching remains active
-> frontend target write request size is visible
-> StorageBackend write request size is visible
-> NVMe command / host request shape is visible or explicitly unavailable
-> request-size owner is named
-> no WAL recovery semantics are weakened
-> next bottleneck is named from counters after the change
-> cleanup remains clean
```

## Required Evidence

```text
phase140_frontend_request_size_profile_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
target_write_observed=true
backend_storage_batching_effective=true
target_write_request_max_bytes=<bytes>
target_write_request_avg_bytes=<bytes>
backend_write_request_max_bytes=32768
backend_full_block_batch_max=8
frontend_request_size_owner=<host_nvme|target_limit|backend_limit|unknown>
phase140_shape_result=<host_limited|target_limited|backend_limited|tunable|unknown>
post_phase140_bottleneck=<frontend_request_size|wal_append_small_writes|wal_append_syscall|wal_encode|unknown>
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
- Keep the scope to request-size evidence unless a one-line product limit is
  found.

## Candidate Work

1. Add target-side request-size counters if they are missing before
   `StorageBackend.Write`.
2. Compare target request size with backend request size.
3. Inspect the NVMe command/request shape the target receives, if available.
4. If a product-side cap causes 32KB requests, make it explicit and gate any
   safe increase.
5. If the host/NVMe initiator chooses 32KB, close with `host_limited` and stop
   optimizing WAL coalescing for this benchmark shape.

## Exit Criteria

Phase 140 can close when the live supported-lab gate names the owner of the
32KB frontend request shape and cleanup is clean. If the shape is host-driven,
the close report should stop this WAL coalescing thread and recommend either a
host/request-size experiment or a different backend bottleneck.
