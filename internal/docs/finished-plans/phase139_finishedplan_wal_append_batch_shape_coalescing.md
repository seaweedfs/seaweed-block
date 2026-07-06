# Phase 139 Finished Plan: WAL Append Batch Shape Coalescing

Status: **closed 2026-07-06, live gate PASS**.

## Problem

Phase 138 showed WAL append/write-at calls were small: about 33KB average and
max write size. The open question was whether this was a WAL writer coalescing
gap or whether the frontend only delivered small write requests to the durable
backend.

## Work

Phase 139 added product-owned upstream write-shape counters:

- backend write request ops/bytes/max/average bytes;
- full-block batch calls/blocks/max/average blocks.

It did not add cross-request buffering or delayed ACK behavior. That would be a
semantic change because each frontend `Write` currently returns after its WAL
append and observer path.

## Gate

Phase 139 added:

- `scripts/run-phase139-wal-append-batch-shape-coalescing-gate.sh`
- `testops/scenarios/nvme-tcp-wal-append-batch-shape-coalescing-chain.yaml`

The gate reruns the 512MiB mounted NVMe/TCP profile, requires active backend
batching, compares the Phase 138 WAL write-at shape, and classifies whether the
shape improved, is frontend-request-limited, or is blocked.

## Evidence

```text
phase139_wal_append_batch_shape_coalescing_status=ok
backend_write_request_max_bytes=32768
backend_write_request_avg_bytes=32723
backend_full_block_batch_max=8
backend_full_block_batch_avg=7
wal_append_writeat_max_bytes=33072
wal_append_writeat_avg_bytes=33012
wal_append_writeat_calls=17979
phase139_shape_result=frontend_request_limited
post_phase139_bottleneck=wal_append_small_writes
next_recommendation=phase140_frontend_request_size_profile
cleanup_status=ok
```

Run bundle:

```text
results\20260706-134824-6806
22 actions: 22 passed, 0 failed
```

## Conclusion

The WAL writer is not failing to coalesce a larger available run; it receives
8-block / 32KB backend requests from the frontend path. Phase 140 should inspect
the frontend/NVMe request-size shape before changing WAL append semantics. This
remains source/lab evidence, not a published performance, RoCE, or NVMe/RDMA
claim.
