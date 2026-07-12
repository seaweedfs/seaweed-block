# Phase 134 Finished Plan: Durable Backend Write Batching

Status: **closed 2026-07-04, live gate PASS**.

## Problem

Phase 126 localized the mounted NVMe/TCP write gap to backend write cost, not to
network bandwidth or sync/flush time. During Phase 134 implementation we also
found that the existing `backend_write_ops` counter only measured
`StorageBackend.Write` calls from the frontend. It did not prove how many
internal storage block writes happened underneath one large frontend write.

## Change

- Added optional `storage.WriteBatcher` for contiguous full-block write batches.
- Routed `StorageBackend.writeBytes` through that seam for aligned full-block
  runs, capped at 64 blocks per batch.
- Kept partial-block and non-contiguous writes on the existing path.
- Disabled batching when `WriteAckRequireObserverAck` is active so strict ACK
  semantics cannot be bypassed.
- Implemented batch support in `memorywal`, `walstore`, and `smartwal`.
- Made `walstore` perform a real batch append path: one validation/admission
  pass, one LSN allocation, and coalesced adjacent WAL bytes where possible.
  Each block still remains an independent WAL record with its own LSN.
- Added `/status/durable` write-profile counters:
  `backend_storage_write_calls`, `backend_storage_write_blocks`,
  `backend_storage_batch_calls`, and `backend_storage_batch_blocks`.
- Extended Phase 120/126 profiling scripts and added the Phase 134 live gate.

## Evidence

Local:

```text
go test ./core/frontend/durable ./core/storage/... -count=1
go test ./core/frontend/nvme ./core/host/volume ./cmd/blockvolume ./cmd/sw-block -count=1
```

Live:

```text
C:\work\swblock.exe run testops/scenarios/nvme-tcp-durable-backend-write-batching-chain.yaml
run bundle: results\20260704-152759-5441
20 actions: 20 passed, 0 failed
```

Gate summary:

```text
phase134_durable_backend_write_batching_status=ok
target_write_observed=true
target_write_bytes=118259712
backend_write_bytes=118259712
backend_write_ops=3634
backend_storage_write_calls=3634
backend_storage_write_blocks=28872
backend_storage_batch_calls=3613
backend_storage_batch_blocks=28851
backend_storage_batching_effective=true
strict_ack_batch_disabled=true
cleanup_status=ok
```

## Boundaries

Phase 134 is a backend execution improvement and instrumentation close gate. It
does not claim a performance SLO, RoCE, NVMe/RDMA, GPU Direct, cuFile/cuObject,
NIXL, or production HA. The next performance phase should use the new counters
to determine whether backend batching improved wall-clock throughput and what
the next bottleneck is.
