# Phase 134 Durable Backend Write Batching QA Sign-Off

Verdict: **PASS**.

Validated source tree: local `phase134-durable-write-batching` working tree
synced to `m02:/tmp/seaweed_block`.

Run:

```powershell
C:\work\swblock.exe run testops/scenarios/nvme-tcp-durable-backend-write-batching-chain.yaml `
  -output results\phase134-durable-write-batching-run1.json `
  -html results\phase134-durable-write-batching-run1.html
```

Run bundle:

```text
results\20260704-152759-5441
20 actions: 20 passed, 0 failed
```

## Evidence

```text
phase134_durable_backend_write_batching_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
batch_scope=contiguous_full_block_writes
batch_max_blocks=64
unit_batch_regression_passed=true
strict_ack_batch_disabled=true
read_after_write_regression_passed=true
target_write_observed=true
target_write_bytes=118259712
backend_write_bytes=118259712
backend_write_ops=3634
backend_write_duration_ms=11192
backend_storage_write_calls=3634
backend_storage_write_blocks=28872
backend_storage_batch_calls=3613
backend_storage_batch_blocks=28851
backend_storage_batching_effective=true
backend_sync_ops=9
cleanup_status=ok
```

## What This Proves

- The live mounted NVMe/TCP path still writes through the durable backend and
  exposes target/backend counters through `/status/durable`.
- Contiguous full-block writes exercise the new bounded batch path:
  `backend_storage_batch_calls=3613` and
  `backend_storage_batch_blocks=28851`.
- Internal storage write-call count is lower than block fan-out:
  `3634 < 28872`.
- The strict ACK safety guard is covered by unit tests:
  batch-shaped writes with `WriteAckRequireObserverAck` and no observer fail
  before a local batch write.
- Cleanup is clean.

## Boundaries

This is not a performance SLO, RoCE, NVMe/RDMA, GPU Direct, cuFile/cuObject, or
NIXL claim. The wall-clock write number is evidence only. Phase 134 proves the
batch seam and counters, not production-grade throughput.

The existing `backend_write_ops` counter remains a frontend/backend API call
counter. Phase 134 adds separate `backend_storage_*` counters because
`backend_write_ops` alone cannot prove internal storage fan-out reduction.

## Additional Local Checks

```text
go test ./core/frontend/durable ./core/storage/... -count=1
go test ./core/frontend/nvme ./core/host/volume ./cmd/blockvolume ./cmd/sw-block -count=1
```

Both passed.

One unrelated local process-harness test still failed during the wider check:

```text
go test ./core/frontend/iscsi -run TestT2Process_ISCSI_ReopenAfterMove_ServesNewLineage -count=1 -v
projection did not return to Healthy ... within 30s
```

That test uses a 512-byte partial iSCSI write and does not exercise the new
full-block batch path. It should be tracked separately as an iSCSI/process
harness flake or bug, not as a Phase 134 blocker.
