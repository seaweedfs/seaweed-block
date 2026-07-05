# Phase 138 Finished Plan: WAL WriteAt Shape Profile

Status: **closed 2026-07-05, live gate PASS**.

## Problem

Phase 137 reduced the WAL encode/copy seam and moved the dominant
backend-internal cost to WAL append/write-at. The product still did not expose
whether that cost came from syscall count, small coalesced write size, or
circular-WAL wrap/padding behavior.

## Work

Phase 138 added product-owned write-at shape counters to `/status/durable`:

- WAL append write-at call count.
- WAL append write-at bytes.
- WAL append write-at max bytes.
- Derived average write-at bytes.
- WAL wrap count.
- WAL padding bytes.

The phase did not optimize the write path. It only made the shape visible.

## Gate

Phase 138 added:

- `scripts/run-phase138-wal-writeat-shape-profile-gate.sh`
- `testops/scenarios/nvme-tcp-wal-writeat-shape-profile-chain.yaml`

The gate reruns the 512MiB mounted NVMe/TCP profile, requires active backend
batching, asserts the new write-at counters are populated, and classifies the
next bottleneck.

## Evidence

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

Run bundle:

```text
results\20260705-153411-45ea
22 actions: 22 passed, 0 failed
```

## Conclusion

The measured shape is small coalesced writes: about 33KB average and max
write-at size, with negligible wrap/padding. Phase 139 should improve or
further explain the batch/coalescing shape that feeds the WAL writer. This is
still a source/lab profile, not a published performance, RoCE, or NVMe/RDMA
claim.
