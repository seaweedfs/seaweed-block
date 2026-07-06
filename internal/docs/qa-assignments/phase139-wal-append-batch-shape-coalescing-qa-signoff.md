# Phase 139 WAL Append Batch Shape Coalescing QA Sign-Off

Verdict: **PASS**.

Validated source tree: local `phase139-wal-append-batch-shape` working tree,
synced to `m02:/tmp/seaweed_block` as clean `HEAD` plus Phase 139 overlay only.
The known unrelated dirty NVMe files were not copied into the lab tree.

Run:

```powershell
C:\work\swblock.exe run testops/scenarios/nvme-tcp-wal-append-batch-shape-coalescing-chain.yaml `
  -output results\phase139-batch-shape-run1.json `
  -html results\phase139-batch-shape-run1.html
```

Run bundle:

```text
results\20260706-134824-6806
22 actions: 22 passed, 0 failed
```

## Evidence

```text
phase139_wal_append_batch_shape_coalescing_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
profile_size_mib=512
phase138_wal_append_writeat_avg_bytes=33013
phase138_wal_append_writeat_max_bytes=33072
phase138_wal_append_writeat_calls=17979
unit_batch_shape_regression_passed=true
target_write_observed=true
target_write_bytes=588066816
backend_write_bytes=588066816
backend_write_request_ops=17971
backend_write_request_bytes=588066816
backend_write_request_max_bytes=32768
backend_write_request_avg_bytes=32723
backend_storage_batch_calls=17952
backend_storage_batch_blocks=143552
backend_full_block_batch_calls=17952
backend_full_block_batch_blocks=143552
backend_full_block_batch_max=8
backend_full_block_batch_avg=7
wal_append_ops=17979
wal_append_bytes=593535650
wal_append_duration_ms=379
wal_append_writeat_calls=17979
wal_append_writeat_bytes=593535650
wal_append_writeat_max_bytes=33072
wal_append_writeat_avg_bytes=33012
wal_copy_duration_ms=93
wal_encode_duration_ms=269
wal_checksum_duration_ms=110
dirty_map_update_duration_ms=76
backend_storage_batching_effective=true
phase139_shape_result=frontend_request_limited
post_phase139_bottleneck=wal_append_small_writes
next_recommendation=phase140_frontend_request_size_profile
cleanup_status=ok
```

## Finding

The 33KB WAL write-at shape is imposed by the frontend request shape:

- `backend_write_request_max_bytes=32768`.
- `backend_full_block_batch_max=8`.
- `wal_append_writeat_max_bytes=33072`, which is 8 WAL records with fixed
  per-record overhead.

The WAL writer is coalescing each received batch correctly. Larger coalescing
inside the WAL writer would require holding multiple frontend `Write` calls
open or changing ACK timing, so this phase correctly closes as
`frontend_request_limited` rather than forcing a speculative backend buffer.

The next phase should profile or adjust frontend request size. This phase does
not claim a performance SLO, RoCE, or NVMe/RDMA.

## Cleanup

The gate reused the Phase 126 cleanup verifier and finished clean:

```text
cleanup_status=ok
```
