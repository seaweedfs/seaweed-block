# Phase 138 WAL WriteAt Shape Profile QA Sign-Off

Verdict: **PASS**.

Validated source tree: local `phase138-wal-writeat-shape-profile` working tree,
synced to `m02:/tmp/seaweed_block` as clean `HEAD` plus Phase 138 overlay only.
The known unrelated dirty NVMe files were not copied into the lab tree.

Run:

```powershell
C:\work\swblock.exe run testops/scenarios/nvme-tcp-wal-writeat-shape-profile-chain.yaml `
  -output results\phase138-wal-writeat-shape-run1.json `
  -html results\phase138-wal-writeat-shape-run1.html
```

Run bundle:

```text
results\20260705-153411-45ea
22 actions: 22 passed, 0 failed
```

## Evidence

```text
phase138_wal_writeat_shape_profile_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
profile_size_mib=512
phase137_wal_append_duration_ms=375
unit_wal_writeat_shape_regression_passed=true
target_write_observed=true
target_write_bytes=588075008
backend_write_bytes=588075008
backend_storage_batch_calls=17953
backend_storage_batch_blocks=143555
backend_storage_batching_effective=true
wal_append_ops=17979
wal_append_bytes=593543918
wal_append_duration_ms=380
wal_append_writeat_calls=17979
wal_append_writeat_bytes=593543918
wal_append_writeat_max_bytes=33072
wal_append_writeat_avg_bytes=33013
wal_append_wrap_count=8
wal_append_padding_bytes=13136
wal_copy_duration_ms=96
wal_encode_duration_ms=275
wal_checksum_duration_ms=112
dirty_map_update_duration_ms=73
post_phase138_bottleneck=wal_append_small_writes
next_recommendation=phase139_wal_append_batch_shape_coalescing
cleanup_status=ok
```

## Finding

The append bottleneck is a small-write shape, not wrap/padding:

- `wal_append_writeat_calls=17979`.
- Average write-at size is only `33013` bytes.
- Max write-at size is only `33072` bytes.
- Wrap/padding happened only `8` times with `13136` bytes total padding.

The next phase should inspect and improve the batch/coalescing shape feeding the
WAL writer. This phase is observation only; it does not claim a performance SLO,
RoCE, or NVMe/RDMA.

## Cleanup

The gate reused the Phase 126 cleanup verifier and finished clean:

```text
cleanup_status=ok
```
