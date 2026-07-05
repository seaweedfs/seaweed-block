# Phase 137 Reduce WAL Record Encode / Copy QA Sign-Off

Verdict: **PASS**.

Validated source tree: local `phase137-reduce-wal-record-encode-copy` working
tree, synced to `m02:/tmp/seaweed_block` as clean `HEAD` plus Phase 137 overlay
only. The known unrelated dirty NVMe files were not copied into the lab tree.

Run:

```powershell
C:\work\swblock.exe run testops/scenarios/nvme-tcp-reduce-wal-record-encode-copy-chain.yaml `
  -output results\phase137-wal-encode-copy-run1.json `
  -html results\phase137-wal-encode-copy-run1.html
```

Run bundle:

```text
results\20260705-090453-2584
24 actions: 24 passed, 0 failed
```

## Evidence

```text
phase137_reduce_wal_record_encode_copy_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
profile_size_mib=512
phase136_wal_encode_duration_ms=753
phase136_wal_copy_duration_ms=593
phase136_wal_encode_copy_duration_ms=1346
unit_wal_encode_copy_regression_passed=true
preencode_data_copy_removed=true
batch_append_encodes_direct_to_pending=true
target_write_observed=true
target_write_bytes=588075008
backend_write_bytes=588075008
backend_storage_batch_calls=17953
backend_storage_batch_blocks=143555
backend_storage_batching_effective=true
wal_copy_ops=143573
wal_copy_bytes=588075008
wal_copy_duration_ms=93
wal_encode_ops=143573
wal_encode_bytes=593530782
wal_encode_duration_ms=270
wal_checksum_ops=143573
wal_checksum_bytes=592382198
wal_checksum_duration_ms=110
wal_append_ops=17979
wal_append_bytes=593543918
wal_append_duration_ms=375
dirty_map_update_ops=143573
dirty_map_update_duration_ms=74
wal_encode_copy_duration_ms=363
wal_encode_copy_reduced_vs_phase136=true
post_phase137_bottleneck=wal_append
next_recommendation=phase138_wal_writeat_shape_profile
cleanup_status=ok
```

## Finding

Phase 137 reduced the WAL encode/copy seam without changing the WAL format or
recovery semantics:

- `wal_encode + wal_copy` dropped from `1346ms` to `363ms`.
- The extra pre-encode block copy is gone.
- Batch append now encodes records directly into the coalesced pending buffer,
  instead of encoding each record into a temporary slice and copying it again.
- Regression tests prove caller-buffer mutation after `Write` / `WriteBatch`
  does not change reads or recovered bytes.

The bottleneck moved to WAL append/write-at shape: `wal_append_duration_ms=375`.
The next phase should inspect pwrite count/shape/coalescing before any NVMe/RDMA
or SLO claim.

## Cleanup

The gate reused the Phase 126 cleanup verifier and finished clean:

```text
cleanup_status=ok
```
