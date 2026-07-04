# Phase 136 WAL Append / Copy / Checksum Profile QA Sign-Off

Verdict: **PASS**.

Validated source tree: local `phase136-wal-append-profile` working tree, synced
to `m02:/tmp/seaweed_block` as clean `HEAD` plus Phase 136 overlay only. The
known unrelated dirty NVMe test/runtime files were not copied into the lab tree.

Run:

```powershell
C:\work\swblock.exe run testops/scenarios/nvme-tcp-wal-append-copy-checksum-profile-chain.yaml `
  -output results\phase136-wal-profile-run1.json `
  -html results\phase136-wal-profile-run1.html
```

Run bundle:

```text
results\20260704-160121-60c2
24 actions: 24 passed, 0 failed
```

## Evidence

```text
phase136_wal_append_copy_checksum_profile_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
profile_size_mib=512
unit_wal_profile_regression_passed=true
target_write_observed=true
target_write_bytes=588075008
backend_write_bytes=588075008
backend_storage_batch_calls=17952
backend_storage_batch_blocks=143552
backend_storage_batching_effective=true
wal_copy_ops=143573
wal_copy_bytes=588075008
wal_copy_duration_ms=593
wal_encode_ops=143573
wal_encode_bytes=593530782
wal_encode_duration_ms=753
wal_checksum_ops=143573
wal_checksum_bytes=592382198
wal_checksum_duration_ms=100
wal_append_ops=17981
wal_append_bytes=593543918
wal_append_duration_ms=338
dirty_map_update_ops=143573
dirty_map_update_duration_ms=67
post_phase136_bottleneck=wal_encode
next_recommendation=phase137_reduce_wal_record_encode_copy
cleanup_status=ok
```

## Finding

The live 512MiB mounted NVMe/TCP path still reaches the durable backend and
the Phase 134 batch path is still active. The new product-owned counters split
the backend-internal cost:

- WAL record encode is the largest measured internal cost: `753ms`.
- WAL data copy is close behind: `593ms`.
- WAL append/write-at is smaller but visible: `338ms`.
- Checksum and dirty-map update are not dominant in this run.

The next phase should reduce WAL record encode/copy cost before returning to
NVMe/RDMA, RoCE, or broader transport work. This is profiling evidence, not a
performance/SLO claim.

## Cleanup

The gate reused the Phase 126 cleanup verifier and finished clean:

```text
cleanup_status=ok
```
