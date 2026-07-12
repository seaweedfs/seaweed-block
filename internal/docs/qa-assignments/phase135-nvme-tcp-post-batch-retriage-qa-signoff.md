# Phase 135 NVMe/TCP Post-Batch Write-Path Retriage QA Sign-Off

Verdict: **PASS**.

Validated source tree: local `phase135-post-batch-retriage` working tree synced
to `m02:/tmp/seaweed_block`.

Run:

```powershell
C:\work\swblock.exe run testops/scenarios/nvme-tcp-post-batch-retriage-chain.yaml `
  -output results\phase135-post-batch-retriage-run1.json `
  -html results\phase135-post-batch-retriage-run1.html
```

Run bundle:

```text
results\20260704-153919-e781
18 actions: 18 passed, 0 failed
```

## Evidence

```text
phase135_nvme_tcp_post_batch_retriage_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
profile_size_mib=512
profile_comparable_with_phase126=true
network_baseline_mibps=4099.38
block_nvme_seq_write_mibps=172.80
block_nvme_seq_read_mibps=531.67
local_path_seq_write_mibps=1075.63
local_path_seq_read_mibps=531.67
block_vs_local_write_ratio=0.161
block_vs_local_read_ratio=1.000
target_write_observed=true
target_write_bytes=588075008
target_write_ops=17971
target_write_duration_ms=27124
backend_write_bytes=588075008
backend_write_ops=17971
backend_write_duration_ms=25953
backend_storage_write_calls=17971
backend_storage_write_blocks=143573
backend_storage_batch_calls=17953
backend_storage_batch_blocks=143555
backend_storage_batching_effective=true
backend_sync_ops=8
backend_sync_duration_ms=42
post_batch_bottleneck=backend_write
next_recommendation=phase136_wal_append_copy_checksum_profile
cleanup_status=ok
```

## Finding

The Phase 134 batch path is active at scale, but the live post-batch profile
still localizes the write gap to backend write work:

- Phase 126 comparable write: `177.72 MiB/s`.
- Phase 135 comparable write: `172.80 MiB/s`.
- Local-path comparator remains much faster: `1075.63 MiB/s`.
- Sync cost remains small: `backend_sync_duration_ms=42`.
- Network comparator remains much faster: `4099.38 MiB/s`.

The next phase should inspect `walstore`/durable backend internals below the
batch seam: WAL append encode/copy/checksum, dirty-map update, per-record
logging, and write amplification. Starting NVMe/RDMA now would not address the
measured bottleneck.

## Cleanup

Final cleanup verification on m02:

```text
cleanup_status=ok
k8s_residue_count=0
iscsi_residue_count=0
process_residue_count=0
multipath_residue_count=0
hostpath_residue_count=0
failure_count=0
```
