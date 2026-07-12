# Phase 135 Finished Plan: NVMe/TCP Post-Batch Write-Path Retriage

Status: **closed 2026-07-04, live gate PASS**.

## Problem

Phase 134 proved the durable backend batch path and new
`backend_storage_*` counters. The remaining question was whether batching
changed the supported-lab NVMe/TCP write bottleneck or simply shifted the next
dominant cost lower inside the backend.

## Gate

Phase 135 added:

- `scripts/run-phase135-nvme-tcp-post-batch-retriage-gate.sh`
- `testops/scenarios/nvme-tcp-post-batch-retriage-chain.yaml`

The gate reuses the Phase 125/126 profile shape at 512MiB for comparability,
requires active `backend_storage_*` batch counters, and emits a post-batch next
recommendation.

## Evidence

```text
phase135_nvme_tcp_post_batch_retriage_status=ok
profile_size_mib=512
profile_comparable_with_phase126=true
network_baseline_mibps=4099.38
block_nvme_seq_write_mibps=172.80
block_nvme_seq_read_mibps=531.67
local_path_seq_write_mibps=1075.63
block_vs_local_write_ratio=0.161
target_write_observed=true
target_write_bytes=588075008
backend_write_bytes=588075008
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

Run bundle:

```text
results\20260704-153919-e781
18 actions: 18 passed, 0 failed
```

## Conclusion

Batching is active, but the comparable mounted NVMe/TCP write result remains in
the same range as Phase 126 (`172.80 MiB/s` versus `177.72 MiB/s`) while the
local-path comparator remains much faster. Sync and network are not the
dominant cost. The next phase should profile the backend below the batch seam:
WAL record encode/copy/checksum, dirty-map updates, and write amplification.

Do not start NVMe/RDMA or make a performance/SLO claim from this evidence.
