# Current Plan: Phase 127 Durable Backend Large-Write Batching

Status: planning and implementation.

Phase 126 closed the ambiguity left by Phase 125. The write bottleneck is now
product-owned evidence, not a `kubectl top` inference:

```text
network_baseline_mibps=4180.60
block_nvme_seq_write_mibps=177.72
block_nvme_seq_read_mibps=520.85
local_path_seq_write_mibps=1115.47
local_path_seq_read_mibps=536.13
block_vs_local_write_ratio=0.159
block_vs_local_read_ratio=0.971
target_write_observed=true
target_write_bytes=588075008
target_write_ops=17972
target_write_duration_ms=34233
backend_write_bytes=588075008
backend_write_ops=17972
backend_write_duration_ms=33186
backend_sync_ops=9
backend_sync_duration_ms=73
write_path_observation=backend_write
cleanup_status=ok
```

The duration fields are cumulative per-operation handler/backend time, not
wall-clock elapsed time. They are useful for locating the hot path, not for
publishing a throughput SLO.

## Goal

Reduce the durable backend write cost for large sequential NVMe/TCP writes by
optimizing the byte-range to LogicalStorage write path. The first target is the
adapter/storage seam, not the NVMe transport:

```text
frontend byte write -> durable.StorageBackend.writeBytes
-> LogicalStorage.Write per block -> WAL/SmartWAL metadata path
```

## Why This Is Next

Phase 126 showed:

- the 100GbE TCP network is not the bottleneck;
- Block read is comparable to same-node local-path read;
- target and backend write bytes match, so writes are reaching product code;
- backend write cumulative time accounts for nearly all target write time;
- backend sync time is small in this gate.

Starting NVMe/RDMA now would optimize the wrong boundary. The next product
change should reduce the number/cost of backend write operations for large
sequential writes or expose a clearer lower-level storage bottleneck.

## Deliverables

1. Characterize current write fan-out.

   Required evidence:

   ```text
   backend_write_ops=<number>
   backend_write_bytes=<number>
   logical_storage_write_ops=<number if available>
   avg_backend_write_bytes=<number>
   ```

2. Implement one narrow large-write optimization, guarded by tests.

   Candidate approaches:

   ```text
   coalesce aligned contiguous blocks before storage call
   add a LogicalStorage batch-write seam if single-block calls dominate
   reduce unnecessary per-block observer/log overhead in the no-replica path
   ```

3. Rerun the same Phase 126 evidence shape.

   Required output:

   ```text
   phase127_durable_backend_large_write_batching_status=ok
   before_block_nvme_seq_write_mibps=<number>
   after_block_nvme_seq_write_mibps=<number>
   before_backend_write_ops=<number>
   after_backend_write_ops=<number>
   before_backend_write_duration_ms=<number>
   after_backend_write_duration_ms=<number>
   local_path_seq_write_mibps=<number>
   cleanup_status=ok
   ```

4. Preserve non-claims:

   ```text
   nvme_rdma_supported=false
   roce_claim_allowed=false
   performance_slo_claim_allowed=false
   ```

## Exit Criteria

- Unit/component tests prove the optimized path preserves byte-level write/read
  correctness, partial-block behavior, lineage fencing, and sync semantics.
- Runner gate passes and archives before/after evidence.
- Cleanup verifier reports zero residue.
- The result names the next bottleneck honestly if write performance does not
  improve.

## Non-Claims

Phase 127 does not implement NVMe/RDMA, RoCE, GPU Direct, cuFile/cuObject,
NIXL, production HA, broad host compatibility, or a performance SLO. It is a
durable backend write-path optimization phase for the already-supported
NVMe/TCP lab path.
