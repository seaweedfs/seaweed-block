# Current Plan: Phase 136 WAL Append / Copy / Checksum Profile

Status: planning.

Phase 135 reran the 512MiB supported-lab NVMe/TCP write profile after Phase 134
batching. The batch path was active, but wall-clock write behavior remained in
the same range as Phase 126:

```text
phase135_nvme_tcp_post_batch_retriage_status=ok
block_nvme_seq_write_mibps=172.80
local_path_seq_write_mibps=1075.63
block_vs_local_write_ratio=0.161
backend_write_duration_ms=25953
backend_storage_write_calls=17971
backend_storage_write_blocks=143573
backend_storage_batch_calls=17953
backend_storage_batch_blocks=143555
backend_sync_duration_ms=42
post_batch_bottleneck=backend_write
next_recommendation=phase136_wal_append_copy_checksum_profile
```

This means the next useful work is not NVMe/RDMA. It is to split backend write
cost below the batch seam.

## Goal

```text
mounted NVMe/TCP write still reaches durable backend
-> backend write time is split into named internal costs
-> WAL record encode/copy/checksum cost is visible
-> WAL append/write-at cost is visible
-> dirty-map/update/bookkeeping cost is visible
-> next bottleneck is named from counters
-> cleanup remains clean
```

## Required Evidence

```text
phase136_wal_append_copy_checksum_profile_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
target_write_observed=true
backend_storage_batching_effective=true
wal_encode_ops=<count>
wal_encode_bytes=<bytes>
wal_encode_duration_ms=<duration>
wal_append_ops=<count>
wal_append_bytes=<bytes>
wal_append_duration_ms=<duration>
wal_checksum_ops=<count>
wal_checksum_bytes=<bytes>
wal_checksum_duration_ms=<duration>
dirty_map_update_ops=<count>
dirty_map_update_duration_ms=<duration>
post_phase136_bottleneck=<wal_encode|wal_append|checksum|dirty_map|other|unknown>
next_recommendation=<specific next phase>
cleanup_status=ok
```

## Boundaries

- Do not optimize in this phase unless the counter insertion itself exposes a
  trivial bug.
- Do not add broad tracing frameworks or logging in the data path.
- Do not claim performance improvement, SLO, RoCE, NVMe/RDMA, GPU Direct,
  cuFile/cuObject, or NIXL.
- Do not weaken WAL recovery semantics: every write still needs an LSN and a
  recoverable record.

## Candidate Work

1. Add low-overhead atomic duration/byte/op counters around `walEntry.encode`,
   checksum work, `walWriter.append/appendBatch`, and dirty-map updates.
2. Expose those counters through `/status/durable` under `WriteProfile`.
3. Extend the Phase 135 wrapper or add a Phase 136 wrapper that asserts the new
   fields are populated during a mounted NVMe/TCP write.
4. Classify the next bottleneck:
   - WAL encode/copy/checksum dominates -> reduce record copy/checksum work;
   - WAL append/write-at dominates -> inspect pwrite shape/write amplification;
   - dirty-map dominates -> inspect map/shard/update path;
   - unknown -> add the missing counter instead of guessing.

## Exit Criteria

Phase 136 can close when the live supported-lab gate names the backend-internal
dominant cost from product-owned counters and cleanup is clean. If no single
cost dominates, close with the measured distribution and a concrete next
experiment.
