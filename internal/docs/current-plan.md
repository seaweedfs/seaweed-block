# Current Plan: Phase 135 Post-Batch NVMe/TCP Write-Path Retriage

Status: planning.

Phase 134 closed the first backend execution fix after the Phase 126 write-path
finding:

```text
large mounted NVMe/TCP write
-> StorageBackend sees frontend writes
-> contiguous full-block runs use bounded storage WriteBatch
-> walstore batches admission/LSN allocation/WAL append
-> /status/durable exposes backend_storage_* counters
-> live gate proves backend_storage_write_calls < backend_storage_write_blocks
```

The next step is not another protocol feature. It is to rerun the same
supported-lab write-path profile with the new counters and decide what actually
dominates after batching.

## Goal

```text
mounted NVMe/TCP write profile after Phase 134 batching
-> target/backend/storage counters agree
-> wall-clock write/read numbers are captured
-> local-path comparator is captured in the same run
-> batch counters prove the optimized path is active
-> next bottleneck is classified from evidence
-> no unsupported performance or RDMA claim is made
```

## Required Evidence

```text
phase135_nvme_tcp_post_batch_retriage_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
network_baseline_mibps=<observed or unavailable-with-reason>
block_nvme_seq_write_mibps=<observed>
block_nvme_seq_read_mibps=<observed>
local_path_seq_write_mibps=<observed>
local_path_seq_read_mibps=<observed>
block_vs_local_write_ratio=<observed>
target_write_observed=true
target_write_bytes=<bytes>
backend_write_bytes=<bytes>
backend_storage_write_calls=<calls>
backend_storage_write_blocks=<blocks>
backend_storage_batch_calls=<calls>
backend_storage_batch_blocks=<blocks>
backend_storage_batching_effective=true
backend_sync_ops=<observed>
backend_sync_duration_ms=<observed>
post_batch_bottleneck=<backend_write|backend_sync|target_protocol|benchmark_shape|unknown>
next_recommendation=<specific next phase>
cleanup_status=ok
```

## Boundaries

- Do not claim a fixed MiB/s SLO.
- Do not claim RoCE, NVMe/RDMA, GPU Direct, cuFile/cuObject, or NIXL.
- Do not start NVMe/RDMA until the TCP path's post-batch bottleneck is named.
- Do not add another optimization before the profile identifies the next
  dominant cost.

## Candidate Work

1. Extend the Phase 134 wrapper or add a Phase 135 wrapper that runs the
   Phase 125/126 profile shape with the new `backend_storage_*` counters.
2. Prefer the previous 512MiB profile size for comparability unless the lab is
   constrained; if a smaller size is used, mark it as non-comparable.
3. Keep the same 100GbE frontend IP map (`m01=10.0.0.1,m02=10.0.0.3`) and
   expected route device (`enp1s0np0`).
4. Classify the next bottleneck from evidence:
   - backend write still dominates -> inspect WAL append / memcpy / checksum;
   - backend sync dominates -> group commit / flush cadence;
   - target protocol dominates -> NVMe target copy/queue path;
   - local-path is also slow -> benchmark/lab shape.
5. Update the roadmap/release docs with the classification only after the gate
   passes.

## Exit Criteria

Phase 135 can close when the live supported-lab gate produces a clean profile
with active batch counters and a concrete next recommendation. If evidence is
ambiguous, close as instrumentation-only and name the missing counter instead
of guessing.
