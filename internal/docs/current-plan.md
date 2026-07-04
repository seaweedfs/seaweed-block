# Current Plan: Phase 134 Durable Backend Large-Write Batching Gate

Status: planning.

Phase 133 closed the Kubernetes NVMe/TCP mounted correctness gap from Phase
132:

```text
desired path set changes old->new
-> CSI-node owner connects the new desired path
-> CSI-node owner prunes the stale old host path for the same NQN
-> mounted pod UID/I/O are preserved
-> CRD/report/dashboard agree
```

That makes the supported-lab NVMe/TCP mounted path coherent enough to return to
the Phase 126 performance finding. Phase 126 showed that the write gap is not
network-bound and not dominated by sync time:

```text
network_baseline_mibps=4180.60
block_nvme_seq_write_mibps=177.72
local_path_seq_write_mibps=1115.47
block_vs_local_write_ratio=0.159
backend_write_ops=17972
backend_write_duration_ms=33186
backend_sync_ops=9
backend_sync_duration_ms=73
top_bottleneck=backend_write
next_recommendation=phase127_durable_backend_write_batching
```

The next product change should therefore target backend write fan-out, not
RoCE/NVMe-RDMA.

## Goal

```text
large sequential mounted NVMe/TCP write
-> target still receives the same bytes
-> durable backend writes fewer/larger physical write operations
-> read-back data remains byte-correct
-> backend write counters prove the optimization path was exercised
-> cleanup remains clean
```

This phase is an optimization gate, but it must still be correctness-first:
partial-block writes, repeated writes to the same LBA, and sync/flush semantics
must remain safe.

## Required Evidence

```text
phase134_durable_backend_write_batching_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
target_write_observed=true
target_write_bytes=<bytes>
backend_write_bytes=<same bytes or justified physical bytes>
backend_write_ops_before=<from baseline or control run>
backend_write_ops_after=<optimized run>
backend_write_op_reduction_ratio=<after/before>
backend_write_duration_ms=<observed>
backend_sync_ops=<observed>
read_after_write_verified=true
partial_block_regression_passed=true
same_lba_overwrite_regression_passed=true
cleanup_status=ok
```

The exact throughput number is evidence, not a product claim. PASS requires the
operation count and correctness assertions, not a fixed MiB/s target.

## Boundaries

- Do not claim NVMe/RDMA, RoCE, GPU Direct, cuFile/cuObject, NIXL, production
  HA, broad host compatibility, or performance/SLO.
- Do not optimize by dropping durability, skipping sync semantics, or assuming
  sequential-only writes globally.
- Do not add broad configurability unless the benchmark proves the default
  needs a guard.
- Do not change frontend publication, failover, reconnect, or Kubernetes
  lifecycle behavior in this phase.

## Candidate Implementation

1. Inspect the durable backend write path and identify where large sequential
   target writes are split into many small backend writes.
2. Add a minimal batching/coalescing path for contiguous full-block writes.
3. Preserve the existing direct path for partial-block or non-contiguous writes
   unless a safe merge is obvious.
4. Add unit tests for:
   - contiguous full-block batching;
   - partial-block writes preserving untouched bytes;
   - same-LBA overwrite ordering;
   - sync/flush counter behavior.
5. Extend the Phase 126 gate or add a Phase134 wrapper that runs the same
   mounted NVMe/TCP profile shape and captures before/after backend counters.
6. Update docs only after the gate proves correctness and product-owned
   counters show the optimized path.

## Exit Criteria

Phase 134 can close only if local correctness tests and a live supported-lab
gate both pass. If the optimization does not reduce backend write fan-out, keep
the instrumentation and document the next bottleneck instead of claiming a
performance improvement.
