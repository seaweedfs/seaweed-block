# Current Plan: Phase 125 Block NVMe/TCP Write-Path Profile

Status: planning and implementation.

Phase 124 split the current NVMe/TCP performance question with an independent
local-path comparator:

```text
network_baseline_mibps=3769.28
block_nvme_seq_write_mibps=118.74
block_nvme_seq_read_mibps=273.50
local_path_seq_write_mibps=324.87
local_path_seq_read_mibps=235.29
block_vs_local_read_ratio=1.162
block_vs_local_write_ratio=0.366
shape_fsync_penalty=1.180
top_bottleneck=block_target_or_backend
cleanup_status=ok
```

The read path is not the immediate problem: Block read was slightly above the
local-path comparator. The write path is the gap: Block write was about 36% of
local-path write on the same app node and same `dd conv=fsync` shape. Phase 125
should profile the Block write path before any NVMe/RDMA implementation work.

## Goal

Identify whether the Block NVMe/TCP write gap is mainly:

```text
blockvolume target CPU / copy path
durable backend write or fsync path
current benchmark shape
instrumentation gap / unknown
```

Required output:

```text
phase125_block_nvme_tcp_write_path_profile_status=ok
network_baseline_mibps=<number>
local_path_seq_write_mibps=<number>
block_nvme_seq_write_mibps=<number>
blockvolume_cpu_peak_percent=<number|unknown>
blockvolume_cpu_avg_percent=<number|unknown>
block_write_duration_ms=<number>
local_write_duration_ms=<number>
write_path_observation=<target_cpu|backend_sync|benchmark_shape|unknown>
top_bottleneck=<target_cpu|backend_sync|benchmark_shape|unknown>
next_recommendation=<specific next phase>
cleanup_status=ok
```

## Why This Is Next

NVMe/RDMA only improves the transport. Phase 124 shows the current TCP network
has far more headroom than the mounted Block path, and the read path is not
behind the local-path comparator. The next useful question is narrower: what in
the write path makes Block much slower than a same-node local-path PVC?

## Deliverables

1. Add a Phase 125 gate that reuses Phase 124 and collects write-time runtime
   evidence:

   ```text
   kubectl top pods --containers during write
   blockvolume process CPU snapshots
   blockvolume logs around write workload
   app pod write duration
   local-path comparator write duration
   ```

2. If cheap and safe, add minimal target-side timing logs or existing debug
   counters for the write path. Do not add a broad observability framework.

3. Preserve non-claims:

   ```text
   nvme_rdma_supported=false
   roce_claim_allowed=false
   performance_slo_claim_allowed=false
   ```

4. Emit one concrete recommendation:

   ```text
   phase126_target_copy_cpu_optimization
   phase126_durable_backend_write_optimization
   phase126_benchmark_shape_correction
   phase126_start_real_nvme_rdma_target
   ```

## Exit Criteria

- Runner scenario passes and archives the evidence bundle.
- Cleanup verifier reports zero residue.
- The gate names a write-path bottleneck class, or records `unknown` with the
  missing evidence needed to classify it.
- No RoCE/NVMe-RDMA or performance/SLO claim is added.

## Non-Claims

Phase 125 still does not implement NVMe/RDMA, RoCE, GPU Direct, cuFile/cuObject,
NIXL, production HA, or a performance SLO. It is a write-path profiling gate for
choosing the next data-plane investment.
