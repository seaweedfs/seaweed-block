# Current Plan: Phase 124 NVMe/TCP Target / Backend / Shape Split

Status: planning and implementation.

Phase 123 proved the configured data-plane network is not the immediate
bottleneck:

```text
network_baseline_mibps=4106.55
publish_target=10.0.0.1:4420
k8s_mounted_seq_write_mibps=127.74
k8s_mounted_seq_read_mibps=248.06
k8s_mounted_small_write_iops=755.16
cleanup_status=ok
```

The gap is large, but Phase 123 could not distinguish blockvolume target path,
durable backend, Kubernetes mounted filesystem overhead, and current `dd` test
shape. Phase 124 should split those before any NVMe/RDMA implementation work.

## Goal

Compare the current Block NVMe/TCP mounted path against a same-shape Kubernetes
local-path PVC and a small test-shape matrix.

Required output:

```text
phase124_nvme_tcp_target_backend_shape_split_status=ok
network_baseline_mibps=<number>
local_path_seq_write_mibps=<number>
local_path_seq_read_mibps=<number>
block_nvme_seq_write_mibps=<number>
block_nvme_seq_read_mibps=<number>
block_vs_local_read_ratio=<number>
block_vs_local_write_ratio=<number>
shape_fsync_penalty=<number|unknown>
top_bottleneck=<test_shape|k8s_mount|block_target_or_backend|unknown>
next_recommendation=<specific next phase>
cleanup_status=ok
```

## Why This Is Next

If local-path PVC with the same `dd` shape is also slow, the bottleneck is
mostly test shape / mounted filesystem / host behavior. If local-path is much
faster but Block is slow, the next split is inside blockvolume target/backend.
Only if the current target/backend path approaches the host/Kubernetes baseline
does it make sense to spend on NVMe/RDMA.

## Deliverables

1. Add a Phase 124 gate that creates two PVCs in one clean lab run:

   ```text
   local-path PVC
   sw-block NVMe/TCP PVC over 10.0.0.x
   ```

2. Run the same minimal matrix on both:

   ```text
   seq write with fsync
   seq read
   optional seq write without fsync, if safe
   small write loop
   ```

3. Preserve Phase 123 context:

   ```text
   route_dev=enp1s0np0
   network_baseline_mibps=<iperf3>
   nvme_rdma_supported=false
   roce_claim_allowed=false
   performance_slo_claim_allowed=false
   ```

4. Emit a recommendation:

   ```text
   phase125_blockvolume_target_cpu_profile
   phase125_backend_durable_write_profile
   phase125_test_shape_correction
   phase125_start_real_nvme_rdma_target
   ```

## Exit Criteria

- Runner scenario passes and archives the evidence bundle.
- Both PVC paths write/read and clean up.
- The gate names a bottleneck class from ratios, or records `unknown` with a
  specific missing comparator.
- Cleanup verifier reports zero residue.
- No RoCE/NVMe-RDMA or performance/SLO claim is added.

## Non-Claims

Phase 124 still does not implement NVMe/RDMA, RoCE, GPU Direct, cuFile/cuObject,
NIXL, production HA, or a performance SLO. It is a split gate for choosing the
next data-plane investment.
