# Current Plan: Phase 123 NVMe/TCP Performance Bottleneck Triage

Status: planning and implementation.

Phase 122 closed the 100GbE TCP frontend-address baseline:

```text
publish_target=10.0.0.1:4420
publish_target_route_dev=enp1s0np0
seq_write_mibps=115.11
seq_read_mibps=250.98
small_write_iops=606.64
cleanup_status=ok
```

This proves the path is on the configured data-plane network, but it does not
prove a performance claim. The measured throughput is far below what the
network can carry, so the next work should identify the bottleneck before
adding NVMe/RDMA or GPU-direct complexity.

## Goal

Produce an evidence-backed bottleneck map for the current Kubernetes NVMe/TCP
path.

Required output:

```text
phase123_nvme_tcp_bottleneck_triage_status=ok
publish_target=<10.0.0.x>:4420
route_dev=enp1s0np0
network_baseline_mibps=<number>
host_local_nvme_tcp_mibps=<number>
k8s_mounted_nvme_tcp_mibps=<number>
top_bottleneck=<network|target|backend|k8s_attach|fio_shape|unknown>
next_recommendation=<specific next phase>
cleanup_status=ok
```

## Why This Is Next

If NVMe/TCP is limited by the blockvolume process, backend durability path, or
test shape, NVMe/RDMA will not automatically fix the product bottleneck. If the
limit is actually network or host configuration, RDMA work would also be
misleading. Phase 123 should make the next engineering decision evidence-based.

## Deliverables

1. A diagnostic gate that reuses the Phase 122 frontend map and captures:

   ```text
   ip route get <publish-target-host>
   iperf3 or equivalent network baseline when available
   CPU snapshot during fio
   fio profile parameters
   blockvolume logs around NVMe/TCP I/O
   ```

2. A small fio matrix, not a broad benchmark suite:

   ```text
   sequential write/read, current shape
   sequential write/read with higher iodepth if supported
   small write, current shape
   ```

3. A comparison between:

   ```text
   host/network baseline
   direct or host-local target path if available
   Kubernetes mounted PVC path
   ```

4. A closed recommendation:

   ```text
   optimize NVMe/TCP target path
   tune fio/test shape
   investigate backend/durable store
   start real NVMe/RDMA target
   defer RDMA and improve status/docs only
   ```

## Exit Criteria

- The gate runs from a clean lab and leaves `cleanup_status=ok`.
- The publish target is still the configured 100GbE TCP address, not the
  management LAN.
- At least one independent network or host-path comparator exists, or the gate
  records why it cannot be collected.
- The report names a likely bottleneck with evidence, not intuition.
- RoCE/NVMe-RDMA remains a non-claim unless a real RDMA target moves bytes.

## Non-Claims

Phase 123 still does not implement NVMe/RDMA, RoCE, GPU Direct, cuFile/cuObject,
NIXL, production HA, or a performance SLO. It is a decision gate for the next
data-plane investment.
