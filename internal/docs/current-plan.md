# Current Plan: Phase 122 NVMe/TCP 100GbE Live Baseline

Status: planning and implementation.

Phase 120 measured the default NVMe/TCP path on the Kubernetes InternalIP /
management LAN (`192.168.1.x`). Phase 121 closed the configuration gap: block
nodes can now carry an explicit frontend/data-plane IP (`10.0.0.x`) while
status preserves the management IP and records the selected network class.

Phase 122 should now run the live baseline that Phase 120 was not allowed to
claim.

## Goal

Measure the current Kubernetes NVMe/TCP path over the configured 100GbE TCP
frontend address, without claiming RoCE or NVMe/RDMA.

Required evidence shape:

```text
phase122_nvme_tcp_100gbe_baseline_status=ok
management_ip=<192.168.1.x>
publish_target=<10.0.0.x>:4420
publish_target_network_class=100gbe_tcp
publish_target_source=configured_data_plane
frontend_transport=tcp
nvme_rdma_supported=false
roce_claim_allowed=false
seq_write_mibps=<number>
seq_read_mibps=<number>
small_write_iops=<number>
cleanup_status=ok
```

## Why This Is Next

The project has two different questions:

1. Can Block publish NVMe/TCP through Kubernetes CSI? Already yes.
2. Is the data path fast enough on the intended 100GbE fabric? Unknown until
   the target is bound to a 100GbE IP and measured.

Running more RDMA or GPU design before this number would be premature. If
NVMe/TCP over 100GbE is already bottlenecked elsewhere, that bottleneck should
guide NVMe/RDMA scope.

## Deliverables

1. Extend the Phase 120 performance gate to pass `--frontend-ip-map` and
   `--frontend-network-class 100gbe_tcp`.

2. Add route/interface evidence that the publish target is not a management LAN
   `192.168.1.x` address.

3. Record the same metrics as Phase 120:

   ```text
   seq_write_mibps
   seq_read_mibps
   small_write_iops
   ```

4. Preserve explicit non-claims:

   ```text
   frontend_transport=tcp
   nvme_rdma_supported=false
   roce_claim_allowed=false
   performance_slo_claim_allowed=false
   ```

## Exit Criteria

- Generated Helm values include the 100GbE frontend IP map.
- Rendered cluster-spec uses `10.0.0.x` for `data_addr` and `192.168.1.x` for
  `ctrl_addr`.
- The live publish target is `<10.0.0.x>:4420`.
- Writer/reader and the three baseline measurements complete.
- Report/CRD/operator-snapshot evidence shows the configured data-plane network
  class.
- Cleanup verifier reports zero residue.

## Non-Claims

Phase 122 still does not implement or validate NVMe/RDMA, RoCE transport, GPU
Direct, cuFile/cuObject, NIXL production acceleration, broad host compatibility,
or performance SLOs.
