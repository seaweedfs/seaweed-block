# Phase 121 QA: Data-Plane Address Capability

## Purpose

Phase 120 proved NVMe/TCP works on the default Kubernetes InternalIP path, but
that path uses management LAN addresses such as `192.168.1.181`. Phase 121
must prove Seaweed Block can carry an explicit frontend/data-plane IP before any
100GbE performance baseline or RDMA claim is made.

## Gate

Run:

```bash
bash scripts/run-phase121-data-plane-address-capability-gate.sh <repo>
```

or with TestOps:

```bash
swblock run testops/scenarios/data-plane-address-capability-chain.yaml
```

## Required PASS Evidence

The summary file must contain:

```text
phase121_data_plane_address_capability_status=ok
management_ip_m01=192.168.1.181
publish_target_ip_m01=10.0.0.181
publish_target_network_class=100gbe_tcp
publish_target_source=configured_data_plane
frontend_transport=tcp
nvme_rdma_supported=false
roce_claim_allowed=false
internal_ip_not_reused_as_performance_target=true
cleanup_status=ok
```

The rendered cluster spec must show:

```text
data_addr: "10.0.0.181:19101"
ctrl_addr: "192.168.1.181:19102"
sw-block.seaweedfs.com/management-ip: "192.168.1.181"
sw-block.seaweedfs.com/frontend-ip: "10.0.0.181"
sw-block.seaweedfs.com/frontend-network-class: "100gbe_tcp"
```

## Boundary

This is a source/local capability gate. It does not install the product and does
not measure throughput. A later live gate may use the same generated values to
run NVMe/TCP over a real 100GbE IP.

## Non-Claims

This gate does not prove NVMe/RDMA, RoCE I/O, GPU Direct, NIXL, cufile,
cuObject, performance SLOs, or published-image readiness.
