# Phase 121 Finished Plan: Data-Plane Address Capability

Status: closed, QA PASS on 2026-07-02.

## Problem

Phase 120 measured NVMe/TCP through the default Kubernetes InternalIP path,
which in the lab resolves to management LAN addresses such as
`192.168.1.181:4420`. That is valid functional evidence, but it is not a
100GbE performance baseline and must not be confused with RoCE/NVMe-RDMA.

## What Changed

- `sw-block ops generate-helm-values` can now accept:

  ```text
  --frontend-ip-map m01=10.0.0.181,m02=10.0.0.184,tp01=10.0.0.188
  --frontend-network-class 100gbe_tcp
  ```

- Helm values now carry optional `managementIP`, `frontendIP`, and
  `frontendNetworkClass` per block node.

- The chart renders:

  ```text
  data_addr = frontendIP:dataPort
  ctrl_addr = internalIP:controlPort
  labels:
    sw-block.seaweedfs.com/management-ip
    sw-block.seaweedfs.com/frontend-ip
    sw-block.seaweedfs.com/frontend-network-class
  ```

- Master observation, operator-snapshot, and `SwBlockCluster.status.nodes[]`
  preserve `internalIP` as management evidence while optionally surfacing
  `frontendIP` and `frontendNetworkClass`.

- CRD and values schema were updated so the new status/config fields are not
  pruned by Kubernetes schema validation.

## Verification

Source/local checks:

```text
go test ./core/host/master ./core/lifecycle ./core/ops ./cmd/sw-block
helm lint charts/seaweed-block
helm template ... frontendIP=10.0.0.181 ...
```

Phase gate:

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

QA sign-off:

```text
internal/docs/qa-assignments/phase121-data-plane-address-capability-qa-signoff.md
```

## Non-Claims

Phase 121 does not implement NVMe/RDMA, RoCE I/O, GPU Direct, NIXL, cufile,
cuObject, performance SLOs, broad host compatibility, or a live throughput
baseline. It only makes the address/capability model explicit enough for the
next live 100GbE NVMe/TCP baseline.
