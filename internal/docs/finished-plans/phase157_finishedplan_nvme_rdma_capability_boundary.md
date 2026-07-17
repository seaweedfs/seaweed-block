# Phase 157 Finished Plan: NVMe/RDMA Capability Boundary

Status: **closed 2026-07-17, local gate PASS**.

## Problem

Seaweed Block has strong NVMe/TCP supported-lab evidence and separate external
RDMA/VFS/object evidence in the mono RDMA work. The risk was treating host RDMA
capability or external-library success as a Block NVMe/RDMA product claim.

## Work

Phase 157 added a release-boundary document and gate that:

- keeps NVMe/RDMA and RoCE as product non-claims;
- separates host capability inputs from volume-server/product capability;
- names the current product gap: no real NVMe-oF/RDMA listener or Kubernetes
  publish/attach path;
- defines the minimum gates required before any claim;
- recommends a read-only volume capability probe as the next step.

## Evidence

```text
phase157_nvme_rdma_capability_boundary_status=ok
current_nvme_tcp_supported_lab_status=source_gated
current_roce_claim_allowed=false
current_nvme_rdma_claim_allowed=false
rdma_host_capability_inputs_documented=true
rdma_volume_server_capability_inputs_documented=true
rdma_transport_product_gap_documented=true
required_live_io_gate_documented=true
required_k8s_publish_gate_documented=true
phase157_decision=keep_nvme_rdma_non_claim_until_product_transport_gates
next_recommendation=phase158_nvme_rdma_volume_capability_probe
```

## Conclusion

The next phase should add a read-only capability surface for current frontend
transport support/refusal before any RDMA data path implementation.
