# Current Plan: Phase 157 NVMe/RDMA Capability Boundary

Status: planning.

Phase 156 closed the WAL multi-block release-smoke decision:

```text
phase156_wal_multiblock_release_smoke_decision_status=ok
runtime_opt_in_name=durable-wal-multiblock-records
default_wal_format_unchanged=true
source_gated_status=kept
published_image_smoke_required=true
published_image_smoke_scope=explicit_opt_in_recovery_status
recovery_test_disable_flusher_user_claim=false
performance_slo_claim_allowed=false
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
phase156_decision=keep_source_gated_until_matching_image_smoke
next_recommendation=phase157_nvme_rdma_capability_boundary
```

## Goal

Define the next NVMe/RDMA boundary before implementation. The project already
has a TCP NVMe supported-lab path and has explicit RoCE/NVMe-RDMA non-claims.
Before adding code, Phase 157 should identify what product evidence would make
NVMe/RDMA a real claim versus a host capability or external RDMA library
experiment.

## Required Evidence

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
performance_slo_claim_allowed=false
next_recommendation=<specific next phase>
```

## Boundaries

- Do not claim NVMe/RDMA or RoCE from host capability alone.
- Do not claim acceleration without same-shape baseline and product-owned
  evidence.
- Do not mix the external RDMA library experiment with the Block product claim
  unless a concrete transport path is wired and gated.
- Keep TCP NVMe supported-lab claims separate from future RDMA claims.

## Candidate Work

1. Inventory current Block NVMe/TCP evidence and current RDMA/RoCE non-claims.
2. Record what live facts are available from host preflight and volume-server
   capability APIs.
3. Define the minimum product gates for NVMe/RDMA: standalone live I/O,
   Kubernetes publish/attach, status surface, cleanup, and explicit fallback.
4. Decide the next implementation phase only after the evidence boundary is
   clear.

## Exit Criteria

Phase 157 can close when the roadmap and docs clearly separate host capability,
external RDMA experiments, and a future Block NVMe/RDMA product claim.
