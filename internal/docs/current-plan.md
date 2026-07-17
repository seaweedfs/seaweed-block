# Current Plan: Phase 158 NVMe/RDMA Volume Capability Probe

Status: planning.

Phase 157 closed the NVMe/RDMA capability boundary:

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
phase157_decision=keep_nvme_rdma_non_claim_until_product_transport_gates
next_recommendation=phase158_nvme_rdma_volume_capability_probe
```

## Goal

Add a read-only capability surface for the current Block NVMe frontend support.
The surface should report that TCP is implemented and RDMA is unsupported, with
a stable refusal/fallback reason, before any RDMA data path is attempted.

## Required Evidence

```text
phase158_nvme_rdma_volume_capability_probe_status=ok
frontend_transport_capability_surface_present=true
nvme_tcp_supported=true
nvme_rdma_supported=false
nvme_rdma_refusal_reason=<stable_reason>
volume_server_capability_query_supported=true
k8s_status_or_report_surface_updated=<true|deferred_with_reason>
host_capability_not_product_claim=true
no_rdma_listener_started=true
tcp_behavior_unchanged=true
performance_slo_claim_allowed=false
next_recommendation=<specific next phase>
```

## Boundaries

- Do not implement an RDMA listener in this phase.
- Do not claim NVMe/RDMA attach, RoCE live I/O, acceleration, or performance.
- Do not change TCP behavior or the existing typed RDMA refusal.
- Keep the surface read-only and product-owned.

## Candidate Work

1. Locate the existing NVMe frontend transport selection/refusal code.
2. Add a small read-only capability DTO/API or status field for frontend
   transport support.
3. Assert TCP supported, RDMA unsupported, stable reason, and no RDMA listener.
4. Decide whether the next step should be a standalone RDMA listener spike or a
   Kubernetes publish-context design.

## Exit Criteria

Phase 158 can close when users and gates can query the product and see the
current transport capability boundary directly from Seaweed Block, not only from
docs.
