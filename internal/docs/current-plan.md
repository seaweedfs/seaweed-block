# Current Plan: Phase 159 NVMe/RDMA Standalone Listener Design Gate

Status: planning.

Phase 158 closed the read-only volume capability probe:

```text
phase158_nvme_rdma_volume_capability_probe_status=ok
frontend_transport_capability_surface_present=true
nvme_tcp_supported=true
nvme_rdma_supported=false
nvme_rdma_refusal_reason=nvme_rdma_transport_unsupported
volume_server_capability_query_supported=true
host_capability_not_product_claim=true
no_rdma_listener_started=true
tcp_behavior_unchanged=true
performance_slo_claim_allowed=false
phase158_decision=capability_probe_added_rdma_still_unsupported
next_recommendation=phase159_nvme_rdma_standalone_listener_design_gate
```

## Goal

Define the smallest safe standalone NVMe/RDMA listener implementation slice.
This phase should produce an executable design gate before data-path coding:
what the listener must accept, what it must refuse, what evidence proves live
I/O, and how the existing TCP path remains unchanged.

## Required Evidence

```text
phase159_nvme_rdma_standalone_listener_design_gate_status=ok
rdma_listener_design_documented=true
rdma_transport_scope_documented=true
standalone_live_io_gate_defined=true
rdma_capability_endpoint_contract_preserved=true
tcp_behavior_unchanged=true
k8s_publish_attach_deferred_until_standalone_pass=true
fallback_refusal_required=true
cleanup_gate_defined=true
performance_slo_claim_allowed=false
next_recommendation=<specific next phase>
```

## Boundaries

- Do not implement Kubernetes NVMe/RDMA publish/attach in this phase.
- Do not claim RoCE live I/O, acceleration, or performance.
- Do not remove the typed unsupported reason until a standalone live I/O gate
  proves the RDMA target path.
- Keep NVMe/TCP behavior and existing tests unchanged.

## Candidate Work

1. Document the minimal NVMe-oF/RDMA listener architecture and code seams.
2. Define standalone live I/O proof: target bind, subsystem/namespace export,
   host connect, write/read verification, disconnect, cleanup.
3. Define refusal/fallback behavior for unsupported host/kernel/RDMA device
   states.
4. Define how `/status/frontend-capabilities` changes only after live support
   is implemented.
5. Produce a gate script that verifies the design contract and prevents
   premature Kubernetes/performance claims.

## Exit Criteria

Phase 159 can close when the RDMA listener implementation path is concrete
enough to code without expanding scope into Kubernetes publish/attach or
performance benchmarking.
