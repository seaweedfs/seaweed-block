# Current Plan: Phase 160 NVMe/RDMA Transport Adapter Seam

Status: planning.

Phase 159 closed the standalone listener design gate:

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
phase159_decision=design_rdma_as_transport_adapter_not_fake_tcp_listener
next_recommendation=phase160_nvme_rdma_transport_adapter_seam
```

## Goal

Create the explicit code seam that separates reusable NVMe command/session
handling from the current NVMe/TCP PDU wire path. This prepares for a real
RDMA listener without changing TCP behavior or claiming RDMA support.

## Required Evidence

```text
phase160_nvme_rdma_transport_adapter_seam_status=ok
tcp_pdu_wire_path_isolated=true
reusable_nvme_command_handlers_preserved=true
rdma_adapter_interface_defined=true
rdma_transport_still_unsupported=true
capability_endpoint_still_reports_rdma_unsupported=true
nvme_tcp_tests_pass=true
mounted_or_existing_tcp_gate_unchanged=<true|not_run_with_reason>
k8s_publish_attach_claim_allowed=false
performance_slo_claim_allowed=false
next_recommendation=<specific next phase>
```

## Boundaries

- Do not implement RDMA listener I/O yet.
- Do not alter the public TCP behavior or existing typed RDMA refusal.
- Do not update Kubernetes publish/attach for RDMA.
- Do not claim RoCE, acceleration, or performance.

## Candidate Work

1. Identify the smallest transport adapter interface needed between session
   lifecycle and command handling.
2. Keep NVMe/TCP on the existing PDU reader/writer through that interface.
3. Add tests proving TCP behavior and RDMA unsupported status are unchanged.
4. Leave `/status/frontend-capabilities` reporting RDMA unsupported until a real
   listener passes standalone live I/O.

## Exit Criteria

Phase 160 can close when the code has a clear adapter seam for future RDMA work
and all TCP/refusal gates remain green.
