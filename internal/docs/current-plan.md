# Current Plan: Phase 161 NVMe/RDMA Standalone Preflight Refusal

Status: planning.

Phase 160 closed the transport adapter seam:

```text
phase160_nvme_rdma_transport_adapter_seam_status=ok
tcp_pdu_wire_path_isolated=true
reusable_nvme_command_handlers_preserved=true
rdma_adapter_interface_defined=true
rdma_transport_still_unsupported=true
capability_endpoint_still_reports_rdma_unsupported=true
nvme_tcp_tests_pass=true
phase160_decision=tcp_pdu_transport_isolated_rdma_still_unsupported
next_recommendation=phase161_nvme_rdma_standalone_preflight_refusal
```

## Goal

Add standalone RDMA preflight/refusal evidence while keeping RDMA unsupported by
default. The product should be able to explain why RDMA cannot start on the
current host: missing module, missing RDMA device, invalid bind address, or
unsupported implementation state.

## Required Evidence

```text
phase161_nvme_rdma_standalone_preflight_refusal_status=ok
rdma_preflight_probe_present=true
nvme_rdma_module_fact_reported=true
rdma_device_fact_reported=true
rdma_bind_address_fact_reported=true
stable_failure_reasons_reported=true
rdma_listener_still_not_started=true
capability_endpoint_still_reports_rdma_unsupported=true
tcp_behavior_unchanged=true
k8s_publish_attach_claim_allowed=false
performance_slo_claim_allowed=false
next_recommendation=<specific next phase>
```

## Boundaries

- Do not implement an RDMA listener yet.
- Do not allow Kubernetes publish/attach for RDMA.
- Do not claim RoCE performance or live I/O.
- Keep TCP tests and the current RDMA typed refusal green.

## Candidate Work

1. Add a read-only RDMA preflight DTO/helper for module/device/bind-address
   evidence.
2. Surface stable failure reasons without changing RDMA listener state.
3. Extend the capability probe or a companion status path to include preflight
   facts while preserving `supported=false`.
4. Gate that TCP behavior and existing refusal tests are unchanged.

## Exit Criteria

Phase 161 can close when a user can ask the product why RDMA is unsupported on
the current host and receive stable, host-specific evidence without starting an
RDMA listener.
