# Current Plan: Phase 162 NVMe/RDMA Standalone Listener Skeleton Gate

Status: planning.

Phase 161 closed standalone RDMA preflight/refusal:

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
phase161_decision=rdma_preflight_facts_surface_unsupported_state
next_recommendation=phase162_nvme_rdma_standalone_listener_skeleton_gate
```

## Goal

Define the disabled-by-default standalone RDMA listener skeleton. This phase
should wire the start/refusal decision path and failure mapping without claiming
RDMA live I/O or enabling Kubernetes publish/attach.

## Required Evidence

```text
phase162_nvme_rdma_standalone_listener_skeleton_gate_status=ok
rdma_listener_start_path_defined=true
rdma_listener_disabled_by_default=true
preflight_failure_maps_to_stable_reasons=true
capability_endpoint_reports_listener_started_false=true
tcp_behavior_unchanged=true
linux_nvme_connect_live_io_not_claimed=true
k8s_publish_attach_claim_allowed=false
performance_slo_claim_allowed=false
next_recommendation=<specific next phase>
```

## Boundaries

- Do not perform live RDMA I/O yet.
- Do not make `--nvme-transport=rdma` a supported user path.
- Do not publish RDMA frontend targets through Kubernetes.
- Do not claim performance or acceleration.

## Candidate Work

1. Add a disabled-by-default RDMA listener skeleton or start-decision object.
2. Map preflight failures to stable reasons before any listener creation.
3. Keep capability status on `supported=false` and `listenerStarted=false`.
4. Gate that TCP behavior and current refusal tests remain green.

## Exit Criteria

Phase 162 can close when the RDMA start decision is explicit and testable, while
the product still refuses RDMA safely by default.
