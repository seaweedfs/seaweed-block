# Current Plan: Phase 163 NVMe/RDMA Standalone Listener Implementation Spike

Status: planning.

Phase 162 closed the disabled-by-default listener skeleton:

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
phase162_decision=rdma_start_decision_skeleton_disabled_by_default
next_recommendation=phase163_nvme_rdma_standalone_listener_impl_spike
```

## Goal

Attempt the smallest standalone NVMe/RDMA listener implementation spike. The
only acceptable success proof is a standalone live I/O gate using Linux
`nvme connect -t rdma` against a Seaweed Block target. Kubernetes publish/attach
and performance claims stay deferred.

## Required Evidence

```text
phase163_nvme_rdma_standalone_listener_impl_spike_status=<ok|blocked_with_reason>
rdma_listener_impl_attempted=true
rdma_bind_ip=<100Gb/RoCE/data-plane IP or blocked>
rdma_device=<device or blocked>
linux_nvme_connect_rdma_succeeded=<true|false>
standalone_write_read_verified=<true|false>
disconnect_cleanup_status=<ok|not_reached>
capability_endpoint_reports_rdma_supported=<true|false>
tcp_behavior_unchanged=true
k8s_publish_attach_claim_allowed=false
performance_slo_claim_allowed=false
next_recommendation=<specific next phase>
```

## Boundaries

- Do not wire Kubernetes publish/attach in this phase.
- Do not claim performance or acceleration.
- If live RDMA I/O cannot pass, keep `supported=false` and report the blocking
  reason instead of partial success.
- Preserve TCP behavior and existing refusal tests.

## Candidate Work

1. Select the minimal RDMA implementation library/path.
2. Bind only to the RDMA/data-plane address, not the management LAN.
3. Feed real RDMA commands into the existing transport adapter seam.
4. Use Linux `nvme connect -t rdma` for live proof.
5. Verify write/read and cleanup, or close as blocked with precise evidence.

## Exit Criteria

Phase 163 can close only when the standalone gate either passes real RDMA live
I/O or records a concrete blocker that keeps the product in unsupported state.
