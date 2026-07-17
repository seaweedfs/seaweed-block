# Phase 158 Finished Plan: NVMe/RDMA Volume Capability Probe

Status: **closed 2026-07-17, local gate PASS**.

## Problem

Phase 157 documented that host RoCE capability and external RDMA work are not a
Seaweed Block NVMe/RDMA product claim. The remaining gap was that users still
had to infer the current product boundary from docs and prior gates instead of
querying the volume process itself.

## Work

Phase 158 added a read-only volume status endpoint:

```text
GET /status/frontend-capabilities?volume=<id>
```

The endpoint reports frontend transport capabilities owned by the volume
process:

- NVMe/TCP is supported and can report whether its listener is started.
- NVMe/RDMA is unsupported, has no listener implementation, starts no listener,
  and returns `reason=nvme_rdma_transport_unsupported`.
- Host capability is explicitly not projected as a product claim.

The phase did not implement an RDMA listener, change TCP behavior, or make any
performance/SLO claim.

## Evidence

```text
phase158_nvme_rdma_volume_capability_probe_status=ok
frontend_transport_capability_surface_present=true
nvme_tcp_supported=true
nvme_rdma_supported=false
nvme_rdma_refusal_reason=nvme_rdma_transport_unsupported
volume_server_capability_query_supported=true
k8s_status_or_report_surface_updated=deferred_with_reason
host_capability_not_product_claim=true
no_rdma_listener_started=true
tcp_behavior_unchanged=true
performance_slo_claim_allowed=false
phase158_decision=capability_probe_added_rdma_still_unsupported
next_recommendation=phase159_nvme_rdma_standalone_listener_design_gate
```

## Conclusion

The product can now answer "does this volume support NVMe/RDMA?" directly and
truthfully: TCP yes, RDMA no, with a stable unsupported reason. The next phase
should design the standalone RDMA listener and its live I/O gate before touching
Kubernetes publish/attach.
