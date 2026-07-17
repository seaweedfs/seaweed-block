# Phase 159 Finished Plan: NVMe/RDMA Standalone Listener Design Gate

Status: **closed 2026-07-17, local gate PASS**.

## Problem

Phase 158 let the product report that NVMe/TCP is supported and NVMe/RDMA is
unsupported. The next risk was starting RDMA implementation from the wrong seam:
wrapping the existing NVMe/TCP byte-stream session in a different listener.

That would not prove Linux `nvme connect -t rdma`, and it would create another
semantic loop instead of a real capability loop.

## Work

Phase 159 documented and gated the standalone RDMA listener design:

- current code seams in `transport.go`, `target.go`, `session.go`, `wire.go`,
  command handlers, `blockvolume`, and the status capability endpoint;
- why `ListenerFactory` is a selection seam but not sufficient for true RDMA;
- required standalone live I/O proof using Linux `nvme connect -t rdma`;
- required 100Gb/RoCE/data-plane bind IP evidence;
- stable failure reasons and cleanup requirements;
- continued deferral of Kubernetes publish/attach and performance claims.

## Evidence

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

## Conclusion

The next phase should create the explicit transport adapter seam while
preserving every NVMe/TCP test. Only after that should the project implement a
real RDMA listener.
