# Phase 158 QA Sign-Off: NVMe/RDMA Volume Capability Probe

Status: **PASS** on 2026-07-17.

Run bundle:
`results/phase158-nvme-rdma-volume-capability-probe-gate`.

## Verdict

Phase 158 adds a read-only product-owned capability surface for the current
NVMe frontend boundary. The volume status server now exposes:

```text
GET /status/frontend-capabilities?volume=<id>
```

The current result is deliberately conservative: NVMe/TCP is supported and
NVMe/RDMA is unsupported with the stable reason
`nvme_rdma_transport_unsupported`. No RDMA listener is started, no performance
or acceleration claim is made, and TCP behavior is unchanged.

## Evidence

```text
phase158_nvme_rdma_volume_capability_probe_status=ok
frontend_transport_capability_surface_present=true
nvme_tcp_supported=true
nvme_rdma_supported=false
nvme_rdma_refusal_reason=nvme_rdma_transport_unsupported
volume_server_capability_query_supported=true
k8s_status_or_report_surface_updated=deferred_with_reason
k8s_status_or_report_defer_reason=volume_endpoint_first_no_k8s_claim
host_capability_not_product_claim=true
no_rdma_listener_started=true
tcp_behavior_unchanged=true
performance_slo_claim_allowed=false
go_test_volume_blockvolume_nvme=ok
status_endpoint_capability_test_seen=true
blockvolume_capability_helper_test_seen=true
rdma_parse_refusal_test_seen=true
rdma_target_refusal_test_seen=true
status_endpoint_route_present=true
stable_rdma_reason_present=true
rdma_listener_not_started_in_capability=true
rdma_boundary_doc_mentions_capability_endpoint=true
rdma_boundary_doc_mentions_current_probe=true
phase158_decision=capability_probe_added_rdma_still_unsupported
next_recommendation=phase159_nvme_rdma_standalone_listener_design_gate
cleanup_status=ok
```

## Checked Paths

- `core/host/volume` status server route, JSON DTO, loopback guard, and volume
  identity check.
- `cmd/blockvolume` capability projection for NVMe/TCP and NVMe/RDMA.
- Existing NVMe/RDMA typed refusal tests remain in place.
- Release and roadmap docs keep RDMA as a non-claim.

## Conclusion

Phase 158 closes the capability-observation gap without starting RDMA work. The
next safe step is a standalone NVMe/RDMA listener design gate before any
Kubernetes publish/attach or performance claim.
