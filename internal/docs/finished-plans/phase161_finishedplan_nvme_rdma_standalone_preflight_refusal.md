# Phase 161 Finished Plan: NVMe/RDMA Standalone Preflight Refusal

Status: **closed 2026-07-17, local gate PASS**.

## Problem

After Phase 160, the code had a transport adapter seam but still reported RDMA
as a flat unsupported reason. That was truthful but not very actionable: a user
could not tell whether the host lacked `nvme-rdma`, RDMA devices, or a usable
bind address.

## Work

Phase 161 extended the read-only capability surface with preflight facts:

- `nvme_rdma_module`: checks `/sys/module/nvme_rdma` and `/proc/modules`;
- `rdma_device`: checks `/sys/class/infiniband`;
- `rdma_bind_address`: rejects empty, unspecified, or loopback bind addresses
  and records non-loopback addresses as candidates.

These facts are attached to the RDMA capability while preserving
`supported=false`, `listenerImplemented=false`, `listenerStarted=false`, and
`reason=nvme_rdma_transport_unsupported`.

## Evidence

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
phase161_decision=rdma_preflight_facts_surface_unsupported_state
next_recommendation=phase162_nvme_rdma_standalone_listener_skeleton_gate
```

## Conclusion

RDMA remains unsupported, but the unsupported state is now explainable from the
product itself. The next step is a standalone listener skeleton gate before
attempting live I/O.
