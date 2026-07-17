# Phase 161 QA Sign-Off: NVMe/RDMA Standalone Preflight Refusal

Status: **PASS** on 2026-07-17.

Run bundle:
`results/phase161-nvme-rdma-standalone-preflight-refusal-gate`.

## Verdict

Phase 161 adds read-only RDMA preflight facts to the frontend capability
surface. The product can now report host-specific evidence for why RDMA cannot
start: `nvme_rdma_module`, `rdma_device`, and `rdma_bind_address`.

RDMA remains unsupported. No RDMA listener is started, `blockvolume` still
rejects `--nvme-transport=rdma`, TCP behavior is unchanged, and no Kubernetes or
performance claim is made.

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
go_test_volume_blockvolume_nvme=ok
status_dto_preflight_fact_present=true
capability_dto_preflight_field_present=true
rdma_preflight_helper_present=true
nvme_rdma_module_probe_present=true
nvme_rdma_module_sysfs_probe_present=true
nvme_rdma_proc_modules_probe_present=true
rdma_device_probe_present=true
rdma_device_sysfs_probe_present=true
rdma_bind_address_probe_present=true
rdma_bind_invalid_reason_present=true
rdma_bind_candidate_reason_present=true
module_missing_reason_present=true
device_missing_reason_present=true
rdma_unsupported_reason_preserved=true
rdma_listener_not_started=true
rdma_supported_false_preserved=true
capability_preflight_test_seen=true
loopback_refusal_test_seen=true
nonloopback_candidate_test_seen=true
status_endpoint_preflight_test_seen=true
parse_refusal_still_tested=true
phase161_decision=rdma_preflight_facts_surface_unsupported_state
next_recommendation=phase162_nvme_rdma_standalone_listener_skeleton_gate
cleanup_status=ok
```

## Conclusion

The product now has a truthful unsupported-state surface with host preflight
evidence. The next phase should define a standalone listener skeleton gate while
keeping the listener disabled and non-claiming by default.
