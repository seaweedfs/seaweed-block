# Phase 105 NVMe/TCP Multi-Host Topology Boundary QA Sign-off

Status: PASS.

Source branch: `phase105-nvme-tcp-multihost-topology`.

QA run:

```text
scenario: testops/scenarios/nvme-tcp-multihost-topology-chain.yaml
run:      20260629-003451-6c16
result:   18/18 PASS
node:     m02
```

## Scope

Phase 105 is a topology-boundary gate. It does not attempt live NVMe/TCP data
I/O. It proves that cross-node loopback NVMe/TCP evidence is blocked and that
the user-facing action is protocol-appropriate.

## Evidence

```text
phase105_nvme_tcp_multihost_topology_status=ok
read_only=true
live_io_claim=false
performance_claim_allowed=false
roce_claim_allowed=false
reason_code=publish_target_loopback_cross_node
expected_action=observe.inspect_publish_target_topology
forbidden_action=safe_k8s.reinstall_external_iscsi
go_test_core_ops=pass
nvme_cross_node_loopback_status=blocked
nvme_cross_node_loopback_reason=publish_target_loopback_cross_node
ready_true_count=0
safe_action=observe.inspect_publish_target_topology
iscsi_remediation_recommended=false
same_node_loopback_non_claim=true
cross_node_non_loopback_live_followup=true
```

## Result

The model keeps the existing protocol-neutral reason
`publish_target_loopback_cross_node`. That is the right status vocabulary: the
unsafe topology is identical regardless of frontend protocol.

The action surface is now protocol-aware:

- iSCSI loopback-cross-node keeps the existing
  `safe_k8s.reinstall_external_iscsi` dry-run recommendation.
- NVMe/TCP loopback-cross-node uses
  `observe.inspect_publish_target_topology`.

No surface may tell an NVMe user to run an iSCSI remediation. The regression is
covered by both direct projection tests and report/explain tests.

## Non-Claims

Phase 105 does not claim:

- cross-node NVMe/TCP attach works;
- RoCE/NVMe-RDMA works;
- live NVMe/TCP I/O;
- performance or SLO;
- production HA.

The next phase should prove the positive live path: cross-node non-loopback
NVMe/TCP attach with writer/reader verification.
