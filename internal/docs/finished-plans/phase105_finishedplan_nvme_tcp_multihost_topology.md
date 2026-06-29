# Phase 105 Finished Plan: NVMe/TCP Multi-Host Topology Boundary

Status: closed, QA PASS.

## Problem

After Phase 104, the product was correctly explicit that NVMe/RDMA is not
implemented. The remaining NVMe multi-host risk was topology: a loopback
NVMe/TCP publish target must not be handed to a workload on a different
Kubernetes node.

This is the same safety class as the earlier iSCSI loopback-cross-node blocker,
but the action surface must not recommend iSCSI remediation for NVMe evidence.

## What Changed

- Reused the existing protocol-neutral reason:
  `publish_target_loopback_cross_node`.
- Added `observe.inspect_publish_target_topology` as a read-only/dry-run action
  for NVMe topology blockers.
- Kept existing iSCSI behavior unchanged:
  `safe_k8s.reinstall_external_iscsi`.
- Added projection and report/explain tests proving:
  - cross-node loopback NVMe/TCP is blocked;
  - `Ready=True` is not emitted;
  - NVMe status is preserved;
  - iSCSI remediation is not surfaced for NVMe.
- Added a TestOps scenario:
  `testops/scenarios/nvme-tcp-multihost-topology-chain.yaml`.

## Verification

Local:

```text
go test ./scripts ./internal/testops ./core/ops ./cmd/sw-block ./core/frontend/nvme -count=1
C:\work\swblock.exe validate testops\scenarios\nvme-tcp-multihost-topology-chain.yaml
bash -n scripts/run-phase105-nvme-tcp-multihost-topology-gate.sh
```

Remote TestOps:

```text
scenario: testops/scenarios/nvme-tcp-multihost-topology-chain.yaml
run:      20260629-003451-6c16
result:   18/18 PASS
```

Terminal evidence:

```text
phase105_nvme_tcp_multihost_topology_status=ok
nvme_cross_node_loopback_status=blocked
nvme_cross_node_loopback_reason=publish_target_loopback_cross_node
ready_true_count=0
safe_action=observe.inspect_publish_target_topology
iscsi_remediation_recommended=false
live_io_claim=false
roce_claim_allowed=false
```

## Remaining Work

Phase 106 should prove the positive live path: cross-node non-loopback NVMe/TCP
publish target, CSI stage, and app writer/reader verification.
