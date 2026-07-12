# Phase 131 Finished Plan: NVMe Kubernetes Live Host-Path Reconnect

Status: closed 2026-07-04, runner PASS.

## Problem

Phase 130 proved a CSI-node owner loop exists, but only as a component/source
contract. The remaining question was whether that owner works in a real mounted
Kubernetes PVC path after a host NVMe path disappears.

## Implementation

Phase 131 adds a live gate rather than a new broad product feature:

- `scripts/run-phase131-nvme-k8s-reconnect-live-gate.sh` wraps the Phase111
  Kubernetes NVMe setup path;
- `scripts/run-phase111-nvme-k8s-path-loss-crd-gate.sh` now has opt-in env
  switches for:
  - CSI-node reconnect owner,
  - forced Stage-2 multipath,
  - scoped host NVMe path disconnect;
- `testops/scenarios/nvme-tcp-k8s-reconnect-live-chain.yaml` drives the gate
  through the normal runner.

The gate injects loss with:

```text
nvme disconnect -d <one controller>
```

It does not use `nvme disconnect-all` for the injected failure.

Product polish included in this phase:

- `realNVMeUtil.Connect` treats `already connected` from `nvme connect` as
  idempotent success;
- mounted NVMe reconnect verification waits for host path visibility instead
  of immediately failing on `list-subsys` lag.

## Evidence

Runner:

```text
testops/scenarios/nvme-tcp-k8s-reconnect-live-chain.yaml
results/20260704-123729-62e4
```

Summary:

```text
phase131_nvme_k8s_reconnect_live_status=ok
stage2_multipath_enabled=true
initial_path_count=2
path_loss_detected=true
after_disconnect_path_count=1
reconnect_owner=csi-node
reconnect_invoked=true
replacement_path_connected=true
reconnected_path_count=2
pod_uid_preserved=true
mounted_io_after_reconnect=ok
crd_status_agrees=true
report_dashboard_agree=true
cleanup_status=ok
failure_count=0
```

## Close Criteria

Phase 131 closes because a live mounted Kubernetes NVMe/TCP PVC survives scoped
host path loss and reconnects through the product CSI-node owner, preserving pod
identity, mounted I/O, and status-surface agreement.

## Non-Claims

This phase does not claim:

- control-plane desired path-set replacement after frontend failover;
- new-node replacement path connection;
- NVMe/RDMA/RoCE;
- throughput/SLO.

## Remaining Work

Phase 132 should prove the replacement/failover path-set case:

```text
mounted PVC has old desired path set
-> frontend replacement/failover changes desired path evidence
-> CSI-node owner connects the new desired path
-> mounted I/O and surfaces remain correct
```
