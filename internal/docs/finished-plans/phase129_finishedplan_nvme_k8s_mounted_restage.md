# Phase 129 Finished Plan: NVMe Kubernetes Mounted Restage Contract

Status: closed 2026-07-03, runner PASS.

## Problem

Phase 128 proved the standalone Linux NVMe host receives ANA Change Notice and
refreshes path state. Kubernetes still had a separate gap: once a PVC staging
path is mounted, `NodeStageVolume` returned immediately on repeated calls and
therefore did not refresh publish context or connect replacement NVMe paths.

That meant a future owner/trigger could call NodeStage again and still get a
no-op, leaving missing paths unrepaired.

## Implementation

`core/csi.NodeServer` now reconciles mounted NVMe staging paths:

- validates the mounted staging identity first;
- refreshes publish context from the control-plane lookup;
- waits for multipath publish context when Stage 2 multipath is requested;
- connects missing NVMe paths for the same NQN;
- verifies all desired paths are connected;
- updates staged identity files and in-memory staged state;
- rejects mounted NQN mismatch before connecting anything;
- never remounts, reformats, or disconnects all sessions.

The host mutation scope is intentionally narrow:

```text
nvme connect <missing path for same NQN>
```

## Evidence

Runner:

```text
testops/scenarios/nvme-k8s-mounted-restage-chain.yaml
results/20260703-173639-8c2b
```

Summary:

```text
phase129_nvme_k8s_mounted_restage_status=ok
mounted_nodestage_reconnects_missing_path=true
mounted_nodestage_rejects_nqn_mismatch=true
mounted_nodestage_does_not_remount=true
restage_owner=node_stage
host_mutation_scope=nvme_connect_missing_paths_only
automatic_k8s_reconnect_claim=false
automatic_trigger_required_next=true
cleanup_status=ok
```

## Close Criteria

Phase 129 closes because the mounted restage code path is now executable and
gated. A repeated NodeStage call can repair missing NVMe paths without
remounting the volume.

## Remaining Work

This is not yet automatic Kubernetes dynamic reconnect. Phase 130 must define
and prove the owner/trigger that notices desired path-set changes and invokes
the bounded restage path for a mounted PVC.
