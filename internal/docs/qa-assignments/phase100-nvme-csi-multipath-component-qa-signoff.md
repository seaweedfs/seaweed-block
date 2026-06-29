# Phase 100 NVMe CSI Multipath Component QA Sign-off

Status: PASS for the component slice.

Validated source: current Phase 100 working tree before commit.

## Scope

This gate validates the Kubernetes CSI NVMe multipath component contract:

```text
blockmaster status frontends
  -> CSI PublishTarget
  -> CSI publish_context
  -> NodeStage NVMe connect calls
```

It does not claim live Kubernetes app-pod NVMe multipath attach. The live gate
remains required before any release claim.

## Evidence

Local component gate:

```text
scripts/run-phase100-nvme-csi-multipath-component-gate.sh .
phase100_nvme_csi_multipath_component_status=ok
go_test_core_csi=pass
control_status_nvme_multipath_grouping=true
control_status_nvme_no_cross_nqn_merge=true
node_stage_nvme_multipath_connects_all_targets=true
node_stage_nvme_multipath_cleanup=true
live_k8s_nvme_multipath_required_for_release=true
```

Local regression:

```text
go test ./core/frontend/nvme ./cmd/blockvolume ./core/csi ./core/launcher -count=1
PASS
```

Scenario validation:

```text
swblock validate testops/scenarios/nvme-csi-multipath-component-chain.yaml
VALID
```

Runner gate:

```text
swblock run testops/scenarios/nvme-csi-multipath-component-chain.yaml
run: 20260627-013844-4a23
result: PASS, 10/10 actions
```

## Checked Behaviors

- Multiple NVMe frontends with the same `NQN` and `NSID` become one CSI
  multipath publish target.
- NVMe frontends with different NQNs are not merged into one target.
- CSI publish context preserves both the legacy first path (`nvmeAddr`) and the
  full path list (`nvmeAddrs`).
- NodeStage reads `nvmeAddrs` and calls NVMe connect for every address.
- Mount failure still disconnects and does not leave a staged entry.
- The gate explicitly records that live Kubernetes NVMe multipath attach is
  still required.

## Non-Claims

- No live k3s dynamic PVC NVMe multipath attach yet.
- No app writer/reader mounted through multiple NVMe paths yet.
- No NVMe subsystem/controller residue cleanup proof yet.
- No RoCE, performance, broad distro/kernel compatibility, or production HA
  claim.

## Verdict

PASS for Phase 100 D1/D2 component work. Continue Phase 100 with a live
Kubernetes gate that proves dynamic PVC, app writer/reader, multiple NVMe path
connection, and cleanup.
