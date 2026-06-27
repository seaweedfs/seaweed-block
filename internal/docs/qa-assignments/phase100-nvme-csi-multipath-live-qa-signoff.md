# Phase 100 NVMe CSI Multipath Live QA Sign-off

Status: PASS.

Validated source: current Phase 100 working tree before commit.

## Scope

This gate validates the live Kubernetes path:

```text
dynamic PVC protocol=nvme, replicationFactor=2
  -> launcher creates two NVMe frontends for one NQN/NSID
  -> CSI ControllerPublish/NodeStage preserve the multipath requirement
  -> NodeStage connects both NVMe portal addresses
  -> app pod writes and reads through the mounted namespace
  -> PVC delete leaves no Seaweed Block NVMe subsystem residue
```

## Evidence

Runner:

```text
swblock run testops/scenarios/nvme-csi-multipath-live-chain.yaml
run: 20260627-024451-2ee8
result: PASS, 18/18 actions
```

Summary:

```text
phase100_nvme_csi_multipath_live_status=ok
dynamic_pvc_writer_reader=pass
generated_nvme_listen_count=2
generated_nqn_unique_count=1
generated_nsid_unique_count=1
generated_iscsi_arg_count=0
node_stage_nvme_multipath_count=1
node_stage_two_portals_count=1
run_pass_count=1
nvme_residue_count=0
```

NodeStage evidence:

```text
NodeStageVolume: ... staged transport=nvme
  portal=127.0.0.1:4420
  portals=127.0.0.1:4420,127.0.0.1:4421
  multipath=true
```

Cleanup evidence:

```text
nvme_residue_count=0
kubectl get pv,pvc,pod -A: no Seaweed Block test residue
```

## Findings Closed During The Gate

- Dynamic PVC multipath required `stage2_multipath` to flow through
  `CreateVolumeResponse.VolumeContext`, `ControllerPublish` publish context, and
  `NodeStage` bounded refresh. Without that, the app path worked but staged only
  one NVMe portal.
- Dynamic lifecycle status needed fresh observed replica IDs as a read-only
  fallback for CSI publish-target aggregation. Static topology remains the
  allow-list for static volumes.
- The live gate must import `:local` images to every schedulable node and remove
  stale containerd tags before import. Otherwise k3s may run old `sw-block` or
  `sw-block-csi` binaries even after a fresh Docker build.

## Non-Claims

- No RoCE/RDMA transport claim.
- No performance claim.
- No broad distro/kernel compatibility claim.
- No production HA or long soak claim.

## Verdict

PASS. Phase 100 closes the Kubernetes CSI NVMe multipath attach slice for the
supported lab path: dynamic PVC, two NVMe paths for one NQN/NSID, mounted
writer/reader, and zero NVMe residue after delete.
