# Phase 100 Finished Plan: Kubernetes CSI NVMe Multipath Attach

Status: closed on 2026-06-27.

## What Closed

Phase 100 closed the first supported-lab Kubernetes NVMe multipath attach path:

```text
dynamic PVC protocol=nvme, replicationFactor=2
  -> launcher emits two blockvolume frontends for one NQN/NSID
  -> blockmaster status exposes a multipath publish target
  -> CSI publish context carries nvmeAddrs
  -> NodeStage connects both NVMe portal addresses
  -> app pod writes/reads through the mounted namespace
  -> PVC delete leaves no Seaweed Block NVMe subsystem residue
```

## Product Changes

- `PublishTarget` and CSI publish context now carry `NVMeAddrs`.
- Master status lookup groups NVMe frontend paths only when they share one
  `NQN` and `NSID`.
- CSI `CreateVolumeResponse.VolumeContext` carries `stage2_multipath=true`
  forward from StorageClass parameters.
- `ControllerPublish` preserves the multipath request in publish context.
- `NodeStage` waits for refreshed NVMe multipath publish context and connects
  every portal for the same NQN.
- Dynamic lifecycle status can use fresh observed replica IDs as read-only
  evidence for publish-target aggregation.
- The live gate imports fresh local images to all schedulable nodes and removes
  stale k3s/containerd tags before import.

## Gates

Component:

```text
swblock run testops/scenarios/nvme-csi-multipath-component-chain.yaml
run: 20260627-013844-4a23
result: PASS, 10/10 actions
```

Live Kubernetes:

```text
swblock run testops/scenarios/nvme-csi-multipath-live-chain.yaml
run: 20260627-024451-2ee8
result: PASS, 18/18 actions
```

Live summary:

```text
phase100_nvme_csi_multipath_live_status=ok
dynamic_pvc_writer_reader=pass
generated_nvme_listen_count=2
generated_nqn_unique_count=1
generated_nsid_unique_count=1
node_stage_nvme_multipath_count=1
node_stage_two_portals_count=1
nvme_residue_count=0
```

## Non-Claims

- No RoCE/RDMA claim.
- No performance or soak claim.
- No broad distro/kernel compatibility claim.
- No production HA claim.
- No backup/snapshot/restore claim.
