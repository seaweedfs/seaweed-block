# Phase 114 NVMe/TCP K8s Multi-Volume Mounted Path Isolation QA Sign-off

Status: PASS.

Validated scenario:

```text
testops/scenarios/nvme-tcp-k8s-multivolume-mounted-path-isolation-chain.yaml
```

Strict passing QA run:

```text
20260630-054520-7489
```

Result:

```text
29/29 PASS
```

## Scope

Phase 114 extends the single-volume Phase 112 and Phase 113 NVMe/TCP mounted
path-loss/restore gates to the multi-volume case:

- install Helm with `protocol=nvme`, two Kubernetes nodes,
  `replicationFactor=2`, stage-2 multipath, operator-status, and
  lifecycle-owner;
- create two RF=2 NVMe/TCP PVCs;
- keep one long-lived mounted pod on each PVC;
- remove one generated `sw-blockvolume` deployment for volume 1;
- prove volume 1 becomes `blocked/nvme_multipath_path_missing` with one
  observed live host path;
- prove volume 2 remains `ready/first_volume_verified` with two observed live
  host paths;
- prove both mounted pods keep the same UID and continue I/O during path loss;
- restore the removed deployment;
- prove both volumes return to `Ready=True/first_volume_verified` with two
  observed live host paths;
- prove both mounted pods keep the same UID and continue I/O after restore;
- cleanup leaves zero Kubernetes/iSCSI/process/multipath/hostPath residue.

## Terminal Evidence

```text
phase114_nvme_k8s_multivolume_mounted_path_isolation_status=ok
volume_1_id=pvc-e7668b99-5288-4a6b-845c-c571dc69440c
volume_2_id=pvc-20b08812-fc0c-4aa3-9b3e-89b30725c734
host_pre_loss_1_live_path_count=2
host_pre_loss_2_live_path_count=2
target_deployment=sw-blockvolume-pvc-e7668b99-5288-4a6b-845c-c571dc69440c-r1
host_after_loss_1_live_path_count=1
host_after_loss_2_live_path_count=2
mounted_pods_preserved_after_loss=true
mounted_io_after_loss_count=2
degraded_volume_id=pvc-e7668b99-5288-4a6b-845c-c571dc69440c
untouched_volume_id=pvc-20b08812-fc0c-4aa3-9b3e-89b30725c734
degraded_volume_path_count=1
untouched_volume_path_count=2
degraded_volume_reason=nvme_multipath_path_missing
untouched_volume_reason=first_volume_verified
cross_volume_reason_mixup=false
degraded_surface_ready_true_count=0
host_after_restore_1_live_path_count=2
host_after_restore_2_live_path_count=2
mounted_pods_preserved_after_restore=true
mounted_io_after_restore_count=2
restored_volume_count=2
restored_all_path_count=2
restored_all_reason=first_volume_verified
cleanup_status=ok
k8s_residue_count=0
iscsi_residue_count=0
process_residue_count=0
multipath_residue_count=0
hostpath_residue_count=0
failure_count=0
```

## Gate Results

| Check | Result | Evidence |
| --- | --- | --- |
| Two-volume RF=2 NVMe source path | PASS | both volumes start as `ready/first_volume_verified` with two live host paths |
| Mounted pod before injection | PASS | both pods write/read before path loss |
| Path-loss injection | PASS | one generated blockvolume deployment for volume 1 scaled to zero |
| Mounted I/O during path loss | PASS | both pod UIDs unchanged, both pods write/read after loss |
| Degraded status isolation | PASS | volume 1 `blocked/nvme_multipath_path_missing`, volume 2 remains `ready/first_volume_verified` |
| Cross-volume reason isolation | PASS | `cross_volume_reason_mixup=false` |
| No false Ready while degraded | PASS | `degraded_surface_ready_true_count=0` |
| Path restore status | PASS | both volumes return to `ready/first_volume_verified` with `path_count=2` |
| Mounted I/O after restore | PASS | both pod UIDs unchanged, both pods write/read after restore |
| Cleanup | PASS | zero residue across Kubernetes, iSCSI, process, multipath, and hostPath |

## Resolved Findings

Earlier Phase 114 strict drafts exposed three real gaps before the final pass:

- chart/render path: stage-2 multipath had to be carried into Helm/CSI flags,
  not only StorageClass parameters;
- CSI/NVMe staging path: NodeStage had to fail closed for single-path stage-2
  NVMe and recreate staging identity for an already-connected same-NQN
  subsystem;
- host evidence path: `nvme list-subsys -v` parsing had to match target
  `traddr/trsvcid` exactly, not accidentally match `src_addr` text.

The final gate also uses bounded host-path polling after restore. This does not
weaken the claim: it still requires two live kernel NVMe paths before PASS, but
avoids treating the short reconnect window as a terminal product failure.

## Verdict

PASS. Phase 114 closes the multi-volume mounted NVMe/TCP path-isolation gate:
one volume can lose and regain a path while mounted, the other volume remains
isolated, status surfaces do not mix reasons, and both mounted workloads keep
the same pod identity and continue I/O through loss and restore.

## Non-Claims

This gate does not claim RoCE/NVMe-RDMA, performance/SLO, broad distro/kernel
compatibility, production HA under arbitrary path churn, or returned-replica
rebuild correctness beyond the RF=2 NVMe/TCP mounted-path isolation exercised
here.
