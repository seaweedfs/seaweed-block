# Phase 113 NVMe/TCP K8s Mounted Path Restore QA Sign-off

Status: PASS.

Validated scenario:

```text
testops/scenarios/nvme-tcp-k8s-mounted-path-restore-chain.yaml
```

Final QA run:

```text
20260629-223946-7799
```

Result:

```text
29/29 actions PASS
```

## Scope

Phase 112 proved that a mounted RF=2 NVMe/TCP PVC can continue I/O after one
observed path is removed. Phase 113 proves the corresponding restoration path:
after the removed blockvolume deployment is scaled back to one replica, the
same mounted pod continues I/O and the status surface returns to a healthy
two-path state.

The gate installs the Helm stack with:

- `protocol=nvme`
- two selected Kubernetes nodes
- `replicationFactor=2`
- operator-status enabled in write mode
- lifecycle-owner enabled

It creates one RF=2 NVMe/TCP PVC, verifies mounted I/O before path loss, removes
one generated blockvolume path, verifies mounted I/O during the degraded
one-path state, then restores the deployment and verifies both mounted I/O and
status convergence back to `Ready=True/first_volume_verified`.

## Terminal Evidence

```text
phase113_nvme_k8s_mounted_path_restore_status=ok
mounted_pod_uid_before=d72069aa-d3b6-4edb-9aeb-c78204d35f6b
mounted_pod_uid_after=d72069aa-d3b6-4edb-9aeb-c78204d35f6b
mounted_pod_uid_preserved=true
mounted_io_after_path_loss=ok
before_path_count=2
after_path_count=1
crd_reason=nvme_multipath_path_missing
surface_ready_true_count=0
mounted_pod_uid_after_restore=d72069aa-d3b6-4edb-9aeb-c78204d35f6b
mounted_pod_uid_preserved_after_restore=true
mounted_io_after_restore=ok
restored_path_count=2
restore_crd_reason=first_volume_verified
restore_report_reason=first_volume_verified
restore_operator_snapshot_reason=first_volume_verified
restore_explain_reason=first_volume_verified
cleanup_status=ok
```

Mounted workload evidence:

```text
mounted-after-restore.log:
before-path-loss
after-path-loss
after-restore
```

Cleanup verifier:

```text
cleanup_status=ok
k8s_residue_count=0
iscsi_residue_count=0
process_residue_count=0
multipath_residue_count=0
hostpath_residue_count=0
failure_count=0
```

Post-run verifier also returned `cleanup_status=ok`.

## Gate Results

| Check | Result | Evidence |
| --- | --- | --- |
| Healthy source path | PASS | RF=2 NVMe/TCP PVC starts with `before_path_count=2` and `Ready=True/first_volume_verified` |
| Mounted pod before injection | PASS | mounted pod writes `before-path-loss` |
| Path-loss injection | PASS | scaled one generated `sw-blockvolume` deployment to zero |
| Mounted I/O during path loss | PASS | same pod UID, mounted write/read after path loss |
| Degraded status honesty | PASS | `after_path_count=1`, `blocked/nvme_multipath_path_missing`, no false volume `Ready=True` |
| Path restore | PASS | scaled the target deployment back to one replica |
| Mounted I/O after restore | PASS | same pod UID, mounted write/read includes `after-restore` |
| CRD restored status | PASS | `restored_path_count=2`, `restore_crd_reason=first_volume_verified` |
| Report/operator-snapshot/explain restored status | PASS | all report `first_volume_verified` with two paths |
| Cleanup | PASS | zero residue across Kubernetes, iSCSI, process, multipath, and hostPath |

## Verdict

PASS. The supported-lab Kubernetes NVMe/TCP path now has a full mounted
one-path-loss and restore proof:

- the mounted workload remains the same pod;
- I/O succeeds before loss, during one-path loss, and after restore;
- degraded status is honest and non-ready;
- restored status converges back to `Ready=True/first_volume_verified` with two
  paths;
- cleanup is residue-free.

## Non-Claims

This gate does not claim RoCE/NVMe-RDMA, performance/SLO, broad distro/kernel
compatibility, production HA, arbitrary path churn, automatic rebuild, or all
possible NVMe failure modes. It proves only the supported-lab Kubernetes
NVMe/TCP mounted workload behavior for one removed path and its restoration.
