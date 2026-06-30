# Current Plan: Phase 113 NVMe/TCP K8s Mounted Path Restore

Status: closed.

QA run `20260629-223946-7799` passed 29/29 actions. Sign-off:
`internal/docs/qa-assignments/phase113-nvme-k8s-mounted-path-restore-qa-signoff.md`.

## Why This Was Next

Phase 112 proved the mounted workload survives one observed NVMe/TCP path loss:
the same pod kept running and wrote/read through the remaining path while
status honestly reported `blocked/nvme_multipath_path_missing`.

That still left the other half of the operational loop: if the removed
blockvolume path comes back, the system must converge back to the healthy
two-path status without recreating the workload or losing mounted I/O.

Phase 113 closes that restoration gap for the supported lab path.

## Product Goal

Prove that a mounted Kubernetes workload on an RF=2 NVMe/TCP PVC can continue
I/O through path loss and then continue I/O after the removed path is restored,
while the control plane moves from healthy -> degraded -> healthy with the
correct reasons.

Required behavior:

- install Helm with two ready nodes, `protocol=nvme`, operator-status enabled,
  lifecycle-owner enabled, and RF=2;
- create one PVC through CSI and verify the normal writer/reader data path;
- create a long-lived mounted pod on the same PVC and write before path loss;
- wait for `SwBlockVolume.status.nvme.pathCount=2` and
  `Ready=True/first_volume_verified`;
- scale one generated `sw-blockvolume` deployment to zero;
- verify the mounted pod UID is unchanged and mounted I/O still works;
- wait for `SwBlockVolume.status.nvme.pathCount=1` and
  `blocked/nvme_multipath_path_missing`;
- scale the target deployment back to one replica;
- verify the mounted pod UID is still unchanged and mounted I/O still works;
- wait for `SwBlockVolume.status.nvme.pathCount=2` and
  `Ready=True/first_volume_verified`;
- prove report, operator-snapshot, and explain agree with the restored CRD;
- cleanup leaves zero Kubernetes/NVMe/iSCSI/process/multipath/hostPath residue.

## Gate

Scenario:

```text
testops/scenarios/nvme-tcp-k8s-mounted-path-restore-chain.yaml
```

Gate script:

```text
scripts/run-phase113-nvme-k8s-mounted-path-restore-gate.sh
```

The Phase 113 wrapper reuses the Phase 111 path-loss gate with mounted-I/O and
path-restore modes enabled.

Terminal evidence:

```text
phase113_nvme_k8s_mounted_path_restore_status=ok
mounted_pod_uid_preserved=true
mounted_io_after_path_loss=ok
after_path_count=1
crd_reason=nvme_multipath_path_missing
surface_ready_true_count=0
mounted_pod_uid_preserved_after_restore=true
mounted_io_after_restore=ok
restored_path_count=2
restore_crd_reason=first_volume_verified
restore_report_reason=first_volume_verified
restore_operator_snapshot_reason=first_volume_verified
restore_explain_reason=first_volume_verified
cleanup_status=ok
```

## Result

Phase 113 passed on live k3s. The mounted pod kept the same UID, wrote/read
before path loss, wrote/read during one-path loss, and wrote/read after the
target deployment was restored. The authoritative CRD and read-only support
surfaces returned to `Ready=True/first_volume_verified` with two observed NVMe
paths. Cleanup was verified clean.

## Non-Claims

Phase 113 does not claim:

- RoCE/NVMe-RDMA;
- performance or SLO;
- broad distro/kernel compatibility;
- production HA;
- arbitrary path churn;
- automatic rebuild;
- every possible NVMe path failure mode;
- more than the supported lab Kubernetes NVMe/TCP mounted workload behavior for
  one removed path and its restoration.
