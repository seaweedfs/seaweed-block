# Current Plan: Phase 112 NVMe/TCP K8s Mounted Path-Loss I/O

Status: closed.

QA run `20260629-221637-4193` passed 21/21 actions. Sign-off:
`internal/docs/qa-assignments/phase112-nvme-k8s-mounted-path-loss-io-qa-signoff.md`.

## Why This Was Next

Phase 111 closed the live Kubernetes CRD/status non-claim for NVMe/TCP
one-path loss: a real RF=2 PVC could lose one generated blockvolume path and
all read-only surfaces would report `blocked/nvme_multipath_path_missing`
without false `Ready=True`.

That still left one user-visible non-claim: Phase 111 did not prove that an
already-mounted workload can continue I/O through the remaining NVMe path. A
status-only proof is not enough for a storage feature if it does not also prove
the mounted data path survives the tested fault.

Phase 112 closes that gap for the supported lab path.

## Product Goal

Prove that a mounted Kubernetes workload on an RF=2 NVMe/TCP PVC can continue
write/read I/O after one observed NVMe path is removed, while the control plane
still reports the degraded volume honestly as non-ready.

Required behavior:

- install Helm with two ready nodes, `protocol=nvme`, operator-status enabled,
  lifecycle-owner enabled, and RF=2;
- create one PVC through CSI and verify the normal writer/reader data path;
- create a long-lived mounted pod on the same PVC and write before path loss;
- wait for `SwBlockVolume.status.nvme.pathCount=2` and
  `Ready=True/first_volume_verified`;
- scale one generated `sw-blockvolume` deployment to zero;
- verify the mounted pod UID is unchanged;
- write/read through the same mounted pod after path loss;
- wait for `SwBlockVolume.status.nvme.pathCount=1` and
  `blocked/nvme_multipath_path_missing`;
- prove report, operator-snapshot, dashboard, and explain agree with the CRD;
- prove no volume surface claims `Ready=True`;
- cleanup leaves zero Kubernetes/NVMe/iSCSI/process/multipath/hostPath residue.

## Gate

Scenario:

```text
testops/scenarios/nvme-tcp-k8s-mounted-path-loss-io-chain.yaml
```

Gate script:

```text
scripts/run-phase112-nvme-k8s-mounted-path-loss-io-gate.sh
```

The Phase 112 wrapper reuses the Phase 111 path-loss gate with mounted-I/O mode
enabled.

Terminal evidence:

```text
phase112_nvme_k8s_mounted_path_loss_io_status=ok
mounted_pod_uid_preserved=true
mounted_io_after_path_loss=ok
before_path_count=2
after_path_count=1
crd_reason=nvme_multipath_path_missing
report_reason=nvme_multipath_path_missing
operator_snapshot_reason=nvme_multipath_path_missing
dashboard_reason=nvme_multipath_path_missing
explain_reason=nvme_multipath_path_missing
surface_ready_true_count=0
mutation_allowed=false
cleanup_status=ok
```

## Result

Phase 112 passed on live k3s. The mounted pod kept the same UID and wrote/read
after one generated blockvolume path was removed. The authoritative CRD and all
read-only support surfaces reported `blocked/nvme_multipath_path_missing` with
one observed path and no false volume `Ready=True`. Cleanup was verified clean.

## Non-Claims

Phase 112 does not claim:

- RoCE/NVMe-RDMA;
- performance or SLO;
- broad distro/kernel compatibility;
- production HA;
- automatic rebuild or path restoration;
- every possible NVMe path failure mode;
- more than the supported lab Kubernetes NVMe/TCP mounted workload behavior
  under one observed path loss.
