# Phase 112 NVMe/TCP K8s Mounted Path-Loss I/O QA Sign-off

Status: PASS.

Validated scenario:

```text
testops/scenarios/nvme-tcp-k8s-mounted-path-loss-io-chain.yaml
```

Final QA run:

```text
20260629-221637-4193
```

Result:

```text
21/21 actions PASS
```

## Scope

Phase 111 proved that live Kubernetes NVMe/TCP one-path loss projects honestly
to `SwBlockVolume.status` and the read-only support surfaces. Phase 112 adds
the user-visible mounted workload claim that Phase 111 deliberately did not
make.

The gate installs the Helm stack with:

- `protocol=nvme`
- two selected Kubernetes nodes
- `replicationFactor=2`
- operator-status enabled in write mode
- lifecycle-owner enabled

It creates one RF=2 NVMe/TCP PVC, verifies the healthy writer/reader path, then
keeps an extra pod mounted on the PVC while one generated `sw-blockvolume`
deployment is scaled to zero. The same pod must remain the same Kubernetes pod
and must write/read after the path loss through the remaining NVMe path.

## Terminal Evidence

```text
phase112_nvme_k8s_mounted_path_loss_io_status=ok
mounted_pod_uid_before=85ad2d4e-b4e2-4973-8c37-6bb226a59bcb
mounted_pod_uid_after=85ad2d4e-b4e2-4973-8c37-6bb226a59bcb
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

Mounted workload evidence:

```text
mounted-before.log:
before-path-loss

mounted-after.log:
before-path-loss
after-path-loss
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
| Mounted pod before injection | PASS | mounted pod wrote `before-path-loss`; UID `85ad2d4e-b4e2-4973-8c37-6bb226a59bcb` |
| Injection | PASS | scaled one generated `sw-blockvolume` deployment to zero |
| Mounted pod identity | PASS | same pod UID before and after path loss |
| Mounted I/O after path loss | PASS | same pod wrote/read `after-path-loss` after observed path count dropped to one |
| CRD negative projection | PASS | `status=blocked`, `reasonCode=nvme_multipath_path_missing`, `pathCount=1`, no Ready condition `True` |
| Report summary | PASS | `blocked/nvme_multipath_path_missing`; NVMe `path_count=1` |
| Operator snapshot | PASS | volume status `blocked`, reason `nvme_multipath_path_missing`, `nvme.path_count=1` |
| Dashboard snapshot | PASS | `/operator-snapshot.json` matches report operator snapshot |
| Explain | PASS | `ops explain volume --from-bundle` reports `blocked/nvme_multipath_path_missing` |
| Mutation boundary | PASS | only read-only support action, `mutation_allowed=false` |
| Cleanup | PASS | zero residue across Kubernetes, iSCSI, process, multipath, and hostPath |

## Harness Notes

One earlier run (`20260629-221007-1f3a`) found a gate-cleanup defect:

- the mounted workload and product assertions passed;
- the cleanup verifier reported `cleanup_status=failed` because a CSI node pod
  was still terminating;
- the gate had incorrectly appended `cleanup_status=ok` instead of copying the
  verifier's real result.

The gate was fixed before sign-off:

- mounted consumers are deleted before Helm uninstall, while CSI is still
  available for detach/delete;
- the cleanup phase waits for Helm-owned Kubernetes resources to disappear;
- the verifier gets a longer Kubernetes cleanup wait;
- the gate summary now copies the verifier's actual `cleanup-summary.txt`.

The final run passed with the verifier as the authoritative cleanup result.

## Verdict

PASS. The supported-lab Kubernetes NVMe/TCP path now has a mounted workload
one-path-loss proof:

- the volume starts healthy with two NVMe paths;
- one generated blockvolume path can be removed;
- the mounted pod is not recreated;
- the mounted pod continues write/read I/O through the remaining path;
- all status surfaces still refuse to claim volume `Ready=True` and report
  `blocked/nvme_multipath_path_missing`;
- cleanup is residue-free.

## Non-Claims

This gate does not claim RoCE/NVMe-RDMA, performance/SLO, broad distro/kernel
compatibility, production HA, automatic rebuild, path restoration, or all
possible NVMe failure modes. It proves only the supported-lab Kubernetes
NVMe/TCP mounted workload behavior under one observed path loss.
