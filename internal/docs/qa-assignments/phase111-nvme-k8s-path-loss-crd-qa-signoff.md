# Phase 111 NVMe/TCP K8s Path-Loss CRD Honesty QA Sign-off

Status: PASS.

Validated scenario:

```text
testops/scenarios/nvme-tcp-k8s-path-loss-crd-chain.yaml
```

Final QA run:

```text
20260629-213354-02a1
```

Result:

```text
19/19 actions PASS
```

## Scope

Phase 111 closes the Phase 110 non-claim: live Kubernetes CRD projection for
NVMe/TCP path loss.

The gate installs the Helm stack with:

- `protocol=nvme`
- two selected Kubernetes nodes
- `replicationFactor=2`
- operator-status enabled in write mode
- lifecycle-owner enabled

It creates one RF=2 NVMe/TCP PVC, verifies writer/reader data path, waits for a
healthy two-path `SwBlockVolume.status.nvme`, scales one generated
`sw-blockvolume` deployment to zero, then verifies every read-only status
surface refuses to claim `Ready=True`.

## Terminal Evidence

```text
phase111_nvme_k8s_path_loss_crd_status=ok
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

## Gate Results

| Check | Result | Evidence |
| --- | --- | --- |
| Healthy source path | PASS | `SwBlockVolume.status.nvme.pathCount=2`, `status=ready`, `reasonCode=first_volume_verified` |
| Injection | PASS | scaled one generated `sw-blockvolume` deployment to zero; launcher preserves scaled-zero deployments |
| CRD negative projection | PASS | `status=blocked`, `reasonCode=nvme_multipath_path_missing`, `pathCount=1`, Ready condition `False` |
| Report summary | PASS | `managed_volume=<id> status=blocked reason=nvme_multipath_path_missing`; NVMe `path_count=1` |
| Operator snapshot | PASS | volume status `blocked`, reason `nvme_multipath_path_missing`, `nvme.path_count=1` |
| Dashboard snapshot | PASS | `/operator-snapshot.json` matches report operator snapshot |
| Explain | PASS | `ops explain volume --from-bundle` reports `blocked/nvme_multipath_path_missing` and path count 1 |
| Mutation boundary | PASS | only `observe.collect_bundle`, `mutation_allowed=false`; no repair/rebuild/failback/delete claim |
| Cleanup | PASS | zero residue across Kubernetes, iSCSI, process, multipath, hostPath; NVMe disconnected |

## Harness Notes

Two initial red runs were gate-authoring defects, not product defects:

- The first run used the PVC/CR name for `ops explain --from-bundle`; the bundle
  lookup requires the internal `volumeID`.
- The second run treated node-level `ready=true` lines as false volume
  `Ready=True`. The final gate now checks volume readiness conditions only.

The product evidence in both reruns already showed the correct CRD/report/
dashboard state. The final run confirms the corrected gate end-to-end.

## Verdict

PASS. Live Kubernetes NVMe/TCP one-path-loss now projects honestly through
`SwBlockVolume.status` and all read-only support surfaces:

- healthy RF=2 starts with two paths;
- losing one path produces `blocked/nvme_multipath_path_missing`;
- no volume surface claims `Ready=True`;
- no mutating action is suggested or attempted;
- cleanup is residue-free.

## Non-Claims

This gate does not claim RoCE/NVMe-RDMA, performance/SLO, broad
distro/kernel compatibility, production HA, automatic rebuild, or path
restoration. It proves only the supported-lab Kubernetes NVMe/TCP path-loss
status behavior.
