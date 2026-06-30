# Current Plan: Phase 111 NVMe/TCP K8s Path-Loss CRD Honesty

Status: closed.

QA run `20260629-213354-02a1` passed 19/19 actions. Sign-off:
`internal/docs/qa-assignments/phase111-nvme-k8s-path-loss-crd-qa-signoff.md`.

## Why This Is Next

Phase 110 deliberately stopped at support-surface replay of real standalone
path-loss evidence. That was useful, but it left one important non-claim:
Kubernetes `SwBlockVolume.status` had not yet been proven to turn a live RF=2
NVMe/TCP path loss into a non-ready CRD status.

Phase 111 closes that gap. It starts from the real Kubernetes CSI path, creates
one RF=2 NVMe/TCP PVC, verifies the healthy two-path CRD state, then removes one
launcher-managed blockvolume path by scaling one generated deployment to zero.
The launcher reconciler explicitly preserves an operator-scaled-zero deployment,
so this is a stable and product-representative path-loss injection rather than a
race against the reconciler.

## Product Goal

Prove that live Kubernetes NVMe/TCP path loss projects to
`SwBlockVolume.status` and every read-only support surface as
`blocked/nvme_multipath_path_missing`, with no false `Ready=True` and no
mutating action.

Required behavior:

- install Helm with two ready nodes, `protocol=nvme`, operator-status enabled,
  lifecycle-owner enabled, and RF=2;
- create one PVC through CSI and verify writer/reader data path;
- wait for `SwBlockVolume.status.nvme.pathCount=2` and
  `Ready=True/first_volume_verified`;
- scale one generated `sw-blockvolume` deployment to zero;
- wait for `SwBlockVolume.status.nvme.pathCount=1` and
  `blocked/nvme_multipath_path_missing`;
- prove report, operator-snapshot, dashboard, and explain agree with the CRD;
- prove no volume surface claims `Ready=True`;
- cleanup leaves zero Kubernetes/NVMe/iSCSI/process/multipath/hostPath residue.

## Gate

Scenario:

```text
testops/scenarios/nvme-tcp-k8s-path-loss-crd-chain.yaml
```

Gate script:

```text
scripts/run-phase111-nvme-k8s-path-loss-crd-gate.sh
```

Terminal evidence:

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

## Result

Phase 111 passed on live k3s. The CRD and all read-only support surfaces agree
that a one-path RF=2 NVMe/TCP volume is blocked with
`nvme_multipath_path_missing`. The healthy source state had two NVMe paths; the
post-injection state had one path; no volume surface claimed `Ready=True`.

## Non-Claims

Phase 111 does not claim:

- RoCE/NVMe-RDMA;
- performance or SLO;
- broad distro/kernel compatibility;
- production HA;
- automatic rebuild or path restoration;
- more than the supported lab Kubernetes NVMe/TCP path-loss status behavior.
