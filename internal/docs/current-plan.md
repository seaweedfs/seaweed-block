# Current Plan: Phase 115 NVMe/TCP Mounted Multi-Volume Path Churn Soak

Status: planned.

Phase 114 is closed:

```text
QA run: 20260630-054520-7489
Result: 29/29 PASS
Sign-off: internal/docs/qa-assignments/phase114-nvme-k8s-multivolume-mounted-path-isolation-qa-signoff.md
Commit: 188eb31 phase114: validate multi-volume nvme mounted path isolation
```

## Why This Is Next

Phase 112 proved one mounted RF=2 NVMe/TCP PVC survives one path loss.
Phase 113 proved the same single volume returns to two live paths after restore.
Phase 114 proved two mounted RF=2 NVMe/TCP PVCs stay isolated when one volume
loses and restores one path.

The remaining supported-lab NVMe/TCP gap is bounded churn: repeated path loss
and restore across more than one mounted volume. A one-shot gate can miss
stale kernel controllers, stale status reasons, cross-volume path reuse, or
cleanup residue that only appears after the second or third transition.

## Product Goal

Prove that mounted multi-volume NVMe/TCP path recovery is repeatable, not just
single-transition correct.

Required behavior:

- install Helm with two ready nodes, `protocol=nvme`, RF=2, stage-2 multipath,
  operator-status, and lifecycle-owner;
- create two RF=2 NVMe/TCP PVCs and one long-lived mounted pod per PVC;
- verify both volumes start `Ready=True/first_volume_verified` with two live
  host NVMe paths;
- repeatedly remove and restore one generated blockvolume deployment, alternating
  the affected volume;
- after each loss, the affected volume must be
  `blocked/nvme_multipath_path_missing` with one live host path and the other
  volume must remain `ready/first_volume_verified` with two live host paths;
- after each restore, both volumes must return to
  `Ready=True/first_volume_verified` with two live host paths;
- mounted pod UIDs must never change, and mounted write/read must pass after
  every loss and every restore;
- reason codes, path counts, publish targets, and volume identities must never
  cross-contaminate between volumes;
- final cleanup must leave zero Kubernetes/iSCSI/process/multipath/hostPath
  residue.

## Gate

Proposed scenario:

```text
testops/scenarios/nvme-tcp-k8s-multivolume-mounted-path-churn-soak-chain.yaml
```

Proposed gate script:

```text
scripts/run-phase115-nvme-k8s-multivolume-mounted-path-churn-soak-gate.sh
```

The Phase 115 gate should reuse the Phase 114 install/mount/surface helpers, but
drive at least three transitions:

```text
cycle 1: volume 1 lose -> restore
cycle 2: volume 2 lose -> restore
cycle 3: volume 1 lose -> restore
```

Minimum terminal evidence:

```text
phase115_nvme_k8s_multivolume_mounted_path_churn_soak_status=ok
cycle_count=3
mounted_pods_preserved=true
mounted_io_after_loss_count=6
mounted_io_after_restore_count=6
cross_volume_reason_mixup=false
cross_volume_publish_target_mixup=false
all_restored_path_count=2
cleanup_status=ok
failure_count=0
```

## Verification

Local:

```text
bash -n scripts/run-phase115-nvme-k8s-multivolume-mounted-path-churn-soak-gate.sh
swblock validate testops/scenarios/nvme-tcp-k8s-multivolume-mounted-path-churn-soak-chain.yaml
go test ./core/csi ./core/frontend/nvme ./cmd/blockvolume ./core/ops
```

Live:

```text
swblock run testops/scenarios/nvme-tcp-k8s-multivolume-mounted-path-churn-soak-chain.yaml
```

## Non-Claims

Phase 115 still does not claim RoCE/NVMe-RDMA, performance/SLO, broad
distro/kernel compatibility, production HA, node loss, backup/restore, or
unbounded arbitrary path churn. It is a bounded supported-lab NVMe/TCP
multi-volume mounted path churn gate.
