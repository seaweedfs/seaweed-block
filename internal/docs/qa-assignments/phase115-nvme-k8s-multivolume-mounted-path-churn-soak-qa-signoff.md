# Phase 115 NVMe/TCP Mounted Multi-Volume Path Churn Soak QA Sign-off

Status: PASS.

Validated scenario:

```text
testops/scenarios/nvme-tcp-k8s-multivolume-mounted-path-churn-soak-chain.yaml
```

Strict passing QA run:

```text
20260630-123456-c28b
```

Result:

```text
25/25 PASS
```

## Scope

Phase 115 extends Phase 114 from one multi-volume path loss/restore transition
to bounded repeated churn:

```text
cycle 1: volume 1 lose -> restore
cycle 2: volume 2 lose -> restore
cycle 3: volume 1 lose -> restore
```

The gate installs the supported-lab Kubernetes NVMe/TCP path with RF=2,
stage-2 multipath, operator-status, and lifecycle-owner, then keeps one
long-lived mounted pod on each of two PVCs while alternating path loss and
restore across the volumes.

## Terminal Evidence

```text
phase115_nvme_k8s_multivolume_mounted_path_churn_soak_status=ok
volume_1_id=pvc-31f494af-9fb9-40c5-b257-9ed05d6b4951
volume_2_id=pvc-fc5f35e5-2c5c-4a48-ae39-ad8081c146ab
host_pre_churn_1_live_path_count=2
host_pre_churn_2_live_path_count=2
cycle_1_affected_volume=1
host_cycle-1-loss-v1_1_live_path_count=1
host_cycle-1-loss-v2_2_live_path_count=2
host_cycle-1-restore-v1_1_live_path_count=2
host_cycle-1-restore-v2_2_live_path_count=2
cycle_2_affected_volume=2
host_cycle-2-loss-v2_2_live_path_count=1
host_cycle-2-loss-v1_1_live_path_count=2
host_cycle-2-restore-v1_1_live_path_count=2
host_cycle-2-restore-v2_2_live_path_count=2
cycle_3_affected_volume=1
host_cycle-3-loss-v1_1_live_path_count=1
host_cycle-3-loss-v2_2_live_path_count=2
host_cycle-3-restore-v1_1_live_path_count=2
host_cycle-3-restore-v2_2_live_path_count=2
cycle_count=3
mounted_pods_preserved=true
mounted_io_after_loss_count=6
mounted_io_after_restore_count=6
cross_volume_reason_mixup=false
cross_volume_publish_target_mixup=false
all_restored_path_count=2
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
| Initial two-volume RF=2 NVMe attach | PASS | both volumes start with two live host NVMe paths |
| Mounted pod identity | PASS | `mounted_pods_preserved=true` across all churn cycles |
| Mounted I/O after loss | PASS | six post-loss write/read checks pass |
| Mounted I/O after restore | PASS | six post-restore write/read checks pass |
| Cycle 1 volume 1 loss/restore | PASS | volume 1 `2 -> 1 -> 2` live paths, volume 2 stays at 2 |
| Cycle 2 volume 2 loss/restore | PASS | volume 2 `2 -> 1 -> 2` live paths, volume 1 stays at 2 |
| Cycle 3 volume 1 loss/restore | PASS | repeated volume 1 `2 -> 1 -> 2` live paths |
| Reason isolation | PASS | `cross_volume_reason_mixup=false` |
| Publish-target isolation | PASS | `cross_volume_publish_target_mixup=false` |
| Cleanup | PASS | zero residue across Kubernetes, iSCSI, process, multipath, and hostPath |

## Harness Finding Resolved

The first live run reached the final assertion after completing the three churn
cycles, but failed because the gate looked for a `publish_target` field in the
NVMe status payload. The actual NVMe status uses `nvme_addr` and `nvme_addrs`.
The gate now treats `publish_target || nvme_addr` as the publish identity.
The corrected run passed without product changes.

## Verdict

PASS. Phase 115 closes the bounded repeated-transition claim for the
supported-lab NVMe/TCP path: two mounted RF=2 PVCs survive alternating path
loss and restore across three cycles without pod restart, I/O failure,
cross-volume status contamination, publish-target collision, or cleanup
residue.

## Non-Claims

This gate does not claim RoCE/NVMe-RDMA, performance/SLO, broad distro/kernel
compatibility, production HA, node loss, backup/restore, or unbounded arbitrary
path churn.
