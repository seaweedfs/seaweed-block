# Current Plan: Phase 114 NVMe/TCP K8s Multi-Volume Mounted Path Isolation

Status: blocked by a product restore-safety defect.

Strict QA run `20260630-011812-2220` failed. Sign-off:
`internal/docs/qa-assignments/phase114-nvme-k8s-multivolume-mounted-path-isolation-qa-signoff.md`.

## Why This Is Next

Phase 112 proved that a single mounted RF=2 NVMe/TCP PVC can continue I/O after
one observed path is removed. Phase 113 proved the matching single-volume
restore path: the removed blockvolume deployment can return, the mounted pod
keeps the same UID, I/O still works, and status converges back to
`Ready=True/first_volume_verified`.

That still left the multi-volume close gate. A product-ready NVMe/TCP path must
not only handle one PVC; it must prove that path loss and restoration for one
volume do not corrupt, block, or confuse another mounted volume.

## Product Goal

Prove that two mounted Kubernetes workloads on two independent RF=2 NVMe/TCP
PVCs remain isolated when one volume loses and restores one frontend path.

Required behavior:

- install Helm with two ready nodes, `protocol=nvme`, operator-status enabled,
  lifecycle-owner enabled, and RF=2;
- create two PVCs through CSI and verify normal writer/reader data paths;
- create one long-lived mounted pod per PVC;
- wait for both `SwBlockVolume` objects to reach
  `Ready=True/first_volume_verified` and
  `SwBlockVolume.status.nvme.pathCount=2`;
- scale one generated `sw-blockvolume` deployment for volume 1 to zero;
- verify both mounted pod UIDs are unchanged and both mounted pods still write
  and read;
- verify volume 1 becomes `blocked/nvme_multipath_path_missing` with one path;
- verify volume 2 stays `ready/first_volume_verified` with two paths;
- verify no cross-volume reason or identity mix-up;
- scale the removed volume 1 deployment back to one replica;
- verify both mounted pod UIDs are still unchanged and both mounted pods still
  write and read;
- verify both volumes return to `Ready=True/first_volume_verified` with two
  observed paths;
- cleanup leaves zero Kubernetes/NVMe/iSCSI/process/multipath/hostPath residue.

## Gate

Scenario:

```text
testops/scenarios/nvme-tcp-k8s-multivolume-mounted-path-isolation-chain.yaml
```

Gate script:

```text
scripts/run-phase114-nvme-k8s-multivolume-mounted-path-isolation-gate.sh
```

The Phase 114 wrapper also fixes a gate-safety issue found while authoring the
test: cleanup traps must restore `set -e` before returning, otherwise later
mounted-I/O assertions can be silently ignored. The same correction was applied
to the Phase 111 wrapper.

## Current Evidence

The degraded multi-volume isolation portion passes:

```text
mounted_pods_preserved_after_loss=true
mounted_io_after_loss_count=2
degraded_volume_path_count=1
untouched_volume_path_count=2
degraded_volume_reason=nvme_multipath_path_missing
untouched_volume_reason=first_volume_verified
cross_volume_reason_mixup=false
degraded_surface_ready_true_count=0
```

The restored CRD state looks healthy:

```text
sw-block-multi-pvc-1 ... ready first_volume_verified 2
sw-block-multi-pvc-2 ... ready first_volume_verified 2
```

But the mounted workload on the restored volume fails for the full retry window:

```text
sh: can't create /data/phase114-mounted-1.txt: Input/output error
command terminated with exit code 1
```

Cleanup remains clean:

```text
cleanup_status=ok
k8s_residue_count=0
iscsi_residue_count=0
process_residue_count=0
multipath_residue_count=0
hostpath_residue_count=0
failure_count=0
```

## Blocking Gap

The product currently reports the restored affected volume as
`Ready=True/first_volume_verified` with two observed NVMe paths while mounted
I/O on that same volume is returning persistent EIO. That is a false restored
ready claim.

The likely boundary is returned-replica/frontend publication: a restored path
must not be counted as healthy or published as safe until the returned replica
has positive safe evidence. If the product cannot prove that, status must stay
non-ready with a specific reason instead of reporting
`first_volume_verified`.

## Next Engineering Step

Inspect and fix the NVMe restored-path readiness path:

1. Find where restored NVMe paths are counted into
   `SwBlockVolume.status.nvme.pathCount` and `first_volume_verified`.
2. Find where blockmaster/frontend publication reintroduces the restored path.
3. Require positive returned-replica readiness before claiming the restored path
   healthy, or keep the volume non-ready with a precise reason.
4. Re-run Phase 114 strict until the affected mounted pod writes after restore
   and the untouched volume remains isolated.

## Non-Claims

Phase 114 does not claim RoCE/NVMe-RDMA, performance/SLO, broad distro/kernel
compatibility, production HA, arbitrary path churn, automatic rebuild safety,
or safe returned-replica publication. It is specifically the multi-volume
mounted NVMe/TCP path-loss and restore close gate.
