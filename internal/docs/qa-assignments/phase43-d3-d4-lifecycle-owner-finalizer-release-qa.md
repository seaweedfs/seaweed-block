# Phase 43 D3/D4 QA: Delete-Safety Gated Finalizer Release

## Scope

Validate the lifecycle-owner release half:

- hold the Seaweed Block protection finalizer while delete-safety is blocked,
  missing, or stale.
- remove only the Seaweed Block protection finalizer when CRD
  `status.deleteSafety` says release is allowed.

This gate must not execute cleanup and must not mutate PVC/PV/workload/storage
objects.

## Preconditions

- Phase 43 D1/D2 is PASS.
- Use a VAP-capable Kubernetes lab.
- Use an image built from the candidate commit.
- Install with lifecycle-owner enabled:

```text
--set lifecycleOwner.create=true
--set lifecycleOwner.dryRun=false
```

## Required Checks

### G1 Local Contract

```text
go test ./core/ops ./cmd/sw-block
helm lint charts/seaweed-block
```

Pass criteria:

- tests pass.
- chart lint passes.

### G2 Hold On Missing / Blocked / Stale Evidence

Create or patch three `SwBlockVolume` objects with the protection finalizer and
a deletion timestamp:

- missing delete-safety status.
- `status.deleteSafety.state=blocked`, `decision=rejected`.
- `status.deleteSafety.state=requested`, `decision=unknown`,
  `reason=cleanup_evidence_stale`.

Let lifecycle-owner reconcile.

Pass criteria:

- all three objects keep the protection finalizer.
- no finalizer patch removes the protection finalizer.
- lifecycle-owner output reports `finalizer_held=3`,
  `finalizer_released=0`.
- Warning Events explain the hold reason and do not claim release.
- no cleanup command is executed.

### G3 Release On Clean Fresh Evidence

Create or patch one deleting `SwBlockVolume` with:

```text
metadata.finalizers=[
  "example.com/foreign",
  "block.seaweedfs.com/swblockvolume-protection"
]
status.deleteSafety.state=releasable
status.deleteSafety.decision=allowed
status.deleteSafety.finalizerReleaseAllowed=true
```

Let lifecycle-owner reconcile.

Pass criteria:

- finalizers become `["example.com/foreign"]`.
- spec, status, labels, annotations, and ownerReferences are unchanged.
- the object can finish deletion after the Seaweed Block finalizer is removed.
- lifecycle-owner output reports `finalizer_released=1`.
- a bounded Normal `finalizer_released` Event is emitted.

### G4 Admission / RBAC Boundary

Re-run the Phase 42/43 forbidden mutation matrix.

Pass criteria:

- operator-status still cannot patch main `swblockvolumes`.
- lifecycle-owner can patch main `swblockvolumes` only through VAP-admitted
  finalizer-only changes.
- lifecycle-owner cannot create/update/patch/delete pods, PVCs, PVs, Secrets,
  StorageClasses, Deployments, Nodes, CSIDrivers, or CSINodes.
- lifecycle-owner cannot change spec, status, labels, annotations,
  ownerReferences, foreign finalizers, or mixed finalizer + non-finalizer fields.

### G5 Cleanup

Uninstall and verify:

```text
bash scripts/verify-helm-cleanup.sh <repo-root>
```

Pass criteria:

- `cleanup_status=ok`.
- no stuck `SwBlockVolume` objects, VAPs, bindings, RBAC, pods, PVCs, PVs,
  iSCSI sessions/node records, multipath maps, or dmsetup devices remain.

## Blocking Findings

Block D3/D4 if:

- blocked, missing, or stale evidence releases the finalizer.
- clean releasable evidence removes a foreign finalizer.
- any non-finalizer field changes.
- lifecycle-owner executes cleanup.
- lifecycle-owner gains workload/storage mutation power.
