# Phase 43 Finished Plan: Bounded SwBlockVolume Finalizer Lifecycle

Status: closed on 2026-06-15.

Branch: `phase41-lifecycle-owner-foundation`

## Outcome

Phase 43 ships the first real bounded Kubernetes lifecycle mutation in Seaweed
Block:

```text
add/remove only block.seaweedfs.com/swblockvolume-protection on owned
SwBlockVolume objects, admitted by Kubernetes VAP and gated by delete-safety.
```

This closes the control-plane loop that Phase 42 prepared:

```text
status evidence -> delete-safety decision -> admitted lifecycle owner ->
finalizer mutation -> Kubernetes Event / observable object state
```

## Delivered

- Product lifecycle-owner command:
  - `sw-block ops lifecycle-owner`,
  - separate identity from operator-status,
  - dry-run capable,
  - periodic reconcile capable.
- Helm lifecycle-owner packaging:
  - disabled by default,
  - separate Deployment, ServiceAccount, RBAC,
  - ValidatingAdmissionPolicy and binding for finalizer-only patch shape.
- Finalizer add:
  - adds exactly `block.seaweedfs.com/swblockvolume-protection`,
  - preserves existing foreign finalizers,
  - idempotent on repeated reconcile,
  - emits bounded `finalizer_added` Events.
- Delete-safety-gated finalizer release:
  - missing delete-safety -> hold,
  - blocked residue -> hold,
  - stale/unknown evidence -> hold,
  - clean/releasable evidence -> remove only the Seaweed Block protection
    finalizer,
  - preserves foreign finalizers,
  - emits `finalizer_released` or hold-reason Events.
- Operator-status boundary preserved:
  - remains status/events-only,
  - no main-object patch power,
  - no finalizer mutation Events from operator-status.

## QA Evidence

- D1/D2 finalizer add:
  `internal/docs/qa-assignments/phase43-d1-d2-lifecycle-owner-finalizer-add-qa-signoff.md`
  - QA PASS on m02 (`k3s v1.34.4+k3s1`) with real
    ValidatingAdmissionPolicy enforcement,
  - lifecycle-owner finalizer add works and is idempotent,
  - VAP denies spec/status/labels/annotations/ownerReferences/foreign/mixed
    mutations,
  - operator-status cannot patch main `SwBlockVolume`,
  - lifecycle-owner has no workload/storage mutation power.
- D3/D4 finalizer release:
  `internal/docs/qa-assignments/phase43-d3-d4-lifecycle-owner-finalizer-release-qa-signoff.md`
  - QA PASS on m02 with fresh build from `252ec35`,
  - missing/blocked/stale delete-safety holds finalizer,
  - clean/releasable delete-safety releases only the protection finalizer,
  - foreign finalizer remains,
  - spec/status/labels/annotations unchanged,
  - no cleanup executed,
  - cleanup verifier reports zero residue.

## Non-Claims

Phase 43 does not execute cleanup, delete PVC/PV/workload/storage resources,
repair iSCSI/multipath/hostPath residue, rebuild replicas, fail back primaries,
perform backup/restore, implement NVMe ANA parity, or claim production SLOs.

The lifecycle-owner owns only the Seaweed Block `SwBlockVolume` protection
finalizer. It does not own PVC/PV finalizers.

## Required Carry-Forward

Phase 44 should validate the whole user-visible delete lifecycle as an
integrated path:

```text
install -> first PVC -> SwBlockVolume protected -> delete requested ->
blocked residue holds finalizer -> clean evidence releases finalizer ->
object deletion completes -> uninstall cleanup leaves zero residue
```

The close gate should prove CRD status, Events, report/dashboard/explain, and
actual Kubernetes object state agree across the hold and release transitions.
