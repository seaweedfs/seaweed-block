# Current Plan: Phase 43 - First Bounded Finalizer Mutation

Status: open, D1/D2 live QA PASS; D3/D4 implemented locally and awaiting live
QA. Started on 2026-06-15.

Branch: `phase41-lifecycle-owner-foundation`

Previous phase: Phase 42 is closed in
`internal/docs/finished-plans/phase42_finishedplan_lifecycle_owner_admission_gate.md`.

## Product Goal

Ship the first real Kubernetes lifecycle mutation in Seaweed Block:

```text
add/remove only block.seaweedfs.com/swblockvolume-protection on owned
SwBlockVolume objects, gated by delete-safety evidence.
```

Phase 42 proved the lifecycle-owner identity can hold main-object
`patch swblockvolumes` without becoming a broad mutator. Phase 43 turns that
proof into the product path, but keeps the scope narrow: finalizer only, no
cleanup execution, no PVC/PV/workload/storage mutation, no rebuild/failback,
no backup, no NVMe work.

Hard exit statement:

```text
The lifecycle-owner can add the protection finalizer, hold deletion while
delete-safety is blocked or unknown, and remove the finalizer only when
delete-safety is clean and fresh. Every decision is visible through CRD status,
Events, report/dashboard surfaces, and QA evidence.
```

## Why This Is The Right Next Step

This is the first ability closure after the operation-layer foundation:

```text
facts -> judgment -> admitted action owner -> real bounded mutation -> status/Event evidence
```

Starting returned-replica rebuild, NVMe ANA parity, or backup/restore before this
would add more lifecycle transitions without proving the product can safely own
a small Kubernetes mutation. Phase 43 keeps the blast radius intentionally small.

## Scope Contract

| In | Out |
|---|---|
| lifecycle-owner component or mode | operator-status main-object patch |
| SwBlockVolume protection finalizer add/remove | PVC/PV finalizer ownership |
| delete-safety hold/release decisions | cleanup execution |
| Events for hold/release | iSCSI/multipath/hostPath mutation |
| CRD status/report/dashboard agreement | workload/storageclass mutation |
| idempotency and multi-volume isolation | rebuild/failback/promotion |
| uninstall/cleanup zero-residue gate | backup/restore/NVMe ANA parity |

Allowed implementation rule:

```text
operator-status must remain status/events-only.
lifecycle-owner may patch SwBlockVolume main objects only through the
Phase 42-admitted finalizer shape.
```

## D1: Product Wiring And RBAC Separation

Goal: introduce the product lifecycle-owner path without widening
operator-status.

Acceptance:

```text
[ ] lifecycle-owner has a separate identity from operator-status
[ ] lifecycle-owner RBAC matches the Phase 42 boundary
[ ] operator-status still cannot patch SwBlockVolume main objects
[ ] admission policy or equivalent finalizer-only enforcement is installed
[ ] lifecycle-owner can run disabled/dry-run by default if needed for rollout
```

Verification:

```text
kubectl auth can-i patch swblockvolumes --as <operator-status-sa> == no
kubectl auth can-i patch swblockvolumes --as <lifecycle-owner-sa> == yes
Phase 42 forbidden patch matrix still passes
```

## D2: Finalizer Add On Owned SwBlockVolume

Goal: add the protection finalizer idempotently to owned `SwBlockVolume`
objects.

Acceptance:

```text
[ ] lifecycle-owner adds block.seaweedfs.com/swblockvolume-protection
[ ] repeated reconcile does not duplicate or churn finalizers
[ ] spec, status, labels, annotations, ownerReferences are unchanged
[ ] foreign finalizers are not added or removed
[ ] Event and status/action evidence names the volume identity
```

Fail if any non-finalizer field changes.

## D3: Blocked / Unknown Delete Holds Finalizer

Goal: deletion is held when release evidence is unsafe.

Acceptance:

```text
[ ] blocked residue -> deleteSafety.state=blocked decision=rejected
[ ] missing cleanup evidence -> state=requested decision=unknown
[ ] stale cleanup evidence -> state=requested decision=unknown
[ ] finalizer remains present in all three cases
[ ] no cleanup is executed to make the decision pass
[ ] Events explain hold reason without claiming release
```

Fail if blocked, missing, or stale evidence removes the finalizer.

## D4: Clean Delete Releases Finalizer

Goal: release finalizer only when cleanup evidence is clean and fresh.

Acceptance:

```text
[ ] clean cleanup evidence -> state=releasable decision=allowed
[ ] lifecycle-owner removes only the Seaweed Block protection finalizer
[ ] finalizer removal is idempotent
[ ] finalizer_released Event is emitted once or bounded/stable
[ ] object deletion can complete after release
```

Fail if clean evidence causes any PVC/PV/workload/host mutation.

## D5: Multi-Volume Isolation

Goal: one volume's delete lifecycle does not contaminate another.

Scenario:

```text
A: delete requested + residue -> held
B: delete requested + clean evidence -> released
C: ready volume with no delete request -> finalizer remains, no deleteSafety
D: stale evidence -> unknown, held
```

Acceptance:

```text
[ ] A remains held and does not block B release
[ ] B release does not affect A/C/D
[ ] C has no deleteSafety contamination
[ ] D remains unknown/held
[ ] Events and status use the correct volume identity
```

## D6: Close Gate

Phase 43 can close only if:

```text
[ ] Phase 42 D1-D4 admission gate still passes
[ ] finalizer add works on owned SwBlockVolume
[ ] blocked/missing/stale delete-safety holds finalizer
[ ] clean/fresh delete-safety releases finalizer
[ ] operator-status remains status/events-only
[ ] lifecycle-owner has no cleanup/PVC/PV/workload/storage mutation power
[ ] uninstall/cleanup verifier reports zero residue
[ ] QA sign-off records Phase 44 is eligible for delete lifecycle release close
```

## Current Progress

- D1/D2 live QA PASS:
  - `sw-block ops lifecycle-owner` command added.
  - separate Helm lifecycle-owner Deployment/RBAC added, disabled by default.
  - lifecycle-owner admission policy added for finalizer-only patch shape.
  - Kubernetes client can list `SwBlockVolume` objects and patch only
    `metadata.finalizers`.
  - live VAP/RBAC boundary and idempotent finalizer add validated in
    `internal/docs/qa-assignments/phase43-d1-d2-lifecycle-owner-finalizer-add-qa-signoff.md`.
- D3/D4 local implementation complete:
  - lifecycle-owner holds deleting volumes when `status.deleteSafety` is absent,
    blocked, or unknown/stale.
  - lifecycle-owner removes only the Seaweed Block protection finalizer when
    `status.deleteSafety` is releasable and allowed.
  - local tests pass; live QA pending.

## Prerequisites / Risks

- Keep the Phase 42 admission policy in the loop; RBAC alone is not enough for
  CRD finalizers.
- Do not put main-object patch permission on operator-status.
- Deleting a CR with finalizers can easily leave stuck test objects. Every gate
  must include cleanup and force-clean instructions.
- Do not add automatic cleanup to make finalizer release pass.
- Keep Events bounded; repeated reconcile should not create unbounded duplicate
  lifecycle Events.

## Next Step

Run D3/D4 live QA:

```text
blocked/missing/stale delete-safety holds finalizer
clean/fresh delete-safety releases only Seaweed Block finalizer
operator-status remains status/events-only
lifecycle-owner keeps no cleanup/PVC/PV/workload/storage mutation power
```
