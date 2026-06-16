# Current Plan: Phase 44 - Delete Lifecycle Close Gate

Status: open, 0% complete. Started on 2026-06-15.

Branch: `phase41-lifecycle-owner-foundation`

Previous phase: Phase 43 is closed in
`internal/docs/finished-plans/phase43_finishedplan_bounded_finalizer_lifecycle.md`.

## Product Goal

Validate the complete user-visible delete lifecycle as one product path:

```text
install -> first PVC -> SwBlockVolume protected -> delete requested ->
blocked/unknown evidence holds finalizer -> clean evidence releases finalizer ->
object deletion completes -> uninstall leaves zero residue
```

Phase 43 proved add and release independently. Phase 44 proves the integrated
operation behaves as a coherent product capability across Kubernetes objects,
CRD status, Events, report/dashboard/explain surfaces, and cleanup.

## Why This Is Next

The operation layer is only useful if separate pieces compose:

```text
live object state + status evidence + action boundary + admitted mutation +
user-facing explanation + cleanup
```

Phase 44 is the close gate before moving to a broader release or larger
features. It should not add rebuild, failback, backup, or NVMe scope.

## Scope Contract

| In | Out |
|---|---|
| one first-volume user path | returned-replica rebuild |
| SwBlockVolume finalizer add and release | failback / repair |
| delete-safety hold/release status | automatic cleanup execution |
| CRD status/Event/report/dashboard agreement | PVC/PV/workload mutation |
| zero-residue uninstall verification | backup/snapshot/restore |
| multi-volume isolation smoke if cheap | NVMe ANA parity |

Allowed mutation:

```text
lifecycle-owner may patch only SwBlockVolume.metadata.finalizers.
operator-status remains status/events-only.
```

## D1: Release Artifact / Image Alignment

Goal: ensure the shipped image and chart can actually run the lifecycle-owner.

Acceptance:

```text
[ ] candidate image includes sw-block ops lifecycle-owner
[ ] chart renders lifecycleOwner.create=false by default
[ ] lifecycleOwner.create=true installs Deployment/RBAC/VAP
[ ] no chart flag/image skew
```

Fail if the chart references a subcommand or flag absent from the image.

## D2: First PVC Creates Protected SwBlockVolume

Goal: a normal Day-1 PVC path creates an observable, protected
`SwBlockVolume`.

Acceptance:

```text
[ ] Helm install succeeds with operatorStatus + lifecycleOwner enabled
[ ] first PVC writer/reader passes
[ ] SwBlockVolume exists for the PVC
[ ] lifecycle-owner adds exactly one protection finalizer
[ ] operator-status writes Ready=True / first_volume_verified
[ ] finalizer_added Event is bounded
```

## D3: Delete Request Holds On Unsafe Evidence

Goal: a delete request cannot complete while evidence is unsafe.

Acceptance:

```text
[ ] deleting SwBlockVolume with missing cleanup evidence remains Terminating
[ ] blocked residue evidence remains Terminating
[ ] stale cleanup evidence remains Terminating
[ ] status.deleteSafety decision is unknown/rejected as appropriate
[ ] Events explain the hold reason
[ ] lifecycle-owner does not run cleanup
```

Fail if unsafe evidence releases the finalizer.

## D4: Clean Evidence Releases And Deletion Completes

Goal: clean, fresh evidence releases only the Seaweed Block finalizer.

Acceptance:

```text
[ ] clean cleanup evidence sets decision=allowed state=releasable
[ ] lifecycle-owner removes only block.seaweedfs.com/swblockvolume-protection
[ ] foreign finalizers are preserved if present
[ ] SwBlockVolume deletion completes after release
[ ] finalizer_released Event is bounded
```

Fail if any non-finalizer field changes.

## D5: Surface Agreement

Goal: users see the same answer everywhere.

Acceptance:

```text
[ ] kubectl get/describe SwBlockVolume status agrees with ops report
[ ] operator-snapshot agrees with CRD status
[ ] dashboard /operator-snapshot.json agrees
[ ] ops explain names the same hold/release reason
[ ] no false Ready=True appears in blocked/unknown delete states
```

## D6: Multi-Volume Isolation Smoke

Goal: one deleting volume does not contaminate another volume.

Scenario:

```text
A deleting + blocked residue -> held
B deleting + clean evidence -> released
C normal ready volume -> remains ready, protected, no deleteSafety contamination
```

Acceptance:

```text
[ ] A held does not block B release
[ ] B release does not remove A/C finalizer
[ ] C remains Ready=True and protected
[ ] Events/status use the correct volume identity
```

## D7: Close / Release Decision

Phase 44 can close only if:

```text
[ ] D1-D6 PASS on a clean VAP-capable lab
[ ] lifecycle-owner has no cleanup/PVC/PV/workload/storage mutation power
[ ] operator-status remains status/events-only
[ ] final cleanup verifier reports zero residue
[ ] release notes and README claim only bounded SwBlockVolume finalizer
    lifecycle, not automatic cleanup or broad lifecycle automation
```

## Current Progress

- 0%: Phase 44 opened from Phase 43.
- Phase 43 already proved add and release as separate live gates.
- Phase 44 must prove the integrated path and release wording.

## Prerequisites / Risks

- Use a candidate image that includes `sw-block ops lifecycle-owner`.
- Use a VAP-capable lab; Rancher Desktop without VAP is not sufficient.
- Be careful with deleting CRs that have finalizers; every failed gate must
  include admin cleanup instructions.
- Do not paper over unsafe evidence by running cleanup inside lifecycle-owner.

## Next Step

Build or publish a candidate image from the current branch, then author/run the
D1-D4 integrated live scenario:

```text
install with operatorStatus + lifecycleOwner
create first PVC
observe protection finalizer
request delete
prove hold under unsafe evidence
patch/provide clean delete-safety evidence
prove finalizer release and object deletion
```
