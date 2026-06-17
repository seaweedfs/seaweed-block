# Current Plan: Phase 44 - Delete Lifecycle Close Gate

Status: closed on 2026-06-17.

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
[x] candidate image includes sw-block ops lifecycle-owner
[x] chart renders lifecycleOwner.create=false by default
[x] lifecycleOwner.create=true installs Deployment/RBAC/VAP
[x] no chart flag/image skew
```

Fail if the chart references a subcommand or flag absent from the image.

## D2: First PVC Creates Protected SwBlockVolume

Goal: a normal Day-1 PVC path creates an observable, protected
`SwBlockVolume`.

Ownership boundary:

```text
CSI controller creates/updates SwBlockVolume metadata/spec identity after
CreateVolume succeeds. operator-status writes only .status. lifecycle-owner
patches only metadata.finalizers.
```

Acceptance:

```text
[x] Helm install succeeds with operatorStatus + lifecycleOwner enabled
[x] first PVC writer/reader passes
[x] SwBlockVolume exists for the PVC
[x] lifecycle-owner adds exactly one protection finalizer
[x] operator-status writes Ready=True / first_volume_verified
[x] finalizer_added Event is bounded
```

## D3: Delete Request Holds On Unsafe Evidence

Goal: a delete request cannot complete while evidence is unsafe.

Acceptance:

```text
[x] deleting SwBlockVolume with missing cleanup evidence remains Terminating
[x] blocked residue evidence remains Terminating
[x] stale cleanup evidence remains Terminating
[x] status.deleteSafety decision is unknown/rejected as appropriate
[x] Events explain the hold reason
[x] lifecycle-owner does not run cleanup
```

Fail if unsafe evidence releases the finalizer.

Implementation note:

```text
operator-status now lists live SwBlockVolume objects and projects deleteSafety
for objects with deletionTimestamp. Cleanup remains external evidence: pass the
verifier output with --cleanup-summary; operator-status reads it, never runs it.
```

## D4: Clean Evidence Releases And Deletion Completes

Goal: clean, fresh evidence releases only the Seaweed Block finalizer.

Acceptance:

```text
[x] clean cleanup evidence sets decision=allowed state=releasable
[x] lifecycle-owner removes only block.seaweedfs.com/swblockvolume-protection
[x] foreign finalizers are preserved if present
[x] SwBlockVolume deletion completes after release
[x] finalizer_released Event is bounded
```

Fail if any non-finalizer field changes.

## D5: Surface Agreement

Goal: users see the same answer everywhere.

Acceptance:

```text
[x] kubectl get/describe SwBlockVolume status agrees with ops report
[x] operator-snapshot agrees with CRD status
[x] dashboard /operator-snapshot.json agrees
[x] ops explain names the same hold/release reason
[x] no false Ready=True appears in blocked/unknown delete states
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
[x] A held does not block B release
[x] B release does not remove A/C finalizer
[x] C remains Ready=True and protected
[x] Events/status use the correct volume identity
```

## D7: Close / Release Decision

Phase 44 can close only if:

```text
[x] D1-D6 PASS on a clean VAP-capable lab
[x] lifecycle-owner has no cleanup/PVC/PV/workload/storage mutation power
[x] operator-status remains status/events-only
[x] final cleanup verifier reports zero residue
[x] release notes and README claim only bounded SwBlockVolume finalizer
    lifecycle, not automatic cleanup or broad lifecycle automation
```

## Current Progress

- Phase 44 finished plan:
  `internal/docs/finished-plans/phase44_finishedplan_delete_lifecycle_close_gate.md`.
- v0.5 beta candidate release note:
  `docs/releases/v0.5-beta-candidate.md`.
- D2 implementation landed in `e56b844`: normal CSI CreateVolume registers the
  SwBlockVolume identity CR when operator-status or lifecycle-owner surfaces
  are enabled.
- D2 QA PASS:
  `internal/docs/qa-assignments/phase44-d2-integrated-swblockvolume-cr-qa-signoff.md`.
- D3/D4 implementation added in `8669d4a`: operator-status consumes live deleting
  SwBlockVolume objects plus externally generated `cleanup-summary.txt`
  evidence to publish deleteSafety hold/release status. It still does not patch
  finalizers, run cleanup, or mutate PVC/PV/workloads/storage.
- D3/D4 QA PASS:
  `internal/docs/qa-assignments/phase44-d3-d4-delete-lifecycle-close-gate-qa-signoff.md`.
- D3/D4 polish added after QA: report/explain/dashboard can consume
  `--cleanup-summary` in live in-cluster mode, and operator-status skips a
  disappeared SwBlockVolume CR instead of aborting the whole reconcile.
- D5/D6 QA PASS:
  `internal/docs/qa-assignments/phase44-d5-d6-surface-isolation-close-qa-signoff.md`.
- D5/D6 polish added after QA: `ops explain volume --cleanup-summary` can now
  explain a deleting SwBlockVolume even when the live inventory no longer
  contains the managed volume.
- Local checks pass:
  `go test ./core/csi ./cmd/blockcsi`, `go test ./core/ops ./cmd/sw-block`,
  and `helm lint charts/seaweed-block`.
- D2 QA assignment prepared:
  `internal/docs/qa-assignments/phase44-d2-integrated-swblockvolume-cr-qa.md`.
- D3/D4 QA assignment prepared:
  `internal/docs/qa-assignments/phase44-d3-d4-delete-lifecycle-close-gate-qa.md`.
- Phase 43 already proved add and release as separate live gates.
- Phase 44 must prove the integrated path and release wording.

## Prerequisites / Risks

- Use a candidate image that includes `sw-block ops lifecycle-owner`.
- Use a candidate CSI image that includes `blockcsi --swblockvolume-cr-namespace`;
  older CSI images will not support D2 integrated CR creation.
- Use a VAP-capable lab; Rancher Desktop without VAP is not sufficient.
- Be careful with deleting CRs that have finalizers; every failed gate must
  include admin cleanup instructions.
- Do not paper over unsafe evidence by running cleanup inside lifecycle-owner.

## Next Step

Publish matching images from the final release commit and rerun the
release-candidate smoke:

```text
claim bounded SwBlockVolume finalizer lifecycle only
claim evidence-driven hold/release, not automatic cleanup
publish both sw-block and sw-block-csi images together
rerun release-candidate smoke if image digests change
```
