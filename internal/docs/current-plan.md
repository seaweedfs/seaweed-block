# Current Plan: Phase 42 - Lifecycle Owner API / Admission Gate

Status: open, 57% complete. Started on 2026-06-14.

Branch: `phase41-lifecycle-owner-foundation`

Previous phase: Phase 41 is closed in
`internal/docs/finished-plans/phase41_finishedplan_lifecycle_owner_foundation.md`.

## Product Goal

Prove the real Kubernetes API/admission boundary required before Seaweed Block
ships any `SwBlockVolume` finalizer mutation.

Phase 39 proved that CRD finalizers require main-object
`patch swblockvolumes`. Phase 41 defined the lifecycle-owner role and shipped
only dry-run/status decisions. Phase 42 must prove that a future lifecycle owner
can hold that main-object patch permission without becoming a broad mutating
operator.

Hard exit statement:

```text
The lifecycle-owner identity can patch exactly the approved finalizer shape on
SwBlockVolume main objects, and cannot patch spec, unrelated metadata, status
through the main object, or any storage/workload resource.
```

## Why This Is Still Operation Layer Work

This phase is not another generic ops cleanup loop. It is the concrete gate that
lets the product move from status/dry-run guidance to the first safe Kubernetes
lifecycle mutation in Phase 43.

Until this proof exists, returned-replica rebuild/reintegration, NVMe ANA parity,
backup/restore, and any cleanup executor would add more state transitions before
the product has a trusted mutation owner.

## Scope Contract

| In | Out |
|---|---|
| real apiserver/envtest/admission harness | product finalizer controller |
| lifecycle-owner RBAC proof | cleanup execution |
| finalizer-only admission rule or equivalent enforcement | rebuild/failback |
| accepted/rejected patch audit evidence | backup/restore |
| delete-safety precondition carry-forward | NVMe ANA parity |
| multi-volume object isolation | broad production lifecycle claims |

Allowed implementation rule:

```text
Phase 42 may introduce test-only lifecycle-owner identities, RBAC, and
admission/enforcement code needed to prove the boundary.

Phase 42 must not enable product finalizer add/remove in the released
operator-status path.
Phase 42 must not broaden operator-status beyond status/events-only.
```

## D1: Harness Choice And Baseline

Goal: choose and wire the real API proof.

Acceptance:

```text
[x] choose one primary harness: live Kubernetes ValidatingAdmissionPolicy gate
[x] load real SwBlockVolume CRD schema
[x] install lifecycle-owner RBAC separate from operator-status
[x] install admission policy/webhook or equivalent finalizer-only enforcement
[x] prove operator-status RBAC remains status/events-only
[x] fail on real admission mistakes instead of mock-only proof
```

Verification:

```text
scripts/run-phase42-lifecycle-owner-admission-gate.sh
scripts/run-phase42-lifecycle-owner-admission-gate.ps1
testops/scenarios/lifecycle-owner-admission-gate-chain.yaml
```

## D2: Allowed Finalizer Patch

Goal: prove the one intended mutation works.

Allowed patch shape:

```json
{"metadata":{"finalizers":["block.seaweedfs.com/swblockvolume-protection"]}}
```

Acceptance:

```text
[ ] lifecycle-owner can add the Seaweed Block finalizer on a real apiserver
[ ] lifecycle-owner can remove the Seaweed Block finalizer on a real apiserver
[ ] admission policy propagation is proven before positive/negative assertions
[ ] repeated add/remove is idempotent
[ ] spec, labels, annotations, ownerReferences, and status are preserved
[ ] audit evidence records request, decision, reason, and observed object diff
```

## D3: Forbidden Main-Object Patches

Goal: prove broad main-object patch permission is effectively confined.

Forbidden patches:

```text
spec changes
status through the main object
labels or annotations changes
ownerReferences changes
deletionTimestamp manipulation
foreign finalizer add/remove
mixed finalizer + spec patch
mixed finalizer + unrelated metadata patch
```

Acceptance:

```text
[ ] every forbidden patch is rejected by real API/admission
[ ] rejection reason is stable enough for QA evidence
[ ] object is unchanged after every rejected patch
[ ] status cannot be mutated by lifecycle-owner through main-object no-op or
    `/status` subresource paths
```

## D4: Forbidden Resource Mutations

Goal: prove lifecycle-owner does not become a storage/workload mutator.

Forbidden resources:

```text
pods
deployments
persistentvolumeclaims
persistentvolumes
storageclasses
secrets
nodes
csidrivers
csinodes
```

Acceptance:

```text
[ ] create/update/patch/delete are denied for each forbidden resource
[ ] no product action attempts these mutations
[ ] operator-status permissions are unchanged
```

## D5: Delete-Safety Preconditions Stay External

Goal: prove Phase 42 does not bypass the Phase 41 decision model.

Acceptance:

```text
[ ] clean delete-safety evidence permits finalizer-release intent
[ ] blocked residue rejects finalizer-release intent
[ ] missing or stale cleanup evidence returns unknown
[ ] decisions are visible through CRD status/actions
[ ] Phase 42 does not execute cleanup to make the decision pass
[ ] lifecycle-owner action remains dry-run with mutation_allowed=false until
    Phase 43
```

## D6: Multi-Volume Isolation

Goal: prove API/admission and action decisions are per object.

Scenario:

```text
A: finalizer patch allowed
B: spec patch rejected
C: blocked delete-safety rejected
D: stale evidence unknown
```

Acceptance:

```text
[ ] A's allowed patch does not affect B/C/D
[ ] B's rejected patch leaves B unchanged
[ ] C/D status decisions do not block A's API proof
[ ] all Events/audit evidence use the correct volume identity
[ ] stale deleteSafety clears when current evidence is absent
```

## D7: Close Gate

Phase 42 can close only if:

```text
[ ] real API/admission proof is used
[ ] finalizer-shaped add/remove works
[ ] forbidden main-object patches fail
[ ] forbidden resource mutations fail
[ ] operator-status remains status/events-only
[ ] delete-safety remains status/action evidence, not cleanup execution
[ ] QA sign-off records that Phase 43 is eligible to implement the first real
      finalizer mutation
```

## Current Progress

- 0%: Phase 42 opened from the Phase 41 lifecycle-owner foundation. The
  planning contract is drafted in
  `internal/docs/ref/phase42-lifecycle-owner-api-admission-gate.md`.
- 14%: D1 dev-complete. Chose a live Kubernetes
  `ValidatingAdmissionPolicy` harness instead of mock-only tests. Added
  PowerShell/Bash runners and a TestOps scenario that apply the real
  `SwBlockVolume` CRD, create separate operator-status and lifecycle-owner
  identities, install finalizer-only admission, and assert operator-status
  remains status/events-only. Local Rancher Desktop smoke fails closed with
  `blocked_reason=validating_admission_policy_unavailable`; QA must run this on
  a cluster with `ValidatingAdmissionPolicy` support.
- 21%: D1 QA found a real live-admission defect: optional-field CEL comparisons
  denied a legitimate finalizer add when `.status` or `ownerReferences` were
  absent, and the gate had no VAP propagation wait. The harness now guards
  optional fields with `has()` and waits until a known-bad lifecycle-owner patch
  is denied before running assertions. This is pending QA rerun on m02.
- 28%: D1 QA re-run passed on m02 (`k3s v1.34.4`) at `116d381`. The
  lifecycle-owner finalizer add/remove is allowed, forbidden main-object patches
  are denied, forbidden resource patch checks are denied, object integrity is
  preserved, and cleanup leaves no admission/RBAC residue.
- 36%: D2/D3/D4 breadth dev-complete. The same live gate now also checks
  idempotent add/remove, annotation/ownerReferences/deletionTimestamp and mixed
  metadata rejection, `/status` denial plus no main-object status mutation,
  object-integrity preservation after rejected patches, and
  create/update/patch/delete denial for forbidden Kubernetes resources. Pending
  QA rerun.
- 43%: D1-D4 QA passed on m02 at `d3a1e0e`. The lifecycle-owner main-object
  patch is confined by real Kubernetes admission to exactly the Seaweed Block
  finalizer, while operator-status remains status/events-only.
- 57%: D5/D6 dev-complete. Added a focused delete-safety decision gate that
  runs the core delete-safety and operator-status regressions, summarizes
  clean/blocked/missing/stale decisions, proves the lifecycle-owner action is
  still `dry_run` with `mutation_allowed=false`, asserts no finalizer patches or
  finalizer Events, and proves multi-volume delete-safety isolation. Pending QA
  rerun.

## Prerequisites / Risks

- `tp01` was reported `NotReady`/unreachable during recent QA. Restore before
  any multi-node live gate.
- Do not broaden operator-status RBAC.
- Do not treat mock-only tests as proof. The gate must exercise a real
  Kubernetes API/admission path or an equivalent envtest harness.
- If finalizer confinement cannot be proven, Phase 42 must fail closed and Phase
  43 must not start.

## Next Step

Run the D5/D6 delete-safety decision gate:

```text
phase42_delete_safety_decision_status=ok
clean_delete_safety_decision=allowed
blocked_delete_safety_decision=rejected
missing_delete_safety_decision=unknown
stale_delete_safety_decision=unknown
lifecycle_owner_action_mode=dry_run
lifecycle_owner_action_mutation_allowed=false
finalizer_patch_count=0
multi_volume_delete_safety_isolation=true
```

QA assignment:
`internal/docs/qa-assignments/phase42-d5-d6-delete-safety-decision-qa.md`.
