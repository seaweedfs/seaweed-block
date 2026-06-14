# Current Plan: Phase 42 - Lifecycle Owner API / Admission Gate

Status: open, 0% complete. Started on 2026-06-14.

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
[ ] choose one primary harness: envtest, live throwaway cluster, or equivalent
[ ] load real SwBlockVolume CRD schema
[ ] install lifecycle-owner RBAC separate from operator-status
[ ] install admission policy/webhook or equivalent finalizer-only enforcement
[ ] prove operator-status RBAC remains status/events-only
```

Verification:

```text
documented harness command
negative test proving mock-only is not the source of truth
```

## D2: Allowed Finalizer Patch

Goal: prove the one intended mutation works.

Allowed patch shape:

```json
{"metadata":{"finalizers":["block.seaweedfs.com/swblockvolume-protection"]}}
```

Acceptance:

```text
[ ] lifecycle-owner can add the Seaweed Block finalizer
[ ] lifecycle-owner can remove the Seaweed Block finalizer
[ ] repeated add/remove is idempotent
[ ] spec, labels, annotations, ownerReferences, and status are preserved
[ ] audit evidence records request, decision, reason, and object diff
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

## Prerequisites / Risks

- `tp01` was reported `NotReady`/unreachable during recent QA. Restore before
  any multi-node live gate.
- Do not broaden operator-status RBAC.
- Do not treat mock-only tests as proof. The gate must exercise a real
  Kubernetes API/admission path or an equivalent envtest harness.
- If finalizer confinement cannot be proven, Phase 42 must fail closed and Phase
  43 must not start.

## Next Step

Implement D1: choose the harness shape, wire the real CRD/RBAC/admission test
surface, and prove the existing status-only observer remains unchanged.
