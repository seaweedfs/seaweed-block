# Phase 42 Draft: Lifecycle Owner API / Admission Gate

Status: draft entry plan. Start only after Phase 41 QA sign-off.

## Product Goal

Prove the Kubernetes API boundary required before Seaweed Block ships any
`SwBlockVolume` finalizer mutation.

Phase 39 proved that CRD finalizers cannot be safely bounded with
`swblockvolumes/finalizers` RBAC alone. Phase 41 defined the lifecycle-owner
role and dry-run action contract. Phase 42 must turn that into a real API
proof.

Hard exit statement:

```text
The lifecycle-owner identity can patch exactly the approved finalizer shape on
SwBlockVolume main objects, and cannot patch spec, unrelated metadata, status
through the main object, or any storage/workload resource.
```

## Scope Contract

| In | Out |
|---|---|
| real apiserver/envtest/admission harness | production finalizer controller |
| lifecycle-owner RBAC proof | cleanup executor |
| finalizer-only admission rule or equivalent enforcement | rebuild/failback |
| negative patch matrix | backup/restore |
| audit evidence for accepted/rejected patches | NVMe ANA parity |

Phase 42 should not mutate real user data or ship product finalizer add/remove.
It is a gate phase.

## D1: Harness Choice And Baseline

Goal: choose the enforcement shape before writing the product mutation.

Acceptance:

```text
[ ] choose one harness: envtest, live throwaway cluster, or equivalent
[ ] load real SwBlockVolume CRD schema
[ ] install lifecycle-owner RBAC separate from operator-status
[ ] install admission policy/webhook or equivalent finalizer-only enforcement
[ ] prove operator-status RBAC remains status/events-only
```

Fail if:

```text
mock-only tests are used as the primary proof
operator-status receives main patch on swblockvolumes
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
[ ] patch preserves spec, labels, annotations, ownerReferences, and status
[ ] audit evidence records request, decision, reason, and observed object diff
```

## D3: Forbidden Main-Object Patches

Goal: prove broad main-object patch permission is effectively confined.

Forbidden patches:

```text
spec changes
status through main object
labels/annotations changes
ownerReferences changes
deletionTimestamp manipulation
foreign finalizer add/remove
mixed finalizer + spec patch
mixed finalizer + unrelated metadata patch
```

Acceptance:

```text
[ ] every forbidden patch is rejected by the real API/admission layer
[ ] rejection reason is stable enough for QA evidence
[ ] the object is unchanged after every rejected patch
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
[ ] kubectl auth can-i or client requests deny create/update/patch/delete
[ ] no product action attempts these mutations
[ ] operator-status permissions are unchanged
```

## D5: Delete-Safety Preconditions Stay External

Goal: prove Phase 42 does not bypass the Phase 41 decision model.

Acceptance:

```text
[ ] clean delete-safety evidence permits finalizer-release intent
[ ] blocked residue rejects finalizer-release intent
[ ] missing/stale cleanup evidence returns unknown
[ ] allowed/rejected/unknown decisions are visible through CRD status/actions
[ ] Phase 42 does not execute cleanup to make the decision pass
```

## D6: Multi-Volume Isolation

Goal: prove admission and action decisions are per object.

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
[ ] delete-safety decisions remain status/action facts, not cleanup execution
[ ] QA sign-off records that Phase 43 is now eligible to implement the first
    real finalizer mutation
```

