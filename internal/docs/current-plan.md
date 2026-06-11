# Current Plan: Phase 39 - Finalizer / Delete Safety

Status: active, 70% complete. Started on 2026-06-10.

Branch: `phase33-testops-failure-hardening`

Previous phase: Phase 38 is closed in
`internal/docs/finished-plans/phase38_finishedplan_lifecycle_action_model_executable_contract.md`.

## Product Goal

Add the first narrow mutating operator path: safe finalizer/delete behavior for
Seaweed Block lifecycle objects.

Phases 35-38 established read-only CRD status, Events, live node evidence,
support/cleanup visibility, and executable action decisions. Phase 39 validates
that model with one real mutation: the operator may protect and release a
`SwBlockVolume` finalizer only when its preconditions and cleanup evidence say
the object can be safely deleted.

The hard exit statement:

```text
Deleting a managed SwBlockVolume either completes safely with no residue or is
blocked with explicit status, reason, evidence, and a non-mutating next step.
The operator mutates only finalizer metadata and status/events in this phase.
```

## Scope Contract

| In | Out |
|---|---|
| SwBlockVolume finalizer contract | PVC finalizer ownership |
| delete-requested status projection | automatic cleanup execution |
| bounded finalizer add/remove mutation | iSCSI/multipath/hostPath deletion by operator |
| idempotent reconcile and retry behavior | promotion/fencing/rebuild/failback |
| cleanup verifier evidence consumption | backup/snapshot/restore |
| Kubernetes Events for blocked/released delete | NVMe ANA parity |
| RBAC narrowed to finalizers + status + events | dashboard mutation buttons |
| TestOps delete-safety gates | broad production delete lifecycle |

Allowed implementation rule:

```text
Phase 39 may patch SwBlockVolume metadata.finalizers, SwBlockVolume/status,
and Kubernetes Events.

Phase 39 must not delete PVC/PV/Pods/Deployments/StorageClasses, run cleanup
scripts, change Helm releases, import images, touch iSCSI/multipath/dmsetup,
remove hostPath data, promote/fence/rebuild/failback, or mutate storage.
```

## D1: Delete-Safety Contract Review

Goal: define exactly what the finalizer owns and which facts are required before
it can release deletion.

Status: dev-complete; QA/internal review pending.

Acceptance:

```text
[x] SwBlockVolume finalizer name is defined
[x] delete states are defined: not_requested, requested, blocked, releasable,
      released
[x] required facts are defined: volume identity, PVC/PV linkage, cleanup
      summary, active sessions, multipath/dmsetup state, generated workload
      residue, hostPath residue
[x] action contract maps delete release/block to Phase 38 evaluator language
[x] non-claims explicitly exclude automatic cleanup and PVC finalizer ownership
```

Verification:

```text
go test ./core/ops
internal review of finalizer/delete contract doc
```

## D2: Status-Only Delete Projection

Goal: before adding finalizer mutation, prove delete-requested and
delete-blocked states project correctly from evidence.

Status: dev-complete; QA/internal review pending.

Acceptance:

```text
[x] deletionTimestamp-like evidence projects DeletionRequested/Blocked status
[x] residue evidence projects CleanupRequired=True and reason=cleanup_required
[x] clean evidence projects delete releasable, not false blocked
[x] report, explain, dashboard, operator-snapshot, and CRD status agree
[x] no finalizer mutation is enabled yet
```

Verification:

```text
go test ./core/ops ./cmd/sw-block
from-bundle replay for clean and residue delete evidence
```

## D3: Finalizer Mutation Boundary

Goal: add the minimum RBAC and code path for finalizer metadata mutation, with
no storage/workload mutation.

Status: dev-complete; QA/internal review pending.

Acceptance:

```text
[x] operator can patch SwBlockVolume metadata.finalizers only
[x] operator cannot patch spec, PVC/PV, pods, deployments, storageclasses,
      secrets, nodes, iSCSI, multipath, hostPath, or Helm resources
[x] finalizer is added idempotently to managed SwBlockVolume objects
[x] finalizer removal requires a releasable delete decision
[x] all finalizer decisions emit status and Events
```

Verification:

```text
go test ./core/ops ./cmd/sw-block
helm template/lint with updated RBAC
kubectl auth can-i boundary sweep
```

## D4: Delete Block Gate

Goal: prove deletion is held when residue or insufficient evidence exists.

Status: dev-complete; QA/live validation pending.

Acceptance:

```text
[x] deleting SwBlockVolume with active/residue evidence keeps finalizer
[x] status shows blocked or cleanup_required with stable reason
[x] safe next step is observe.verify_cleanup or collect bundle, mutation=false
[x] no Ready=True or released event appears while blocked
[x] repeated reconcile does not add duplicate finalizers or unbounded Events
```

Verification:

```text
TestOps/from-bundle delete-residue scenario
live CRD delete attempt if lab is available
```

## D5: Delete Release Gate

Goal: prove deletion completes only when cleanup evidence is clean.

Status: component-complete; QA/live validation pending.

Acceptance:

```text
[x] deleting SwBlockVolume with clean evidence removes finalizer
[ ] object deletion completes
[x] final status/event records release decision before deletion when possible
[x] repeated reconcile is idempotent
[ ] final cleanup verifier returns cleanup_status=ok
```

Verification:

```text
TestOps live delete clean scenario
cleanup verifier on m01/m02/tp01 if lab is healthy
```

## D6: Multi-Volume Isolation Gate

Goal: prove delete-safety for one volume does not affect unrelated volumes.

Status: pending.

Acceptance:

```text
[ ] deleting volume A does not change volume B/C status, finalizers, publish
      targets, or ManagedVolume identity
[ ] blocked delete on volume A does not block status publication for volume B/C
[ ] clean delete on volume A does not trigger cleanup or action on volume B/C
[ ] no cross-volume Events or reason-code mix-up
```

Verification:

```text
TestOps multi-volume delete-safety scenario
```

## D7: Close Gate

Goal: close Phase 39 only after the first mutating operator path is proven
bounded, idempotent, observable, and residue-safe.

Status: pending.

Acceptance:

```text
[ ] D1-D6 pass
[ ] RBAC grants only finalizer/status/event writes
[ ] no storage/workload/host mutation is introduced
[ ] QA validates blocked-delete, clean-delete, and multi-volume isolation gates
[ ] finished plan records non-claims and follow-ups
```

Verification:

```text
go test ./scripts
go test ./core/ops ./cmd/sw-block ./cmd/blockcsi
helm lint charts/seaweed-block
helm template sw-block charts/seaweed-block --namespace kube-system --include-crds \
  --set operatorStatus.create=true --set operatorStatus.dryRun=false
git diff --check
QA strict rerun from clean lab
```

## Current Progress

- 0%: Phase 39 opened. Scope is limited to `SwBlockVolume` finalizer/delete
  safety as the first mutating operator path. PVC finalizers, automatic cleanup,
  repair/rebuild/failback, backup/restore, and NVMe remain out of scope.
- 14%: D1 dev-complete. The delete-safety contract defines the finalizer name,
  owned mutation scope, required cleanup/identity facts, delete states,
  non-claims, and a pure decision function that blocks missing/residue evidence
  and marks clean evidence as releasable without performing mutation.
- 28%: D2 dev-complete. Bundle replay can carry
  `swblockvolume-delete-summary.txt` plus cleanup evidence into
  ManagedVolume delete-safety status. Residue/missing cleanup evidence projects
  blocked/rejected with `CleanupRequired=True`; clean evidence projects
  releasable/allowed without falsely blocking the volume. Summary, explain,
  operator-snapshot, dashboard JSON, and `SwBlockVolume.status.deleteSafety`
  use the same vocabulary.
- 42%: D3 dev-complete. Write-mode operator-status now has an optional
  finalizer client. It reads existing `SwBlockVolume` finalizers, patches only
  the `/finalizers` subresource, preserves unrelated finalizers, adds the
  Seaweed Block finalizer idempotently, and releases it only when
  `deleteSafety.finalizerReleaseAllowed=true`. RBAC is widened only to
  `swblockvolumes/finalizers`; no PVC/PV/workload/storage/host mutation is
  added.
- 56%: D4 component gate dev-complete. A blocked delete-safety decision
  (`decision=rejected`, `state=blocked`, cleanup residue reason) keeps/ensures
  the SwBlockVolume finalizer, writes blocked delete-safety status, emits no
  release event, and never calls the finalizer release path. Live delete-attempt
  validation remains for QA.
- 70%: D5 component gate complete. A releasable delete-safety decision
  (`decision=allowed`, `state=releasable`, `finalizerReleaseAllowed=true`)
  removes the SwBlockVolume finalizer, emits one release event, and is
  idempotent on repeated reconcile. Live object deletion completion and final
  cleanup verifier remain for QA.

## Prerequisites / Risks

- QA reported `tp01` as `NotReady`/unreachable during Phase 38 sign-off. Restore
  `tp01` before D6 multi-volume or any 3-node delete-safety gate.
- This phase must not become a cleanup executor. If residue exists, the correct
  behavior is to block deletion with evidence and a safe next step.
- Finalizer behavior must be idempotent; retries and repeated reconciles are
  expected.

## Next Step

Prepare the D4/D5 QA assignment for live blocked-delete and clean-delete
validation, including final cleanup verification and `tp01` lab-health caveat.
