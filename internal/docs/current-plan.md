# Current Plan: Phase 39 - Finalizer / Delete Safety

Status: design-blocked, 80% complete. Started on 2026-06-10.

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
| finalizer mutation design decision | dashboard mutation buttons |
| TestOps delete-safety gates | broad production delete lifecycle |

Allowed implementation rule:

```text
Phase 39 may patch SwBlockVolume/status and Kubernetes Events.

Finalizer mutation is currently design-blocked. Kubernetes CRDs do not expose a
usable HTTP `/finalizers` endpoint, and patching `metadata.finalizers` on the
main CRD object requires main `patch swblockvolumes` RBAC. The original target
of "RBAC narrowed to finalizers + status + events" is therefore not viable for
CRD finalizers.

Phase 39 must not delete PVC/PV/Pods/Deployments/StorageClasses, run cleanup
scripts, change Helm releases, import images, touch iSCSI/multipath/dmsetup,
remove hostPath data, promote/fence/rebuild/failback, or mutate storage.
```

## D1: Delete-Safety Contract Review

Goal: define exactly what the finalizer owns and which facts are required before
it can release deletion.

Status: design-blocked by live QA.

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
[ ] operator can patch SwBlockVolume metadata.finalizers under an approved
      boundary model
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

Status: blocked by finalizer mutation design.

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

Status: blocked by finalizer mutation design.

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
[ ] approved finalizer mutation boundary is implemented and proven live
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
  `metadata.finalizers`, preserves unrelated finalizers, adds the Seaweed Block
  finalizer idempotently, and releases it only when
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
- 78%: D4/D5 live QA handoff ready. The QA assignment covers RBAC,
  blocked-delete finalizer hold, clean-delete finalizer release, object deletion
  completion, final cleanup verification, and the `tp01` lab-health caveat.
- 80%: D4/D5 live QA found the first bug: the client patched a nonexistent
  `/finalizers` URL for CRDs. The fix keeps RBAC scoped to
  `swblockvolumes/finalizers` but sends the merge patch to the main
  SwBlockVolume resource URL with a body containing only
  `metadata.finalizers`. Awaiting QA re-validation.
- 80% blocked: QA re-validation of `b371e2e` proved the deeper issue. The
  corrected main-object patch is rejected with HTTP 403 because Kubernetes
  authorizes it as main `patch swblockvolumes`; the
  `swblockvolumes/finalizers` grant cannot authorize CRD finalizer mutation.
  D4/D5 stay blocked until we choose a new boundary model.

## Prerequisites / Risks

- QA reported `tp01` as `NotReady`/unreachable during Phase 38 sign-off. Restore
  `tp01` before D6 multi-volume or any 3-node delete-safety gate.
- This phase must not become a cleanup executor. If residue exists, the correct
  behavior is to block deletion with evidence and a safe next step.
- Finalizer behavior must be idempotent; retries and repeated reconciles are
  expected.
- Kubernetes CRDs do not expose a usable HTTP `/finalizers` subresource. A
  finalizer patch must use the main object URL and therefore requires main
  `patch swblockvolumes` authorization. This invalidates the original
  RBAC-only boundary assumption.
- Do not broaden operator-status RBAC to main `patch swblockvolumes` as a
  local fix. That would make the safety boundary code-enforced only. If Phase
  39 continues with operator-owned CRD finalizers, pair main patch RBAC with an
  admission boundary and a live/envtest regression. Otherwise defer finalizer
  mutation to the component that owns the `SwBlockVolume` lifecycle.

## Design Decision Required

Choose one before resuming D4/D5:

1. **Admission-bounded operator finalizer.** Grant operator-status main
   `patch/update swblockvolumes`, then add a ValidatingAdmissionPolicy or
   webhook that rejects any operator-status write touching `.spec` or metadata
   outside `metadata.finalizers`. This is the quickest way to finish Phase 39,
   but the boundary moves from RBAC-only to admission + tests.
2. **Lifecycle-owner finalizer.** Keep operator-status status/events-only and
   move finalizer add/remove to the component that owns `SwBlockVolume` object
   creation and lifecycle. This preserves the read-only operator-status model,
   but it is a larger ownership change because `SwBlockVolume` objects are not
   yet automatically created by CSI.
3. **Code-only main patch.** Grant main patch and rely only on the controller
   implementation to avoid spec writes. This is not recommended.

## Next Step

Stop D6. Decide between admission-bounded operator finalizers and
lifecycle-owner finalizers. After that, update RBAC/tests/QA gates and re-run
D4/D5 live against the real API and real ServiceAccount.
