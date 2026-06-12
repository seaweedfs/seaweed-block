# Current Plan: Phase 39 - Finalizer / Delete Safety

Status: active, 86% complete. Started on 2026-06-10.

Branch: `phase33-testops-failure-hardening`

Previous phase: Phase 38 is closed in
`internal/docs/finished-plans/phase38_finishedplan_lifecycle_action_model_executable_contract.md`.

## Product Goal

Close the delete-safety status boundary while preserving the operator-status
read-only posture.

Phases 35-38 established read-only CRD status, Events, live node evidence,
support/cleanup visibility, and executable action decisions. Phase 39 validates
that model against deletion evidence, but live QA proved `SwBlockVolume`
finalizer mutation cannot be made RBAC-bounded for CRDs without granting main
`patch swblockvolumes`. Phase 39 therefore keeps operator-status limited to
status/events and moves finalizer mutation to a future lifecycle-owner phase.

The hard exit statement:

```text
Deleting a managed SwBlockVolume is not yet automatically protected by this
controller. The product reports whether deletion is safe or blocked with
explicit status, reason, evidence, and a non-mutating next step. The
operator-status controller mutates only CRD status and Kubernetes Events in this
phase.
```

## Scope Contract

| In | Out |
|---|---|
| SwBlockVolume delete-safety contract | PVC finalizer ownership |
| delete-requested status projection | automatic cleanup execution |
| status/events-only operator boundary | iSCSI/multipath/hostPath deletion by operator |
| idempotent reconcile and retry behavior | promotion/fencing/rebuild/failback |
| cleanup verifier evidence consumption | backup/snapshot/restore |
| Kubernetes Events for delete-safety decisions | NVMe ANA parity |
| lifecycle-owner finalizer follow-up | dashboard mutation buttons |
| TestOps delete-safety gates | broad production delete lifecycle |

Allowed implementation rule:

```text
Phase 39 may patch SwBlockVolume/status and Kubernetes Events.

Phase 39 must not delete PVC/PV/Pods/Deployments/StorageClasses, run cleanup
scripts, change Helm releases, import images, touch iSCSI/multipath/dmsetup,
remove hostPath data, promote/fence/rebuild/failback, mutate storage, or patch
SwBlockVolume metadata/finalizers.
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

## D3: Status-Only Delete-Safety Boundary

Goal: keep delete-safety observable while proving operator-status has no
finalizer, storage, or workload mutation power.

Status: dev-complete; QA/internal review pending.

Acceptance:

```text
[x] operator cannot patch SwBlockVolume metadata.finalizers
[x] operator cannot patch spec, PVC/PV, pods, deployments, storageclasses,
      secrets, nodes, iSCSI, multipath, hostPath, or Helm resources
[x] delete-safety status is still written for blocked/releasable decisions
[x] finalizer add/remove is deferred to a future lifecycle owner
[x] delete-safety decisions emit status and Events only
```

Verification:

```text
go test ./core/ops ./cmd/sw-block
helm template/lint with updated RBAC
kubectl auth can-i boundary sweep: status/events yes, finalizers/spec/workloads no
```

## D4: Delete-Safety Blocked Status Gate

Goal: prove residue or insufficient evidence becomes blocked delete-safety
status with non-mutating next steps.

Status: QA PASS on `f167f9a`.

Acceptance:

```text
[x] delete evidence with active/residue facts projects blocked status
[x] status shows blocked or cleanup_required with stable reason
[x] safe next step is observe.verify_cleanup or collect bundle, mutation=false
[x] no Ready=True or release/executed claim appears while blocked
[x] repeated reconcile does not emit unbounded Events
```

Verification:

```text
TestOps/from-bundle delete-residue scenario
live CRD status/event check if lab is available
```

## D5: Delete-Safety Releasable Status Gate

Goal: prove clean evidence becomes releasable delete-safety status without
claiming that operator-status removes finalizers or completes deletion.

Status: QA PASS on status-only path.

Acceptance:

```text
[x] clean evidence projects releasable status
[ ] object deletion is not claimed by operator-status
[x] final status/event records releasable decision when evidence allows it
[x] repeated reconcile is idempotent
[ ] final cleanup verifier returns cleanup_status=ok
```

Verification:

```text
TestOps live delete-safety clean scenario
cleanup verifier on m01/m02/tp01 if lab is healthy
```

## D6: Multi-Volume Isolation Gate

Goal: prove delete-safety for one volume does not affect unrelated volumes.

Status: ready for QA; restore `tp01` first for 3-node validation.

Acceptance:

```text
[ ] delete-safety evidence for volume A does not change volume B/C status,
      targets, or ManagedVolume identity
[ ] blocked delete on volume A does not block status publication for volume B/C
[ ] clean delete evidence on volume A does not trigger cleanup or action on
      volume B/C
[ ] no cross-volume Events or reason-code mix-up
```

Verification:

```text
internal/docs/qa-assignments/phase39-d6-multi-volume-delete-safety-status-isolation-qa.md
```

## D7: Close Gate

Goal: close Phase 39 only after the delete-safety status path is proven
bounded, idempotent, observable, and residue-safe without widening
operator-status mutation power.

Status: pending.

Acceptance:

```text
[ ] D1-D6 pass
[ ] operator-status remains status/events-only
[ ] no storage/workload/host mutation is introduced
[ ] QA validates blocked delete-safety, clean delete-safety, and multi-volume
      isolation gates
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
- 42%: D3 first implementation attempted an optional finalizer client in
  operator-status. Live QA later proved that boundary was not viable for CRDs.
- 56%: D4 component gate projected blocked delete-safety status and safe next
  steps from residue evidence. The finalizer-hold part of the gate is now
  deferred to a future lifecycle owner.
- 70%: D5 component gate projected releasable delete-safety status from clean
  evidence. The finalizer-release part of the gate is now deferred to a future
  lifecycle owner.
- 78%: D4/D5 live QA handoff ready under the original finalizer design. That
  assignment is superseded by the status-only assignment after the
  lifecycle-owner pivot.
- 80%: D4/D5 live QA found the first bug: the client patched a nonexistent
  `/finalizers` URL for CRDs. The fix keeps RBAC scoped to
  `swblockvolumes/finalizers` but sends the merge patch to the main
  SwBlockVolume resource URL with a body containing only
  `metadata.finalizers`. Follow-up QA then exposed the deeper 403 boundary
  issue below.
- 80% blocked: QA re-validation of `b371e2e` proved the deeper issue. The
  corrected main-object patch is rejected with HTTP 403 because Kubernetes
  authorizes it as main `patch swblockvolumes`; the
  `swblockvolumes/finalizers` grant cannot authorize CRD finalizer mutation.
  D4/D5 stay blocked until we choose a new boundary model.
- 86%: Design decision made. Preserve the operator-status read-only/status-only
  model and do not grant main `patch swblockvolumes`. The operator-status
  finalizer executor and `swblockvolumes/finalizers` RBAC are removed. Phase 39
  will close, if QA agrees, as delete-safety status/events only; actual
  finalizer ownership moves to a future lifecycle-owner phase.
- 88%: D4/D5 status-only QA on `4a51bae` passed RBAC and D5, but D4 exposed a
  CRD schema enum gap: `SwBlockVolume.status.allowedActions[].mode` rejected
  `scripted` even though blocked delete-safety emits `observe.verify_cleanup
  mode=scripted`. Added `scripted` to the volume allowedActions mode enum and a
  schema regression test. D4 live rerun is pending.
- 92%: D4 re-run on `f167f9a` passed. The blocked path now writes
  `deleteSafety.state=blocked`, `decision=rejected`,
  `CleanupRequired=True`, and `observe.verify_cleanup mode=scripted
  mutationAllowed=false` with `finalizer_patches=0`. D4 and D5 both pass on the
  status-only path. Proceed to D6 multi-volume status isolation after `tp01` is
  restored if the gate exercises three nodes.

## Prerequisites / Risks

- QA reported `tp01` as `NotReady`/unreachable during Phase 38 sign-off. Restore
  `tp01` before D6 multi-volume or any 3-node delete-safety gate.
- This phase must not become a cleanup executor. If residue exists, the correct
  behavior is to block deletion with evidence and a safe next step.
- Delete-safety status behavior must be idempotent; retries and repeated
  reconciles are expected.
- Kubernetes CRDs do not expose a usable HTTP `/finalizers` subresource. A
  finalizer patch must use the main object URL and therefore requires main
  `patch swblockvolumes` authorization. This invalidates the original
  RBAC-only boundary assumption.
- Do not broaden operator-status RBAC to main `patch swblockvolumes` as a local
  fix. That would make the safety boundary code-enforced only. Finalizer
  mutation is deferred to the component that owns the `SwBlockVolume` lifecycle.

## Design Decision

Chosen path:

Keep operator-status status/events-only. Move finalizer add/remove to a future
lifecycle owner that also owns `SwBlockVolume` object creation and deletion.
This preserves the Phase 35-38 safety boundary and avoids granting
operator-status main `patch swblockvolumes`.

Rejected paths:

1. Admission-bounded operator finalizer: viable later, but adds admission
   policy/webhook complexity to the alpha path.
2. Code-only main patch: not acceptable for this control-plane boundary.

## Next Step

Run D6 with
`internal/docs/qa-assignments/phase39-d6-multi-volume-delete-safety-status-isolation-qa.md`.
Restore `tp01` first if D6 uses the 3-node RF=3 lab.
