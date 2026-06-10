# Current Plan: Phase 38 - Lifecycle Action Model Executable Contract

Status: active, 18% complete. Started on 2026-06-09.

Branch: `phase33-testops-failure-hardening`

Previous phase: Phase 37 is closed in
`internal/docs/finished-plans/phase37_finishedplan_live_node_evidence_hardening.md`.

## Product Goal

Turn safe next-step/action hints into an executable contract before adding
mutating operator behavior.

Phase 35-37 made status trustworthy and visible. Phase 38 makes action
eligibility explicit: a future executor must be able to explain why an action is
allowed as read-only/dry-run, rejected for missing facts, or blocked because it
would mutate storage/authority without a product gate.

The hard exit statement:

```text
Every surfaced action has a typed contract, required facts, policy gate,
executor, invariant/evidence refs, and a testable allow/reject decision. No
operator mutation is added in this phase.
```

## Scope Contract

| In | Out |
|---|---|
| action registry / typed contracts | finalizers/delete safety |
| executable allow/reject evaluator | automatic cleanup |
| dry-run action proof | repair/rebuild/failback |
| rejected-action proof | backup/snapshot/restore |
| CRD/report/dashboard/operator-snapshot agreement | NVMe ANA parity |
| TestOps or unit/component action gates | new mutating RBAC |
| QA assignment for strict action validation | dashboard mutation buttons |

Allowed implementation rule:

```text
Phase 38 may evaluate actions and emit read-only/dry-run/scripted status.

Phase 38 must not patch spec, delete objects, run cleanup, import images,
change Helm releases, promote/fence/rebuild/failback, or mutate storage.
```

## D1: Action Contract Inventory

Goal: pin the current action vocabulary and identify which actions are
read-only, dry-run, scripted, disabled, or future-mutating.

Status: complete.

Acceptance:

```text
[x] every action type has owner executor
[x] every action type has side-effect class
[x] every action type has required facts
[x] every non-observe action has policy gate
[x] no action has mutation_allowed=true
```

Verification:

```text
go test ./core/ops
```

## D2: Executable Allow/Reject Evaluator

Goal: add a code-level evaluator that turns action contracts plus current facts
into an allow/reject decision without performing side effects.

Status: dev-complete; QA/internal review pending.

Acceptance:

```text
[x] dry-run action with required facts returns allowed
[x] missing required facts returns rejected with missing_facts
[x] disabled authority mutation returns rejected
[x] unknown action returns rejected
[x] evaluator output includes mode, side-effect class, executor,
      invariant refs, and evidence requirement
```

Verification:

```text
go test ./core/ops
```

## D3: Surface Agreement For Action Decisions

Goal: ensure report, operator-snapshot, CRD status, and dashboard do not diverge
on the action contract.

Status: pending.

Acceptance:

```text
[ ] operator-snapshot exposes the same action fields as the evaluator contract
[ ] SwBlockVolume.status.allowedActions remains camelCase and schema-valid
[ ] blocked path still has dry-run/read-only actions only
[ ] no surface shows mutationAllowed=true
[ ] action reasons/evidence refs survive from-bundle replay
```

Verification:

```text
go test ./core/ops ./cmd/sw-block
server-side dry-run or live CRD status patch check
```

## D4: Rejected-Action Gate

Goal: prove unsafe or underspecified actions fail closed before any executor can
run.

Status: pending.

Acceptance:

```text
[ ] authority.request_promotion is rejected under current policy
[ ] action with missing required facts is rejected with a stable reason
[ ] rejected action is visible as not executable, not silently omitted
[ ] Kubernetes RBAC remains status/events/read-only only
```

Verification:

```text
go test ./core/ops
TestOps/QA replay gate for rejected action evidence
```

## D5: Dry-Run Action Gate

Goal: prove one non-mutating dry-run action is executable as a dry-run decision
and still does not mutate the cluster.

Status: pending.

Acceptance:

```text
[ ] safe_k8s.reinstall_external_iscsi can evaluate allowed in dry-run mode
[ ] evaluation names preconditions, executor, invariant refs, evidence refs
[ ] no Helm release, Deployment, PVC/PV, image, iSCSI, multipath, or hostPath
      mutation occurs
[ ] report/dashboard/operator-snapshot agree on the dry-run boundary
```

Verification:

```text
go test ./core/ops ./cmd/sw-block
TestOps loopback-cross-node dry-run action replay
```

## D6: Close Gate

Goal: close the action model as a real product foundation for Phase 39
finalizer/delete safety.

Status: pending.

Acceptance:

```text
[ ] D1-D5 pass
[ ] no new mutating RBAC is introduced
[ ] finished plan records non-claims and follow-ups
[ ] QA validates dry-run and rejected-action evidence
```

Verification:

```text
go test ./scripts
go test ./core/ops ./cmd/sw-block ./cmd/blockcsi
helm lint charts/seaweed-block
helm template sw-block charts/seaweed-block --namespace kube-system --include-crds \
  --set operatorStatus.create=true --set operatorStatus.dryRun=false
git diff --check
QA strict rerun or replay from clean bundle
```

## Current Progress

- 0%: Phase 38 opened. Scope is action eligibility only; no operator mutation.
- 18%: D1/D2 dev-complete. `core/ops` now has an executable
  `EvaluateManagedVolumeAction` contract that allows dry-run actions only when
  required facts exist, rejects missing facts with `missing_required_facts`,
  rejects disabled authority mutation with `policy_disabled`, and rejects
  unknown actions. Unit tests cover allowed dry-run, missing facts, disabled
  authority mutation, and unknown action rejection.

## Next Step

Wire the evaluator into one user-visible surface without making actions
mutating. Recommended next slice: expose action evaluation decisions in
operator-snapshot/report for the existing cross-node loopback dry-run action,
then add a rejected-action replay gate for `authority.request_promotion`.
