# Phase 38 Finished Plan: Lifecycle Action Model Executable Contract

Status: closed on 2026-06-10.

Branch: `phase33-testops-failure-hardening`.

## Goal

Turn lifecycle/safe-next-step hints into executable, testable action contracts
before adding mutating operator behavior.

The phase deliberately stopped before finalizers, cleanup execution, promotion,
repair, rebuild, failback, backup, restore, or any new mutating RBAC. Its output
is a bounded decision layer: every surfaced action can explain whether it is
`allowed` or `rejected`, why, which facts it requires, which executor owns it,
which invariants/evidence apply, and whether mutation is allowed.

## Delivered

- Action contract inventory for current ManagedVolume actions, including owner
  executor, side-effect class, required facts, policy gate, invariant refs, and
  evidence requirements.
- Executable evaluator:
  `EvaluateManagedVolumeAction(actionType, ManagedVolumeFacts)`.
- Fail-closed rejection reasons:
  `unknown_action`, `policy_disabled`, `missing_required_facts`, and
  `mutation_not_allowed`.
- Rejected-action evidence renderer for cold review and QA.
- Surface propagation of action decisions through:
  - ManagedVolume projection text,
  - `summary.txt`,
  - `sw-block ops explain`,
  - `operator-snapshot.json`,
  - dashboard `/operator-snapshot.json`,
  - `SwBlockVolume.status.allowedActions[]`.
- CRD-safe camelCase fields for action status:
  `decision`, `decisionReason`, `missingFacts`, `mutationAllowed`, and
  `evidenceRequired`.
- Dry-run proof for `safe_k8s.reinstall_external_iscsi` on
  `publish_target_loopback_cross_node`: decision `allowed`, mode `dry_run`,
  evidence `loopback_cross_node_evidence`, and `mutation_allowed=false`.
- Rejected proof for `authority.request_promotion`: decision `rejected`,
  reason `policy_disabled`, side effect `authority_mutating`, and
  `mutation_allowed=false`.

## QA Evidence

QA verdict: PASS on source commit `ea1618d`.

Gates:

- G1 local contract tests: PASS.
- G2 rejected action evidence: PASS.
- G3 dry-run loopback action: PASS.
- G4 CRD/RBAC boundary: PASS.
- G5 non-mutation and cleanup: PASS.

Validated commands:

```text
go test ./core/ops ./cmd/sw-block ./cmd/blockcsi ./scripts
helm lint charts/seaweed-block
helm template ... --include-crds --set operatorStatus.create=true --set operatorStatus.dryRun=false
git diff --check
```

Sign-off document:

- `internal/docs/qa-assignments/phase38-action-model-executable-contract-qa-signoff.md`

## Non-Claims

- No finalizer/delete safety was added.
- No automatic cleanup was added.
- No image import, Helm release change, host repair, iSCSI/multipath cleanup, or
  hostPath mutation was added.
- No promotion, fencing, rebuild, reintegration, failback, repair, backup,
  snapshot, or restore execution was added.
- No dashboard mutation buttons or mutating operator executor were added.
- `authority.request_promotion` remains contract-only and policy-disabled; it is
  intentionally not surfaced as an executable live action.

## Follow-Ups

- Restore lab node `tp01` before Phase 39 multi-node delete-safety scenarios;
  QA observed it as `NotReady`/unreachable during Phase 38 sign-off.
- Add server-side-dry-run/envtest coverage for CRD status payloads so future
  schema drift is caught before live QA.
- Begin Phase 39: safe finalizer/delete-safety as the first narrow mutating
  operator path, using the Phase 38 action contract as the precondition layer.
