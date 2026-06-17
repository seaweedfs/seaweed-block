# QA Sign-off - Phase 38 Lifecycle Action Model Executable Contract

Verdict: **PASS (G1-G5).** Surfaced lifecycle actions are executable contracts,
not loose text: every action carries a typed contract evaluated to `allowed` or
`rejected` with stable reasons, dry-run actions are allowed without mutation,
disabled/mutating/underspecified actions fail closed, the CRD uses camelCase
decision fields, and the operator holds no mutation power. No storage, workload,
Helm, iSCSI, multipath, hostPath, promotion, repair, rebuild, failback, delete,
backup, or restore mutation occurred.

Date: 2026-06-09

Source commit: `ea1618d` (floor met: `7d72674 add action evaluation contract`,
`f6e8d19 surface action evaluation decisions`, `e4f49fc add rejected action
evidence gate`, `2ad2447 add dry-run action evidence gate`; branch
`phase33-testops-failure-hardening`)

Environment: k3s `v1.34.4+k3s1`; from-bundle replays + a live write-mode install
(operator-status pinned to m02). Note: tp01 was `NotReady`/unreachable during
this run — see non-blocking findings; all gates were validated on m02/m01.

## G1 — Local Contract Tests — PASS

```text
go test ./core/ops ./cmd/sw-block ./cmd/blockcsi ./scripts  -> all ok
helm lint charts/seaweed-block                              -> 1 linted, 0 failed
helm template ... --include-crds --set operatorStatus.create=true --set operatorStatus.dryRun=false
                                                            -> exit 0, 979 lines
git diff --check (local repo)                               -> clean (no whitespace errors)
```

## G2 — Rejected Action Evidence — PASS (evaluator tests)

The evaluator suite (`core/ops/managed_volume_action_evaluator_test.go`) passes,
asserting the exact contract:

```text
TestEvaluateManagedVolumeAction_RejectsDisabledAuthorityMutation:
  authority.request_promotion -> decision=rejected reason=policy_disabled
  (mode=dry_run, side_effect_class=authority_mutating,
   owner_executor=authority_recovery_executor, mutation_allowed=false,
   evidence_required=promotion_readiness_evidence)
TestEvaluateManagedVolumeAction_RejectsMissingRequiredFacts:
  -> decision=rejected reason=missing_required_facts missing_facts=[placement.replica_node]
TestEvaluateManagedVolumeAction_RejectsUnknownAction -> rejected/unknown_action
TestManagedVolumeActionEvaluator_CoversEveryContractEntry -> every contract entry evaluated
TestRenderManagedVolumeActionEvaluationText_ShowsRejectedAction ->
  "managed_volume_action_evaluation authority.request_promotion decision=rejected
   side_effect=authority_mutating executor=authority_recovery_executor reason=policy_disabled"
  "managed_volume_action_evaluation_evidence_required authority.request_promotion promotion_readiness_evidence"
```

Pass criteria met: the rejected action is **visible as rejected** (rendered, not
silently omitted); `authority.request_promotion` has `MutationAllowed=false` in
the contract and the evaluator returns `mutation_allowed=false` (no rejected
action is `true`); the evaluator is a pure decision function — **no executor path
is invoked**. The contract is fail-closed (unknown/disabled/mutating/missing-facts
all rejected before any executor is considered).

This is the assignment's sanctioned "evaluator tests" path: `request_promotion`
is a contract entry (`managed_volume_contract.go:548`, `PolicyGate=disabled`) and
is intentionally **not** attached to any live volume state by
`managedVolumeActionsForProjection` (its switch covers only loopback /
pvc-unbound / writer-mount / image-pull / hostpath), so it is reachable only
through the evaluator/contract — by design, as a future bounded executor's action
held closed by policy.

## G3 — Dry-Run Loopback Action — PASS

Bundle: `cluster-evidence.json` + `unsupported-cross-node-loopback-attach.txt`
(`volume_id=pvc-d38`). `sw-block ops report/explain/dashboard --from-bundle`:

```text
managed_volume=pvc-d38 status=blocked reason=publish_target_loopback_cross_node
managed_volume_action=safe_k8s.reinstall_external_iscsi mode=dry_run side_effect=safe_k8s
  executor=installer_or_operator decision=allowed
managed_volume_action_evidence_required=safe_k8s.reinstall_external_iscsi loopback_cross_node_evidence
operator-snapshot.json: "decision": "allowed", "mutation_allowed": false,
                        "evidence_required": "loopback_cross_node_evidence"
dashboard /operator-snapshot.json: HTTP 200, same fields
no surface claims the action was executed (grep "was executed|reinstalled|action ran" = 0)
```

All G3 pass criteria met. The dry-run action is allowed without mutation, with
its required evidence named, and nothing claims execution.

## G4 — CRD And RBAC Boundary — PASS

Live `auth can-i` (operator-status SA):

```text
patch swblockvolumes --subresource=status: yes
create events: yes
patch pods: no
patch persistentvolumeclaims (default): no
update storageclasses.storage.k8s.io: no
```

CRD `allowedActions[]` camelCase — both **rendered** (helm template) and **live**
(after a loopback reconcile):

```text
rendered CRD schema: decision, decisionReason, missingFacts, mutationAllowed,
                     evidenceRequired all present; snake_case mutation_allowed: 0
live SwBlockVolume.status.allowedActions[safe_k8s.reinstall_external_iscsi]:
  {"decision":"allowed","mutationAllowed":false,
   "evidenceRequired":"loopback_cross_node_evidence","mode":"dry_run",
   "ownerExecutor":"installer_or_operator","sideEffectClass":"safe_k8s", ...}
```

Status patch + events only; no pod/PVC/storageclass mutation. All decision fields
are camelCase; no snake_case `mutation_allowed` in CRD status. PASS.

## G5 — Non-Mutation And Cleanup — PASS

- The operator-status SA cannot create/patch/delete pods, PVC/PV, deployments, or
  storageclasses (G4 can-i), and only patches CRD `/status` + creates Events.
- The action evaluator is a pure decision function — it surfaces decisions and
  never executes; no Helm/image/import/cleanup command is run by the operator,
  and no surface claims execution (G3).
- The replay gates are read-only from-bundle; no iSCSI/multipath/dmsetup/process/
  hostPath mutation.
- Final cleanup verifier: `cleanup_status=ok`, all residue counters 0; helm 0,
  pods 0.

## Blocking Findings

None. The action-model executable contract holds end to end.

## Non-Blocking Findings

1. **Lab infra: tp01 is `NotReady`/unreachable.** During this run the image build
   failed importing to `192.168.1.188` with "No route to host", and
   `kubectl get nodes` shows tp01 `NotReady` (m01/m02 `Ready`). I cannot reach tp01
   to recover it (no SSH route). This is a lab-infrastructure issue, not a Phase
   38 defect — all gates were validated on m02/m01. Flag for the lab admin to
   bring tp01 back (and re-run any multi-node scenario that needs 3 nodes).
2. **`authority.request_promotion` is contract-only (intentional, worth noting).**
   It is a defined contract entry but is not surfaced on any live volume state, so
   it is exercised only by the evaluator (G2). This is the correct fail-closed
   posture for a future executor's action; just noting it so a later reviewer does
   not expect it on a live `SwBlockVolume`.

## Release / Next-Phase Recommendation

- **Phase 38 can close.** Lifecycle actions are now typed, evaluated
  (allowed/rejected with stable reasons), dry-run-without-mutation, fail-closed,
  and surfaced with camelCase CRD vocabulary aligned across report, explain,
  dashboard, operator-snapshot, and CRD status — with no mutation power added.
- Proceed to **Phase 39 (finalizer/delete safety)** as planned. Recommend the lab
  admin restore tp01 before any Phase 39 multi-node delete-safety scenarios.
