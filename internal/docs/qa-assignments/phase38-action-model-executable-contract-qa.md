# Phase 38 QA Assignment: Lifecycle Action Model Executable Contract

Status: ready for QA.

Source branch: `phase33-testops-failure-hardening`.

Required source floor:

- `7d72674 phase38: add action evaluation contract`
- `f6e8d19 phase38: surface action evaluation decisions`
- `e4f49fc phase38: add rejected action evidence gate`
- `2ad2447 phase38: add dry-run action evidence gate`

## Goal

Validate that surfaced lifecycle actions are executable contracts, not loose
text hints.

Phase 38 must prove:

- every action has a typed contract,
- action decisions are `allowed` or `rejected` with stable reasons,
- dry-run actions can be allowed without mutation,
- unsafe or underspecified actions fail closed,
- report, explain, dashboard, operator-snapshot, and CRD status vocabulary stay
  aligned.

No storage, Kubernetes workload, Helm, iSCSI, multipath, hostPath, promotion,
repair, rebuild, failback, delete, backup, or restore mutation is allowed in
this phase.

## G1: Local Contract Tests

Run:

```bash
go test ./core/ops ./cmd/sw-block ./cmd/blockcsi ./scripts
helm lint charts/seaweed-block
helm template sw-block charts/seaweed-block --namespace kube-system --include-crds \
  --set operatorStatus.create=true --set operatorStatus.dryRun=false >/tmp/sw-block-phase38-render.yaml
git diff --check
```

Pass criteria:

- all Go tests pass,
- Helm lint passes,
- Helm template renders with operator-status write mode enabled,
- `git diff --check` has no whitespace errors beyond line-ending warnings.

## G2: Rejected Action Evidence

Use the evaluator tests or a focused component run to verify
`authority.request_promotion`.

Expected:

```text
action_type=authority.request_promotion
decision=rejected
reason=policy_disabled
mode=dry_run
side_effect_class=authority_mutating
owner_executor=authority_recovery_executor
mutation_allowed=false
evidence_required=promotion_readiness_evidence
```

Also verify an action with missing facts returns:

```text
decision=rejected
reason=missing_required_facts
missing_facts=<stable comma-separated facts>
```

Pass criteria:

- rejected actions are visible as rejected, not silently omitted,
- no rejected action has `mutation_allowed=true`,
- no executor path is invoked.

## G3: Dry-Run Action Evidence

Use the loopback cross-node bundle path. A synthetic bundle is acceptable if it
contains:

```text
cluster-evidence.json
unsupported-cross-node-loopback-attach.txt
```

Minimum unsupported attach artifact:

```text
issue=unsupported_cross_node_loopback_attach
app_node=m02
blockvolume_node=m01
frontend=127.0.0.1:3260
volume_id=<volume-id>
replica_id=r1
reason=loopback frontend requires app pod and blockvolume on the same node
```

Run:

```bash
sw-block ops report --from-bundle <bundle> --out /tmp/sw-block-phase38-report
sw-block ops explain --from-bundle <bundle>
sw-block ops dashboard --from-bundle <bundle> --listen 127.0.0.1:9334
```

Pass criteria:

- status is `blocked` with reason `publish_target_loopback_cross_node`,
- surfaced action is `safe_k8s.reinstall_external_iscsi`,
- action has `mode=dry_run`, `side_effect=safe_k8s`,
  `executor=installer_or_operator`, `decision=allowed`,
- `evidence_required=loopback_cross_node_evidence`,
- operator-snapshot has `"decision": "allowed"`,
  `"mutation_allowed": false`, and
  `"evidence_required": "loopback_cross_node_evidence"`,
- dashboard `/operator-snapshot.json` returns HTTP 200 and the same fields,
- no surface claims that the action was executed.

## G4: CRD And RBAC Boundary

If a live k3s lab is available, install with operator-status enabled and check
the ServiceAccount boundary:

```bash
kubectl auth can-i patch swblockvolumes --subresource=status \
  --as system:serviceaccount:kube-system:sw-block-operator-status -n kube-system
kubectl auth can-i create events \
  --as system:serviceaccount:kube-system:sw-block-operator-status -n kube-system
kubectl auth can-i patch pods \
  --as system:serviceaccount:kube-system:sw-block-operator-status -n kube-system
kubectl auth can-i patch pvc \
  --as system:serviceaccount:kube-system:sw-block-operator-status -n default
kubectl auth can-i update storageclasses \
  --as system:serviceaccount:kube-system:sw-block-operator-status
```

Expected:

- status patch and event create: `yes`,
- pods/PVC/storageclasses: `no`.

Also inspect a rendered or live `SwBlockVolume.status.allowedActions[]` payload.
Expected camelCase fields:

```text
decision
decisionReason
missingFacts
mutationAllowed
evidenceRequired
```

Snake-case fields such as `mutation_allowed` must not appear in CRD status.

## G5: Non-Mutation And Cleanup

Confirm Phase 38 did not add mutating behavior.

Pass criteria:

- no Helm release/image/import/cleanup command is executed by the operator,
- no PVC/PV/Deployment/Pod/StorageClass is created, patched, or deleted by the
  action evaluator,
- no iSCSI, multipath, dmsetup, process, or hostPath mutation occurs from the
  replay gates,
- final cleanup verifier returns `cleanup_status=ok` when live gates are run.

## Report

Write the sign-off to:

```text
internal/docs/qa-assignments/phase38-action-model-executable-contract-qa-signoff.md
```

Required verdict fields:

- source commit,
- G1 local contract result,
- G2 rejected action result,
- G3 dry-run loopback action result,
- G4 CRD/RBAC result or explicit reason skipped,
- G5 non-mutation/cleanup result,
- blocking findings,
- non-blocking findings,
- release/next-phase recommendation.
