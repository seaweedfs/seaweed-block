# Phase 37 D4/D5 QA Assignment: Host Prereq And Loopback Blockers

Status: ready for QA.

Source branch: `phase33-testops-failure-hardening`.

Required source floor:

- `9e5a4ea phase37: carry host prereq evidence in bundles`
- `c32e07a phase37: project loopback cross-node evidence`

## Goal

Validate the two remaining Phase 37 node-evidence blockers without widening the
operator boundary:

- D4: iSCSI/multipath prerequisite evidence from read-only artifacts.
- D5: loopback publish target is allowed only for same-node/local consumers and
  becomes visible as `publish_target_loopback_cross_node` for cross-node
  consumers.

The operator must not probe hosts with privileged commands and must not repair,
reinstall, cleanup, mutate PVC/PV, or change image state.

## D4: Host Prereq Replay Gate

Use a synthetic or collected support/failure bundle containing:

```text
host/host-prereq-summary.txt
```

Minimum artifact examples:

```text
node=m02 iscsi_prereq=missing multipath_prereq=ok command_iscsiadm=missing read_only=true
node=tp01 iscsi_prereq=ok multipath_prereq=missing command_multipath=missing command_dmsetup=present read_only=true
```

Run:

```bash
sw-block ops report --from-bundle <bundle> --out /tmp/sw-block-report
sw-block ops dashboard --from-bundle <bundle> --listen 127.0.0.1:9334
sw-block ops explain --from-bundle <bundle>
```

Pass criteria:

- `summary.txt` has node lines with `reason=iscsi_prereq_missing` and
  `reason=multipath_prereq_missing`.
- `operator-snapshot.json` has the same node statuses and reasons.
- Dashboard `/operator-snapshot.json` returns HTTP 200 and the same reasons.
- Safe next steps mention read-only/scripted evidence collection or
  verification, with `mutation_allowed=false`.
- No status surface suggests automatic host repair.

Optional live smoke:

- Run `scripts/collect-helm-support-bundle.sh` or
  `scripts/collect-k8s-failure-snapshot.sh`.
- Confirm the produced bundle includes `host/host-prereq-summary.txt`.
- Replay that bundle with `sw-block ops report --from-bundle`.

## D5: Loopback Cross-Node Gate

Run both existing TestOps scenarios from a clean lab:

```text
testops/scenarios/same-node-alpha-attach-chain.yaml
testops/scenarios/same-node-alpha-attach-negative-chain.yaml
```

Pass criteria for same-node:

- Writer and reader verify successfully.
- Loopback frontend remains accepted when app pod and blockvolume are on the
  same node.
- No `publish_target_loopback_cross_node` appears in the healthy report.

Pass criteria for cross-node negative:

- Scenario exits through the expected unsupported-placement path.
- `unsupported-cross-node-loopback-attach.txt` exists and names:
  `issue=unsupported_cross_node_loopback_attach`, `app_node`,
  `blockvolume_node`, `frontend=127.0.0.1:*`, and `volume_id`.
- Replaying the result bundle with `sw-block ops report --from-bundle` produces
  `managed_volume ... status=blocked reason=publish_target_loopback_cross_node`.
- `operator-snapshot.json`, dashboard `/operator-snapshot.json`, and
  `ops explain --from-bundle` agree on
  `publish_target_loopback_cross_node`.
- No surface shows `Ready=True` for the cross-node loopback case.
- The suggested action is dry-run/read-only, not an executed reinstall.

## Boundary Checks

Confirm the operator-status ServiceAccount still has no mutation power:

```bash
kubectl auth can-i patch swblockvolumes --subresource=status --as system:serviceaccount:kube-system:sw-block-operator-status -n kube-system
kubectl auth can-i create events --as system:serviceaccount:kube-system:sw-block-operator-status -n kube-system
kubectl auth can-i patch pods --as system:serviceaccount:kube-system:sw-block-operator-status -n kube-system
kubectl auth can-i patch pvc --as system:serviceaccount:kube-system:sw-block-operator-status -n default
kubectl auth can-i update storageclasses --as system:serviceaccount:kube-system:sw-block-operator-status
```

Expected:

- status patch and events: `yes`
- pods/PVC/storageclasses: `no`

## Report

Write the sign-off to:

```text
internal/docs/qa-assignments/phase37-d4-d5-host-prereq-loopback-qa-signoff.md
```

Required verdict fields:

- source commit
- D4 replay result
- D4 optional live smoke result, or explicit reason skipped
- D5 same-node result
- D5 cross-node negative result
- boundary result
- final cleanup audit
- blocking findings
- non-blocking findings
