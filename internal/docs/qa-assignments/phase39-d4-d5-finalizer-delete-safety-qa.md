# Phase 39 D4/D5 QA Assignment: Delete-Safety Status Boundary

Status: ready for QA after the lifecycle-owner pivot.

Required source floor:

- `1630de2 phase39: keep delete safety status-only`

Live QA on `b371e2e` proved CRD finalizer mutation cannot be bounded by
`swblockvolumes/finalizers` RBAC alone. The chosen product direction is:
operator-status remains status/events-only, while actual finalizer add/remove is
deferred to a future component that owns the `SwBlockVolume` lifecycle.

## Goal

Validate that delete-safety remains observable and actionable without granting
operator-status any finalizer, spec, workload, storage, or host mutation power.

The controller may write:

```text
SwBlockCluster/status
SwBlockVolume/status
Kubernetes Events
```

It must not write:

```text
SwBlockVolume.metadata.finalizers
SwBlockVolume.spec
PVC/PV/Pod/Deployment/StorageClass/Secret/Node resources
iSCSI/multipath/dmsetup/hostPath/Helm/storage state
```

## Preflight

Use a clean lab. If multi-node gates are run, restore `tp01` first; Phase 38/39
QA previously reported it as `NotReady`/unreachable.

Install with operator-status write mode:

```bash
helm install sw-block charts/seaweed-block --namespace kube-system \
  --create-namespace -f values.day1.yaml \
  --set operatorStatus.create=true \
  --set operatorStatus.dryRun=false \
  --wait --timeout 10m
```

Confirm RBAC:

```bash
kubectl auth can-i patch swblockvolumes --subresource=status \
  --as system:serviceaccount:kube-system:sw-block-operator-status -n kube-system
kubectl auth can-i create events \
  --as system:serviceaccount:kube-system:sw-block-operator-status -n kube-system
kubectl auth can-i patch swblockvolumes \
  --as system:serviceaccount:kube-system:sw-block-operator-status -n kube-system
kubectl auth can-i patch swblockvolumes --subresource=finalizers \
  --as system:serviceaccount:kube-system:sw-block-operator-status -n kube-system
kubectl auth can-i patch pods \
  --as system:serviceaccount:kube-system:sw-block-operator-status -n kube-system
kubectl auth can-i patch pvc \
  --as system:serviceaccount:kube-system:sw-block-operator-status -n default
kubectl auth can-i update storageclasses \
  --as system:serviceaccount:kube-system:sw-block-operator-status
```

Expected:

- status/events: `yes`,
- main `swblockvolumes`, finalizers, pods/PVC/storageclasses: `no`.

## D4: Blocked Delete-Safety Status

Feed delete-safety evidence with residue, for example:

```text
swblockvolume-delete-summary.txt:
delete_requested=true
finalizer_present=true
volume_id=<volume-id>
pvc_name=<pvc-name>
pv_name=<pv-name>

cleanup-summary.txt:
cleanup_status=failed
iscsi_residue_count=1
reason_codes=iscsi_node_records_present
```

Pass criteria:

- `SwBlockVolume.status.deleteSafety.state=blocked`,
- `SwBlockVolume.status.deleteSafety.decision=rejected`,
- reason is the verifier reason or `cleanup_required`,
- `status.conditions[]` includes `CleanupRequired=True`,
- safe next action is `observe.verify_cleanup` or collect bundle with
  `mutationAllowed=false`,
- no `Ready=True` or executed/released mutation claim appears,
- `operator_status=... finalizer_patches=0`,
- no finalizer-added or finalizer-released Events appear,
- repeated reconcile does not mint unbounded Events,
- no PVC/PV/Pod/Deployment/StorageClass/iSCSI/multipath/dmsetup/hostPath
  mutation is performed by operator-status.

## D5: Clean Delete-Safety Status

Feed clean cleanup evidence:

```text
delete_requested=true
finalizer_present=true
volume_id=<volume-id>
pvc_name=<pvc-name>
pv_name=<pv-name>
cleanup_status=ok
k8s_residue_count=0
iscsi_residue_count=0
multipath_residue_count=0
process_residue_count=0
hostpath_residue_count=0
failure_count=0
```

Pass criteria:

- `SwBlockVolume.status.deleteSafety.state=releasable`,
- `SwBlockVolume.status.deleteSafety.decision=allowed`,
- `finalizerReleaseAllowed=true` is visible as a decision fact only,
- no finalizer patch is attempted and no finalizer-released Event is emitted,
- `operator_status=... finalizer_patches=0`,
- repeated reconcile is idempotent,
- final cleanup verifier returns `cleanup_status=ok`,
- all residue counters are zero.

## D6 Preview: Multi-Volume Status Isolation

Do not test finalizer add/remove. Test that delete-safety evidence for volume A
does not change volume B/C status, identity, publish target, or Events.

## Report

Write or update the sign-off at:

```text
internal/docs/qa-assignments/phase39-d4-d5-finalizer-delete-safety-qa-signoff.md
```

Required verdict fields:

- source commit,
- lab node health, especially `tp01`,
- RBAC boundary result,
- D4 blocked delete-safety status result,
- D5 clean delete-safety status result,
- final cleanup audit,
- blocking findings,
- non-blocking findings,
- recommendation for D6 multi-volume status isolation.
