# Phase 39 D4/D5 QA Assignment: Finalizer Delete Safety

Status: ready for QA.

Source branch: `phase33-testops-failure-hardening`.

Required source floor:

- `7143c8f phase39: define delete safety contract`
- `fd3977b phase39: project delete safety status`
- `3340038 phase39: add finalizer mutation boundary`
- `07c50d3 phase39: gate blocked delete finalizer hold`
- `a90dda3 phase39: gate clean delete finalizer release`

## Goal

Validate the first bounded mutating operator path:

```text
SwBlockVolume.metadata.finalizers
```

The operator may add or remove only
`block.seaweedfs.com/swblockvolume-protection`. It must not delete or mutate
PVCs, PVs, Pods, Deployments, StorageClasses, Helm releases, images, iSCSI,
multipath, dmsetup, hostPath, replica authority, rebuild/failback, backup, or
restore state.

## Preflight

Use a clean lab. If multi-node gates are run, restore `tp01` first; Phase 38 QA
reported it as `NotReady`/unreachable.

Render and install with operator-status write mode:

```bash
helm template sw-block charts/seaweed-block --namespace kube-system --include-crds \
  --set operatorStatus.create=true --set operatorStatus.dryRun=false >/tmp/sw-block-phase39.yaml
helm install sw-block charts/seaweed-block --namespace kube-system \
  --create-namespace -f values.day1.yaml \
  --set operatorStatus.create=true \
  --set operatorStatus.dryRun=false \
  --wait --timeout 10m
```

Confirm RBAC:

```bash
kubectl auth can-i patch swblockvolumes --subresource=finalizers \
  --as system:serviceaccount:kube-system:sw-block-operator-status -n kube-system
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

- finalizers/status/events: `yes`,
- pods/PVC/storageclasses: `no`.

## D4: Blocked Delete Holds Finalizer

Create a `SwBlockVolume` stub for a known managed volume and let the operator add
the finalizer.

Then inject delete-safety evidence with residue, for example a bundle or status
source containing:

```text
swblockvolume-delete-summary.txt
cleanup-summary.txt
```

Minimum delete artifact:

```text
delete_requested=true
finalizer_present=true
volume_id=<volume-id>
pvc_name=<pvc-name>
pv_name=<pv-name>
```

Minimum cleanup residue artifact:

```text
cleanup_status=failed
iscsi_residue_count=1
reason_codes=iscsi_node_records_present
```

Delete the `SwBlockVolume` object:

```bash
kubectl -n kube-system delete swblockvolume <name> --wait=false
```

Pass criteria:

- object remains with `metadata.deletionTimestamp` and the Seaweed Block
  finalizer still present,
- `status.deleteSafety.state=blocked`,
- `status.deleteSafety.decision=rejected`,
- `status.deleteSafety.reason` is the verifier reason or `cleanup_required`,
- `status.conditions[]` includes `CleanupRequired=True`,
- safe next action is `observe.verify_cleanup` or collect bundle with
  `mutationAllowed=false`,
- no `finalizer_released` Event appears,
- repeated reconcile does not duplicate the finalizer or mint unbounded
  finalizer-added Events,
- no PVC/PV/Pod/Deployment/StorageClass/iSCSI/multipath/dmsetup/hostPath
  mutation is performed by the operator.

## D5: Clean Delete Releases Finalizer

Use the same object shape, but provide clean cleanup evidence:

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

Delete the `SwBlockVolume` object:

```bash
kubectl -n kube-system delete swblockvolume <name> --wait=true --timeout=2m
```

Pass criteria:

- `status.deleteSafety.state=releasable` and `decision=allowed` are observed
  before deletion when possible,
- `finalizer_released` Event is emitted once,
- the Seaweed Block finalizer is removed,
- the `SwBlockVolume` object deletion completes,
- repeated reconcile is idempotent if the object still exists during the window,
- final cleanup verifier returns `cleanup_status=ok`,
- all residue counters are zero.

## Report

Write the sign-off to:

```text
internal/docs/qa-assignments/phase39-d4-d5-finalizer-delete-safety-qa-signoff.md
```

Required verdict fields:

- source commit,
- lab node health, especially `tp01`,
- RBAC boundary result,
- D4 blocked delete result,
- D5 clean delete result,
- final cleanup audit,
- blocking findings,
- non-blocking findings,
- recommendation for D6 multi-volume isolation.
