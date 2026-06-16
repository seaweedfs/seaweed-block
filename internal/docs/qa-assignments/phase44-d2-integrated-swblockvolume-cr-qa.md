# Phase 44 D2 QA: Integrated SwBlockVolume Creation And Protection

Status: ready for QA.

## Goal

Prove the normal Day-1 PVC path now creates the Kubernetes object required by
the bounded delete lifecycle:

```text
CSI CreateVolume -> SwBlockVolume CR exists -> lifecycle-owner adds protection
finalizer -> operator-status writes Ready=True / first_volume_verified
```

This gate must use a candidate `sw-block` image and candidate
`seaweed-block-csi` image from the same source tree. Older CSI images do not
have `blockcsi --swblockvolume-cr-namespace`.

## Scope

In:

- Helm install with `operatorStatus.create=true`, `operatorStatus.dryRun=false`,
  `lifecycleOwner.create=true`, and `lifecycleOwner.dryRun=false`.
- One first PVC writer/reader flow.
- SwBlockVolume CR identity/spec creation by CSI.
- Protection finalizer add by lifecycle-owner.
- Status/Event publication by operator-status.
- RBAC/admission boundary checks.
- Cleanup verifier.

Out:

- Finalizer release on delete. That is D3/D4.
- Automatic cleanup execution.
- PVC/PV/workload/storage mutation by operator-status or lifecycle-owner.

## Required Checks

### G1: Local Contract

Run:

```text
go test ./core/csi ./cmd/blockcsi ./core/ops ./cmd/sw-block
helm lint charts/seaweed-block
helm template sw-block charts/seaweed-block --namespace kube-system
helm template sw-block charts/seaweed-block --namespace kube-system \
  --set operatorStatus.create=true \
  --set operatorStatus.dryRun=false \
  --set lifecycleOwner.create=true \
  --set lifecycleOwner.dryRun=false
```

Pass:

- Default render does not include `--swblockvolume-cr-namespace`.
- Enabled render includes `--swblockvolume-cr-namespace=kube-system`.
- Enabled render grants CSI `swblockvolumes` get/list/watch/create/update/patch.
- CSI does not receive `swblockvolumes/status` or
  `swblockvolumes/finalizers`.

### G2: Live Install And First Volume

Install from candidate images and create the first PVC using the existing
first-volume path.

Pass:

- Helm install succeeds.
- `sw-block-csi-controller`, `sw-block-csi-node`, `blockmaster`,
  `operator-status`, and `lifecycle-owner` pods are Running/Ready.
- Writer and reader verify the first PVC payload.

### G3: SwBlockVolume CR Exists

After first-volume writer/reader succeeds:

```text
kubectl -n kube-system get swblockvolumes -o json
```

Pass:

- Exactly one `SwBlockVolume` exists for the PVC.
- Object name matches the operator-status naming convention for the PVC.
- `.spec.pvcName` matches the PVC name.
- `.metadata.finalizers` contains exactly one
  `block.seaweedfs.com/swblockvolume-protection`.
- No foreign finalizer is created by Seaweed Block.

Fail:

- No `SwBlockVolume` exists.
- More than one object exists for the PVC.
- Object exists but has no protection finalizer after the lifecycle-owner
  interval.

### G4: Status And Event Agreement

Pass:

- `SwBlockVolume.status.status=ready`.
- `SwBlockVolume.status.reasonCode=first_volume_verified`.
- Ready condition is `True`.
- `deleteSafety` is absent or null for the normal non-deleting volume.
- A bounded Normal Event for finalizer add exists.
- A bounded Normal Event for `first_volume_verified` exists.

Fail:

- The CR is protected but status stays empty after operator-status interval.
- The volume shows `Ready=True` with a reason other than
  `first_volume_verified`.

### G5: Boundary

Run `kubectl auth can-i` as both service accounts.

Pass:

- operator-status can patch only CRD `/status` and create Events.
- operator-status cannot patch main `swblockvolumes`, finalizers, PVCs, PVs,
  pods, deployments, storageclasses, secrets, nodes, csidrivers, or csinodes.
- lifecycle-owner can patch main `swblockvolumes` but VAP denies spec, labels,
  annotations, ownerReferences, foreign finalizers, mixed patches, and status
  mutation.
- CSI controller has `swblockvolumes` spec-object verbs only when the operator
  surfaces are enabled; it has no `swblockvolumes/status` or finalizer-specific
  ownership.

### G6: Cleanup

Uninstall and run the project cleanup verifier.

Pass:

- `cleanup_status=ok`.
- Helm, pods, PVC/PV, iSCSI sessions and node DB records, multipath maps,
  dmsetup devices, product processes, and hostPath residue are zero.

## QA Verdict

D2 passes only when G1-G6 pass on a clean VAP-capable lab.

If G2/G3 fails because the CR is missing, file it as:

```text
SwBlockVolume CR not created by normal CSI CreateVolume path
```

If G3 succeeds but G4 fails, file it as:

```text
operator-status does not write status for CSI-created SwBlockVolume
```
