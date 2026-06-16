# Phase 43 D1/D2 QA: Lifecycle Owner Finalizer Add

## Scope

Validate the first product path that performs a bounded Kubernetes mutation:
the lifecycle-owner may add only
`block.seaweedfs.com/swblockvolume-protection` to existing `SwBlockVolume`
objects. This gate does not validate finalizer release yet.

## Preconditions

- Use a VAP-capable Kubernetes lab.
- Build or publish an image from the candidate commit.
- Install from a clean namespace.

## Required Checks

### G1 Local Contract

```text
go test ./core/ops ./cmd/sw-block
helm template sw-block charts/seaweed-block \
  --namespace kube-system \
  --set lifecycleOwner.create=true \
  --set lifecycleOwner.dryRun=false
```

Pass criteria:

- Tests pass.
- Render includes `sw-block-lifecycle-owner` Deployment, ServiceAccount, RBAC,
  ValidatingAdmissionPolicy, and ValidatingAdmissionPolicyBinding.
- Default chart render still does not install the lifecycle-owner.

### G2 Identity / RBAC Boundary

Install with:

```text
--set lifecycleOwner.create=true
--set lifecycleOwner.dryRun=false
```

Pass criteria:

- operator-status service account cannot patch main `swblockvolumes`.
- lifecycle-owner service account can patch main `swblockvolumes`.
- lifecycle-owner cannot create/update/patch/delete pods, PVCs, PVs, Secrets,
  StorageClasses, Deployments, Nodes, CSIDrivers, or CSINodes.
- lifecycle-owner can create Events.

### G3 Admission Boundary

Against the lifecycle-owner identity, verify:

- adding the Seaweed Block protection finalizer is allowed.
- repeating the add is idempotent.
- patching spec, status, labels, annotations, ownerReferences, or foreign
  finalizers is denied.
- mixed finalizer + non-finalizer mutations are denied.
- existing foreign finalizers, if present before the request, are preserved and
  not added/removed by lifecycle-owner.

### G4 Product Finalizer Add

Create an owned `SwBlockVolume` stub without the protection finalizer and let
the lifecycle-owner reconcile once.

Pass criteria:

- object gains exactly one Seaweed Block protection finalizer.
- spec, status, labels, annotations, and ownerReferences are unchanged.
- repeated reconcile does not duplicate the finalizer.
- a bounded `finalizer_added` Event is emitted.
- lifecycle-owner stdout reports `finalizer_patches=1` on the first reconcile
  and `finalizer_patches=0` on a subsequent reconcile.

### G5 Cleanup

Uninstall and verify:

```text
bash scripts/verify-helm-cleanup.sh <repo-root>
```

Pass criteria:

- `cleanup_status=ok`.
- no stuck `SwBlockVolume` objects, VAPs, bindings, RBAC, pods, PVCs, PVs,
  iSCSI sessions/node records, multipath maps, or dmsetup devices remain.

## Blocking Findings

Block D1/D2 if:

- operator-status gains main-object patch power.
- lifecycle-owner can mutate anything except `SwBlockVolume.metadata.finalizers`
  and Events.
- admission is absent, not propagated, or permits a non-finalizer mutation.
- finalizer add changes any non-finalizer field.
- repeated reconcile churns finalizers or Events unboundedly.
