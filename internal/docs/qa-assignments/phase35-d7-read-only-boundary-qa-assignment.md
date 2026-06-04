# QA Assignment - Phase 35 D7 Read-Only Boundary

Source branch: `phase33-testops-failure-hardening`

Gate: D7 read-only boundary for the Kubernetes-native operator-status
foundation.

## Local Review Summary

The operator-status path is intentionally narrow:

- Helm creates a dedicated `sw-block-operator-status` ServiceAccount only when
  `operatorStatus.create=true`.
- Its ClusterRole grants:
  - `get/list/watch` on `swblockclusters` and `swblockvolumes`
  - `get/update/patch` on `swblockclusters/status` and
    `swblockvolumes/status`
  - `create` on core `events`
- `core/ops.KubernetesStatusClient` exposes only:
  - `WriteClusterStatus`
  - `WriteVolumeStatus`
  - `EmitEvent`
- The client only PATCHes `/status` subresources and POSTs core Events.
- It has no methods for spec mutation, PVC/PV, workloads, Secrets,
  StorageClasses, Helm, iSCSI, multipath, or hostPath mutation.

Scoped local checks already cover this boundary:

```text
go test ./core/ops ./cmd/sw-block ./cmd/blockcsi
helm lint charts/seaweed-block
helm template ... --set operatorStatus.create=true --set operatorStatus.dryRun=false
```

## Required Live QA Checks

Install the chart with the operator-status controller enabled in write mode:

```text
operatorStatus.create=true
operatorStatus.dryRun=false
```

Use the rendered ServiceAccount name from the release, usually:

```text
system:serviceaccount:kube-system:sw-block-seaweed-block-operator-status
```

### Allowed Verbs

These must be `yes`:

```text
kubectl auth can-i get swblockvolumes.block.seaweedfs.com --as <SA> -n kube-system
kubectl auth can-i list swblockvolumes.block.seaweedfs.com --as <SA> -n kube-system
kubectl auth can-i watch swblockvolumes.block.seaweedfs.com --as <SA> -n kube-system
kubectl auth can-i patch swblockvolumes.block.seaweedfs.com/status --as <SA> -n kube-system
kubectl auth can-i update swblockvolumes.block.seaweedfs.com/status --as <SA> -n kube-system
kubectl auth can-i patch swblockclusters.block.seaweedfs.com/status --as <SA> -n kube-system
kubectl auth can-i create events --as <SA> -n kube-system
```

### Forbidden Verbs

These must be `no`:

```text
kubectl auth can-i patch swblockvolumes.block.seaweedfs.com --as <SA> -n kube-system
kubectl auth can-i update swblockvolumes.block.seaweedfs.com --as <SA> -n kube-system
kubectl auth can-i delete swblockvolumes.block.seaweedfs.com --as <SA> -n kube-system
kubectl auth can-i create pods --as <SA> -n kube-system
kubectl auth can-i patch pods --as <SA> -n kube-system
kubectl auth can-i delete pods --as <SA> -n kube-system
kubectl auth can-i create persistentvolumeclaims --as <SA> -n default
kubectl auth can-i patch persistentvolumeclaims --as <SA> -n default
kubectl auth can-i delete persistentvolumeclaims --as <SA> -n default
kubectl auth can-i create persistentvolumes --as <SA>
kubectl auth can-i patch persistentvolumes --as <SA>
kubectl auth can-i delete persistentvolumes --as <SA>
kubectl auth can-i create secrets --as <SA> -n kube-system
kubectl auth can-i patch secrets --as <SA> -n kube-system
kubectl auth can-i create deployments.apps --as <SA> -n kube-system
kubectl auth can-i patch deployments.apps --as <SA> -n kube-system
kubectl auth can-i delete deployments.apps --as <SA> -n kube-system
kubectl auth can-i create storageclasses.storage.k8s.io --as <SA>
kubectl auth can-i patch storageclasses.storage.k8s.io --as <SA>
kubectl auth can-i delete storageclasses.storage.k8s.io --as <SA>
```

### Runtime Boundary

Run one write-mode reconcile against a ready, blocked, or stale bundle and
verify:

```text
operator_status=write_status ... mutation_allowed=false
```

Then verify:

```text
SwBlockCluster.spec is unchanged
SwBlockVolume.spec is unchanged
SwBlockCluster.status is updated
SwBlockVolume.status is updated
Kubernetes Event(s) may be created
no PVC/PV/workload/Secret/StorageClass changed because of operator-status
```

## Pass Criteria

D7 passes when:

- All allowed `can-i` checks return `yes`.
- All forbidden `can-i` checks return `no`.
- Runtime reconcile writes CRD `.status` and Events only.
- Output and operator snapshot still report `mutation_allowed=false`.
- Cleanup leaves no product residue.

