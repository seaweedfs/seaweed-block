# Phase 39 Finished Plan: Delete-Safety Status Boundary

Status: closed on 2026-06-13.

Branch: `phase33-testops-failure-hardening`

## Outcome

Phase 39 originally targeted finalizer/delete safety as the first mutating
operator path. Live QA proved that CRD finalizer mutation cannot be bounded by
`swblockvolumes/finalizers` RBAC alone: CRDs expose no usable HTTP
`/finalizers` endpoint, and modifying `metadata.finalizers` requires main
`patch swblockvolumes` authorization.

The product decision was to preserve the Phase 35-38 operator-status safety
boundary. Phase 39 therefore closes as a status/events-only delete-safety
boundary:

- operator-status writes `SwBlockCluster/status`, `SwBlockVolume/status`, and
  Kubernetes Events only,
- operator-status does not patch `SwBlockVolume.metadata.finalizers`,
  `SwBlockVolume.spec`, workloads, storage resources, or host state,
- delete-safety is visible as blocked/releasable status, reason, evidence,
  `CleanupRequired`, safe next steps, and `finalizerReleaseAllowed` as a fact,
- actual finalizer add/remove is deferred to a future lifecycle-owner component.

## Delivered

- Delete-safety contract and decision projection:
  - states: `not_requested`, `requested`, `blocked`, `releasable`, `released`,
  - blocked residue projects `CleanupRequired=True`,
  - clean evidence projects `releasable/allowed`,
  - missing/residue evidence never claims deletion is safe.
- Status-only operator boundary:
  - Helm RBAC grants CRD reads, CRD `/status` writes, Events create, and
    Kubernetes read-only evidence access,
  - no finalizers, main CRD patch, PVC/PV, workload, storageclass, secret, node,
    or host mutation grant.
- CRD schema hardening:
  - `SwBlockVolume.status.allowedActions[].mode` accepts `read_only`,
    `dry_run`, and `scripted`,
  - regression test covers the enum to prevent the live 422 from returning.
- Multi-volume status isolation:
  - delete-safety evidence for one volume does not contaminate other volumes,
  - unrelated volumes keep their own identity, status, reason, and publish
    target,
  - clean delete-safety evidence can project independently on another volume.

## QA Evidence

- D4/D5 status-only re-validation:
  `internal/docs/qa-assignments/phase39-d4-d5-finalizer-delete-safety-qa-signoff.md`
  - RBAC status/events-only: PASS
  - D4 blocked delete-safety status: PASS on `f167f9a`
  - D5 clean delete-safety status: PASS
  - final cleanup verifier: PASS
- D6 multi-volume status isolation:
  `internal/docs/qa-assignments/phase39-d6-multi-volume-delete-safety-status-isolation-qa-signoff.md`
  - A blocked delete-safety does not contaminate B/C,
  - C can independently become releasable,
  - `finalizer_patches=0`,
  - no finalizer Events,
  - CRD/report surfaces agree.

Local verification during close:

```text
go test ./core/ops ./cmd/sw-block
go test ./cmd/blockcsi ./scripts
helm lint charts/seaweed-block
helm template sw-block charts/seaweed-block --namespace kube-system --include-crds --set operatorStatus.create=true --set operatorStatus.dryRun=false
git diff --check
```

## Non-Claims

Phase 39 does not claim:

- automatic deletion protection for `SwBlockVolume`,
- finalizer add/remove by operator-status,
- PVC/PV finalizer ownership,
- automatic cleanup execution,
- iSCSI/multipath/hostPath deletion by the operator,
- promotion, fencing, rebuild, failback, backup, restore, or NVMe ANA parity.

## Follow-Ups

1. **Clear stale deleteSafety when delete evidence disappears.**
   QA observed a within-volume stale field: after a delete-summary was removed,
   the volume status returned to ready but retained the prior
   `deleteSafety=blocked`. This did not contaminate other volumes, but the field
   should be cleared or refreshed to avoid confusing consumers.
2. **Add live/envtest coverage for KubernetesStatusClient.**
   Multiple defects in Phases 35-39 passed mock unit tests and Helm rendering but
   failed against the real Kubernetes API/CRD/RBAC surface. Add an envtest-style
   harness using real CRD schemas and the operator ServiceAccount permissions.
3. **Restore `tp01`.**
   Lab infra remains `NotReady`/unreachable. Restore before RF=3 live
   multi-node work.
4. **Future lifecycle-owner finalizers.**
   Implement finalizer add/remove in the component that owns
   `SwBlockVolume` object lifecycle, not in operator-status, unless the project
   later chooses an admission-bounded main patch model.
