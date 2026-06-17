# Operation Layer v0.5

The v0.5 operation layer is the first bounded mutating lifecycle path. It is
not a broad operator. It exists to prove one controlled loop:

```text
facts -> judgment -> status/action -> admission-confined mutation -> evidence
```

## Why It Exists

Before this layer, Seaweed Block could observe many states but could not safely
own a Kubernetes lifecycle mutation. Phase 39 showed why: patching finalizers on
a CRD requires main-object patch permission, which is too broad unless an
admission boundary confines the write.

This is the key historical trap:

```text
CRD finalizers are changed through main-object patch
main-object patch can also change spec/metadata
RBAC cannot express "only this one finalizer field" for this CRD
therefore code review alone is not a safety boundary
```

The product choice was not "just add a finalizer". The real choice was whether
Seaweed Block could prove a bounded mutation against a real Kubernetes API.

v0.5 resolves that by separating three components:

| Component | Role |
|---|---|
| CSI | create/update `SwBlockVolume` identity CR |
| operator-status | publish `.status` and Events only |
| lifecycle-owner | add/release only the Seaweed Block protection finalizer |

## End-To-End Loop

```mermaid
flowchart TD
  A[Live facts: PVC, volume, cleanup evidence] --> B[operator-status judgment]
  B --> C[SwBlockVolume.status deleteSafety]
  C --> D{lifecycle-owner decision}
  D -->|missing/stale/residue| E[hold finalizer]
  D -->|allowed + releasable| F[patch finalizer list]
  F --> G[VAP admits only protection finalizer shape]
  G --> H[CR deletion completes]
  E --> I[Warning Event + user-facing reason]
  H --> J[Normal Event + zero residue gate]
```

## Delete-Safety Contract

| Evidence | Decision | State | Finalizer |
|---|---|---|---|
| missing cleanup evidence | unknown | requested | held |
| stale cleanup evidence | unknown | requested | held |
| residue present | rejected | blocked | held |
| clean fresh evidence | allowed | releasable | released |

The cleanup evidence is external. The lifecycle-owner does not execute cleanup.

## Why This Is Not Automatic Cleanup

Automatic cleanup would require an executor that mutates host or Kubernetes
state:

- iSCSI session/node DB cleanup,
- multipath map cleanup,
- dmsetup cleanup,
- hostPath cleanup,
- PVC/PV/workload cleanup.

Those are different risk domains. v0.5 deliberately stops before that line.
The lifecycle-owner only decides whether the `SwBlockVolume` CR may disappear.
It does not repair the world to make that decision true.

## Main Code

| Behavior | Entry point |
|---|---|
| operator-status reconcile | `core/ops/operator_status_controller.go` |
| Kubernetes status writer | `core/ops/kubernetes_status_writer.go` |
| cleanup evidence parsing | `core/ops/cleanup_evidence.go` |
| delete-safety projection | `core/ops/observation_bundle.go` |
| lifecycle-owner reconcile | `core/ops/lifecycle_owner_controller.go` |
| action model | `core/ops/action_model.go` |

## Admission Boundary

The lifecycle-owner needs main-object patch because CRD finalizers are mutated
through the main object. Kubernetes RBAC alone cannot express "finalizers only"
for this CRD. The boundary is therefore:

```text
RBAC permits lifecycle-owner main patch on SwBlockVolume
AND
ValidatingAdmissionPolicy denies any patch that changes spec, status, labels,
annotations, ownerReferences, foreign finalizers, or mixed fields
```

## Failure History Encoded In The Design

Several live-only failures shaped the current design:

| Failure | Lesson |
|---|---|
| status writer payload passed mocks but failed CRD schema | tests must hit real schema or schema-aware conformance |
| CRD condition enum rejected live node facts | status vocabulary must be shared with CRD schema |
| finalizer `/finalizers` patch returned 404 | CRDs do not expose a generic finalizers subresource endpoint |
| main-object finalizer patch returned 403 with status-only RBAC | finalizer ownership needs a separate lifecycle-owner role |
| scripted action mode rejected by CRD enum | action vocabulary must match every surface |
| VAP optional-field CEL denied legitimate patch | admission policy needs live API proof, not only template rendering |

The operation layer is therefore not "more YAML". It is the product result of
schema, RBAC, admission, status, and lifecycle evidence failing in realistic
ways until the boundary became explicit.

## QA Evidence

| Phase | Evidence |
|---|---|
| 41 | lifecycle-owner foundation and dry-run decision model |
| 42 | real Kubernetes ValidatingAdmissionPolicy boundary |
| 43 | finalizer add/release as isolated gates |
| 44 | integrated PVC -> protected CR -> hold/release -> zero-residue path |

## Non-Claims

- No automatic cleanup execution.
- No PVC/PV/workload deletion by operator-status or lifecycle-owner.
- No rebuild, failback, backup, or upgrade execution.
- No production operator claim.
