# Code Map

This page maps product responsibilities to code entry points. It is a guide for
developers; it is not a complete API reference.

## Commands

| Command | Responsibility | Key files |
|---|---|---|
| `sw-block` | operations CLI, report/dashboard/explain, operator-status, lifecycle-owner | `cmd/sw-block/main.go` |
| `blockmaster` | control-plane master and launcher coordination | `cmd/blockmaster/main.go`, `core/host/master/` |
| `blockvolume` | per-volume data-plane process, status endpoint, readiness publishing | `cmd/blockvolume/main.go`, `core/storage/` |
| `blockcsi` | Kubernetes CSI controller/node integration | `cmd/blockcsi/main.go`, `core/csi/` |

## Core Packages

| Package | Role |
|---|---|
| `core/ops` | ManagedVolume model, status projection, CRD writer, action evaluator, lifecycle-owner |
| `core/csi` | CSI CreateVolume/DeleteVolume, Kubernetes metadata registration, target staging |
| `core/storage` | WAL/store behavior and dirty-failure handling |
| `core/host/master` | blockmaster observation and volume coordination |
| `testops` | scenario descriptions and release gates |

## Ownership Split

The current operation layer uses three separate owners:

| Owner | Can mutate | Must not mutate |
|---|---|---|
| CSI | `SwBlockVolume` identity/spec after successful `CreateVolume` | `.status`, finalizers |
| operator-status | `SwBlockCluster.status`, `SwBlockVolume.status`, Events | spec, finalizers, PVC/PV/workloads/storage |
| lifecycle-owner | `SwBlockVolume.metadata.finalizers` only | spec, status, labels, annotations, ownerReferences, PVC/PV/workloads/storage |

This split is deliberate. It prevents a status bug from becoming a storage
mutation and prevents lifecycle ownership from becoming a broad operator.

## Important Entry Points

| Behavior | Entry point |
|---|---|
| Write operator CRD status | `core/ops/operator_status_controller.go` |
| Patch Kubernetes status | `core/ops/kubernetes_status_writer.go` |
| Evaluate safe actions | `core/ops/action_model.go` |
| Add/release protection finalizer | `core/ops/lifecycle_owner_controller.go` |
| Register `SwBlockVolume` from CSI | `core/csi/kubernetes_metadata.go` |
| Evaluate future read-write action maturity | `core/ops/action_model.go`, future owner executors |
| Project delete-safety from cleanup evidence | `core/ops/observation_bundle.go` |
| Parse cleanup verifier output | `core/ops/cleanup_evidence.go` |
| Enrich live node/CSI evidence | `cmd/sw-block/main.go`, `core/ops/kubernetes_node_evidence.go` |
| Replay support bundles | `core/ops/observation_bundle.go`, `scripts/collect-helm-support-bundle.sh` |
| Render report/dashboard/snapshot | `core/ops/observation_report.go`, `core/ops/observation_dashboard.go`, `core/ops/operator_snapshot.go` |
| SmartWAL and recovery frontier handling | `core/storage/smartwal/`, `core/recovery/`, `core/transport/` |
| CRD/RBAC status conformance | `core/ops/kubernetes_status_writer.go`, `core/ops/kubernetes_status_conformance_test.go`, `charts/seaweed-block/crds/` |
| blockvolume process readiness | `cmd/blockvolume/main.go`, `core/host/volume/host.go`, `core/frontend/durable/` |
| Release image/chart compatibility | `charts/seaweed-block/templates/`, `charts/seaweed-block/values.yaml`, `docs/releases/`, `docs/quickstart-kubernetes.md` |
| Future GPUDirect/cuFile probes | no current product code; expected first home is a TestOps/helper utility plus later `core/ops` evidence ingestion |

## Development Rule

Before adding a new lifecycle action, identify:

```text
fact owner
judgment owner
action owner
admission/RBAC boundary
user-visible status
QA gate
failure bundle evidence
```

If any item is missing, the feature is not ready for product code.

Future GPU data-path work has an additional rule: identify whether the claim is
file path (`cuFile` over a mounted PVC), object path (`cuObject`/S3-style), or
transport path (RDMA/NVMe). Do not mix those claims in one code path or test.
