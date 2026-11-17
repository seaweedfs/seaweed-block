# Code Map

This page maps product responsibilities to code entry points. It is a guide for
developers; it is not a complete API reference.

**Use this when:** you look for the package or file that owns a behavior.

## Commands

| Command | Responsibility | Key files |
|---|---|---|
| `sw-block` | operations CLI, report/dashboard/explain, operator-status, lifecycle-owner | `cmd/sw-block/main.go` |
| `blockmaster` | control-plane master and launcher coordination | `cmd/blockmaster/main.go`, `core/host/master/` |
| `blockvolume` | per-volume-replica data-plane process, frontends, status endpoint, readiness publishing | `cmd/blockvolume/main.go`, `core/storage/` |
| `blockcsi` | Kubernetes CSI controller/node integration | `cmd/blockcsi/main.go`, `core/csi/` |

## Core Packages

| Package | Role |
|---|---|
| `core/ops` | ManagedVolume model, status projection, CRD writer, action evaluator, lifecycle-owner |
| `core/csi` | CSI CreateVolume/DeleteVolume, Kubernetes metadata registration, target staging |
| `core/lifecycle` | desired volumes, node inventory, placement intent, verified placement |
| `core/authority` | assignment and epoch publication, workspace moves |
| `core/launcher` | renders and reconciles per-replica `blockvolume` workloads |
| `core/host/master` | blockmaster observation and volume coordination |
| `core/host/volume` | blockvolume host: assignment, projection bridge, status, workspace lease |
| `core/frontend/iscsi`, `nvme`, `ublk` | block frontends over one Read / Write / Sync backend contract |
| `core/frontend/durable` | durable provider and the per-I/O lineage gate |
| `core/frontend/chunkstore` | SeaweedFS chunk backend: HEAD CAS, catalog, snapshots, GC |
| `core/storage` | walstore (WAL plus extent), chunk layer, recovery |
| `core/storage/smartwal` | experimental in-place engine |
| `core/replication`, `core/recovery`, `core/transport` | RF>1 shipping, catch-up and rebuild, replica fencing |
| `testops` | scenario descriptions and release gates |

The [core design](../design/block-design.md) explains the storage,
durability, authority and replication packages in depth; the
[agent workspace design](../design/agent-workspace-design.md) covers the
workspace packages and the [Kubernetes block design](../design/kubernetes-block-design.md)
the CSI, operator and promotion packages.

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
| Evaluate safe actions | `core/ops/managed_volume_action_evaluator.go` |
| Add/release protection finalizer | `core/ops/lifecycle_owner_controller.go` |
| Register `SwBlockVolume` from CSI | `core/csi/kubernetes_metadata.go` |
| Project delete-safety from cleanup evidence | `core/ops/observation_bundle.go` |
| Parse cleanup verifier output | `core/ops/cleanup_evidence.go` |
| Enrich live node/CSI evidence | `cmd/sw-block/main.go`, `core/ops/kubernetes_node_evidence.go` |
| Replay support bundles | `core/ops/observation_bundle.go`, `scripts/collect-helm-support-bundle.sh` |
| Render report/dashboard/snapshot | `core/ops/observation_report.go`, `core/ops/observation_dashboard.go`, `core/ops/operator_snapshot.go` |
| SmartWAL and recovery frontier handling | `core/storage/smartwal/`, `core/recovery/`, `core/transport/` |
| CRD/RBAC status conformance | `core/ops/kubernetes_status_writer.go`, `core/ops/kubernetes_status_conformance_test.go`, `charts/seaweed-block/crds/` |
| blockvolume process readiness | `cmd/blockvolume/main.go`, `core/host/volume/host.go`, `core/frontend/durable/` |
| Release image/chart compatibility | `charts/seaweed-block/templates/`, `charts/seaweed-block/values.yaml`, `docs/releases/`, `docs/quickstart-kubernetes.md` |

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
