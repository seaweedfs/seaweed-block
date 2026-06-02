# Operator Readiness Contract

Status: Phase 35 D1 seed. Read-only CRD/status contract only; no controller
implementation yet.

## Purpose

This document defines how the Phase 22 `ManagedVolume` read model can feed a
future Kubernetes operator without letting the operator rediscover volume state
or bypass authority.

The contract is:

```text
ManagedVolumeProjection
-> Conditions
-> Events
-> read-only / dry-run allowed_actions
-> future operator status
```

It is not:

- a controller loop,
- a mutating admin workflow,
- a repair/rebuild/failback implementation.

The Phase 35 D1 CRD schema lives in:

- `charts/seaweed-block/crds/swblockclusters.block.seaweedfs.com.yaml`
- `charts/seaweed-block/crds/swblockvolumes.block.seaweedfs.com.yaml`

The status-only RBAC seed lives in:

- `charts/seaweed-block/templates/operator-status-rbac.yaml`

## Source Of Truth

The future operator must consume `ManagedVolumeProjection` or the equivalent
product-owned observation API. It must not recompute primary, recovery,
host-path, or workload status from raw pod logs.

Truth ownership remains:

| Domain | Truth Owner | Operator Contract Use |
|---|---|---|
| PVC/PV/Pod/Node | Kubernetes API | mirror intent and runtime blockers |
| Publish/stage | CSI + master publish target | explain attach/reattach state |
| Authority/epoch | blockmaster | expose primary and recovery status |
| Replica durability | blockvolume / master evidence | expose readiness and blockers |
| Host path | host initiator/kernel tools | expose transparent path state |
| Workload check | user/TestOps/app evidence | expose data-check proof |

The operator may publish Conditions/Events. It may not mint authority or decide
promotion.

## Contract Shape

`core/ops` exposes:

```text
ManagedVolumeOperatorContractFromProjection(projection)
```

The contract contains:

- `status.volume_id`
- `status.pvc_name`
- `status.status`
- `status.reason_code`
- `status.conditions[]`
- `status.non_claims[]`
- `status.evidence_refs[]`
- `events[]`
- `allowed_actions[]`

The Kubernetes CRD projection keeps the same vocabulary:

| ManagedVolume Contract | CRD Status Path |
|---|---|
| `status.volume_id` | `SwBlockVolume.status.volumeID` |
| `status.pvc_name` | `SwBlockVolume.status.pvcName` |
| `status.status` | `SwBlockVolume.status.status` |
| `status.reason_code` | `SwBlockVolume.status.reasonCode` |
| `status.conditions[]` | `SwBlockVolume.status.conditions[]` |
| `status.non_claims[]` | `SwBlockVolume.status.nonClaims[]` |
| `status.evidence_refs[]` | `SwBlockVolume.status.evidenceRefs[]` |
| `allowed_actions[]` | `SwBlockVolume.status.allowedActions[]` |

Cluster-level aggregate fields map to `SwBlockCluster.status.*`:

- `nodeCount`
- `volumeCount`
- `readyVolumeCount`
- `blockedVolumeCount`
- `staleVolumeCount`
- `conditions[]`
- `evidenceRefs[]`

All Phase 23 actions are non-executing:

```text
mutation_allowed=false
mode=read_only | dry_run
```

## Condition Mapping

| ManagedVolume Status | Conditions |
|---|---|
| `ready` | `Ready=True` |
| `recovered` | `Ready=True`, `Recovered=True` |
| `blocked` | `Ready=False`, `Blocked=True` |
| `recovering` | `Ready=False`, `Recovering=True` |
| `invalid` / `unsafe` | `Ready=False`, `Invalid=True` |
| `degraded` | `Ready=False` |
| `unknown` | `Ready=Unknown` |

Each condition must include:

- `reason`,
- `severity`,
- message,
- evidence refs when available.

## Event Mapping

The future operator may emit Kubernetes Events from Conditions:

- `severity=info` -> `Normal`
- `severity=warning` -> `Warning`
- `severity=error` -> `Warning`

Events are explanatory. They are not authority decisions.

## Action Mapping

Allowed action entries are currently advisory only. A future operator must not
execute an action unless all of these are true:

1. The action is still present in the latest projection.
2. Preconditions still hold.
3. The operator owns the named `owner_executor`.
4. RBAC allows the target mutation.
5. An audit record will be written.
6. The action has a separate product gate if it can disrupt I/O.

Examples:

| Action | Current Mode | Future Executor |
|---|---|---|
| `observe.collect_bundle` | `read_only` | ops / support bundle collector |
| `safe_k8s.reinstall_external_iscsi` | `dry_run` | installer or operator |
| `safe_k8s.import_csi_image` | `dry_run` | installer or operator |
| `observe.inspect_host_path` | `dry_run` | ops / host diagnostics |
| `authority.request_promotion` | not emitted by Phase 23 for execution | blockmaster recovery only |

## Non-Claims

This contract does not deliver:

- controller reconciliation,
- mutating actions,
- automatic repair/rebuild,
- dashboard buttons,
- production RBAC/audit.

It exists so those future pieces use the same facts and safety boundaries.

## Tests

Pinned by:

- `core/ops/managed_volume_operator_contract_test.go`
- `core/ops/managed_volume_crd_contract_test.go`
- `core/ops/kubernetes_crd_manifests_test.go`

Regression:

```text
go test ./core/ops -run "ManagedVolumeOperatorContract|ManagedVolumeCRDContract|Phase35D1" -count=1
```
