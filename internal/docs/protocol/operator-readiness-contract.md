# Operator Readiness Contract

Status: Phase 37 D1. Read-only CRD/status contract plus actionable operations
fields. The controller may publish status and Events only.

## Purpose

This document defines how the Phase 22 `ManagedVolume` read model can feed a
future Kubernetes operator without letting the operator rediscover volume state
or bypass authority.

The base contract is:

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

Phase 36 extends the same contract with actionable read-only operations fields:

```text
node readiness facts
-> SwBlockCluster.status.nodes[]
support/evidence pointers
-> SwBlockCluster.status.supportBundleRefs[] / evidenceRefs[]
cleanup verifier facts
-> SwBlockCluster.status.cleanup + CleanupRequired condition
safe next-step hints
-> SwBlockCluster.status.safeNextSteps[]
```

These fields are still observation and advice. They do not transfer lifecycle
ownership to the operator.

Phase 37 tightens the node-readiness side of the contract. Node status must be
derived from live Kubernetes, CSI, image, host-prerequisite, and publish-target
facts, not replay-only bundles or helper summaries.

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
| Node readiness | Kubernetes + preflight/verifier evidence | expose schedulability, image, iSCSI, multipath, and hostPath readiness |
| Cleanup residue | cleanup verifier / host evidence | expose residue type and safe next step |
| Support bundle | CLI/TestOps bundle collector | expose evidence refs and collection/replay commands |

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
- `nodes[]`
- `volumeCount`
- `readyVolumeCount`
- `blockedVolumeCount`
- `staleVolumeCount`
- `conditions[]`
- `evidenceRefs[]`
- `supportBundleRefs[]`
- `cleanup`
- `safeNextSteps[]`

Phase 36/37 cluster node status fields are:

| Field | Owner Input | Meaning | Stability |
|---|---|---|---|
| `status.nodes[].name` | Kubernetes / inventory | product node identity | stable |
| `status.nodes[].kubernetesNode` | Kubernetes API | Kubernetes node name when known | stable |
| `status.nodes[].internalIP` | Kubernetes API / inventory | observed node IP | stable |
| `status.nodes[].schedulable` | Kubernetes API | scheduling availability fact | stable |
| `status.nodes[].ready` | Kubernetes API / heartbeat | node Ready fact | stable |
| `status.nodes[].status` | operator-status aggregate | ready/blocked/unknown-style node status | provisional |
| `status.nodes[].reasonCode` | operator-status aggregate | stable reason for non-ready node | provisional |
| `status.nodes[].conditions[]` | operator-status aggregate | Kubernetes-style node readiness conditions | provisional |
| `status.nodes[].evidenceRefs[]` | evidence producer | supporting artifact paths | stable |

Phase 36 introduced the initial node reason codes `node_ready`,
`node_not_ready`, `node_scheduling_disabled`, and `image_missing_on_node`.

Phase 37 adds the live node blocker vocabulary:

| Reason Code | Meaning |
|---|---|
| `csi_node_pod_not_ready` | CSI node pod is absent, not Ready, or blocked by image/runtime state |
| `csi_driver_not_registered` | CSIDriver or per-node CSINode driver registration is missing |
| `iscsi_prereq_missing` | node iSCSI prerequisite evidence is missing or unhealthy |
| `multipath_prereq_missing` | node multipath prerequisite evidence is missing or unhealthy |
| `publish_target_loopback_cross_node` | loopback publish target would be consumed from another node |

The Phase 37 live node evidence contract is pinned in
`core/ops.LiveNodeEvidenceFactContract()`. Each fact names one authority,
one participant, a stability level, and the user-visible projection surface.
The contract is passive and read-only; it does not authorize probes or
operator-executed host changes.

Phase 36 cleanup status fields are:

| Field | Owner Input | Meaning | Stability |
|---|---|---|---|
| `status.cleanup.status` | cleanup verifier | `ok` or `failed` cleanup verdict | stable |
| `status.cleanup.*ResidueCount` | cleanup verifier | residue counters by category | stable |
| `status.cleanup.failureCount` | cleanup verifier | total cleanup verifier failures | stable |
| `status.cleanup.failedPhase` | cleanup verifier | failed cleanup/verifier phase | provisional |
| `status.cleanup.reasonCodes[]` | cleanup verifier | stable residue reason codes | provisional |
| `status.cleanup.evidenceRef` | cleanup verifier | cleanup summary artifact | stable |

Safe next-step fields are advisory only:

| Field | Meaning |
|---|---|
| `status.safeNextSteps[].type` | action identifier such as `observe.collect_bundle` |
| `status.safeNextSteps[].mode` | `read_only`, `dry_run`, or `scripted` |
| `status.safeNextSteps[].command` | suggested command text when safe to print |
| `status.safeNextSteps[].reasonCode` | reason that made the step relevant |
| `status.safeNextSteps[].mutationAllowed` | must remain `false` in Phase 36 |
| `status.safeNextSteps[].evidenceRefs[]` | evidence backing the suggestion |

ManagedVolume actions remain non-executing:

```text
mutation_allowed=false
mode=read_only | dry_run
```

Phase 36 cluster-level `safeNextSteps[]` may also use `mode=scripted` for
user/TestOps-invoked commands such as cleanup verification. `scripted` does not
grant operator execution rights; it is still a printed next-step hint with
`mutationAllowed=false`.

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
| `observe.verify_cleanup` | `scripted` | user/TestOps cleanup verifier |
| `authority.request_promotion` | not emitted by Phase 23 for execution | blockmaster recovery only |

In Phase 36, `scripted` still means user-initiated and outside the operator
reconcile loop. It must not be executed automatically by operator-status.

## Non-Claims

This contract does not deliver:

- controller reconciliation,
- mutating actions,
- automatic repair/rebuild,
- automatic cleanup,
- CR object ownership or finalizers,
- support-bundle upload,
- dashboard buttons,
- production RBAC/audit.

It exists so those future pieces use the same facts and safety boundaries.

## Tests

Pinned by:

- `core/ops/managed_volume_operator_contract_test.go`
- `core/ops/managed_volume_crd_contract_test.go`
- `core/ops/kubernetes_crd_manifests_test.go`
- `core/ops/operator_status_controller_test.go`
- `core/ops/phase37_node_evidence_contract_test.go`

Regression:

```text
go test ./core/ops -run "ManagedVolumeOperatorContract|ManagedVolumeCRDContract|Phase35D1|Phase36D1|Phase37D1|OperatorStatusReconciler" -count=1
```
