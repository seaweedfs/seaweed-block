# Lifecycle Owner Control Contract

Status: Phase 41 D1 contract draft.

## Purpose

This contract separates observation, lifecycle ownership, and execution before
Seaweed Block adds any Kubernetes lifecycle mutation.

The immediate trigger is the Phase 39 finalizer finding: `SwBlockVolume` is a
CRD, so changing `metadata.finalizers` requires main-object
`patch swblockvolumes`. The released `operator-status` controller must not gain
that permission. Finalizer ownership therefore needs a lifecycle-owner boundary,
not a small RBAC tweak.

## Roles

| Role | Purpose | May mutate |
|---|---|---|
| Observer / status writer | Read facts, aggregate judgment, publish `.status`, create Events | CRD `/status`, Kubernetes Events |
| Lifecycle owner | Own CR lifecycle metadata and decide whether a lifecycle mutation is allowed | Explicitly approved lifecycle metadata only |
| Executor | Perform storage, workload, host, or data-plane actions after preconditions pass | Only actions explicitly assigned to that executor |

The roles may share code libraries, but they must not share permissions
implicitly. A component's Kubernetes RBAC is part of the product contract.

## Observer / Status Writer

Current component: `sw-block ops operator-status`.

Responsibilities:

- read live cluster and blockmaster evidence,
- classify volume, node, cleanup, drift, and delete-safety state,
- patch `SwBlockCluster.status` and `SwBlockVolume.status`,
- create bounded Kubernetes Events,
- render the same model into report, dashboard, `operator-snapshot.json`, and
  `ops explain`,
- preserve negative-first behavior: stale, blocked, corrupt, or unreachable
  evidence must not become false `Ready=True`.

Allowed Kubernetes permissions:

```text
get/list/watch swblockclusters, swblockvolumes
get/update/patch swblockclusters/status, swblockvolumes/status
create events
read-only Kubernetes evidence needed for status enrichment
```

Forbidden:

```text
patch/update/delete main SwBlockCluster or SwBlockVolume objects
patch/update metadata.finalizers
create/delete PVCs, PVs, pods, Deployments, StorageClasses, Secrets
run cleanup, repair, rebuild, failback, backup, restore, promotion, fencing
```

The observer may recommend safe next steps, but recommendation is not execution.

## Lifecycle Owner

Current status: not implemented as a mutating controller.

Responsibilities when introduced:

- own the lifecycle metadata for `SwBlockVolume` objects,
- add or remove lifecycle finalizers if Phase 41 approves that path,
- evaluate delete-safety preconditions before release,
- publish lifecycle decisions back through status and Events,
- stay idempotent under repeated reconcile,
- never execute storage/host cleanup as part of finalizer release.

Possible Kubernetes permissions:

```text
get/list/watch swblockvolumes
patch swblockvolumes              # only if finalizer mutation is approved
get/update/patch swblockvolumes/status
create events
```

If `patch swblockvolumes` is granted, Phase 41 must add an enforcement layer
that proves the lifecycle owner cannot mutate `.spec` or unrelated metadata.
Acceptable enforcement shapes include:

- admission policy or webhook that admits only the approved finalizer patch,
- generated patch builder plus envtest/live-apiserver negative tests proving
  spec and unrelated metadata patches fail,
- both, if the implementation is not obviously constrained.

Code review alone is not enough because prior phases repeatedly found live API
schema/RBAC failures that mock tests missed.

## Executor

Current status: no lifecycle executor is introduced by this contract.

Executor examples for future phases:

- cleanup executor for iSCSI node DB, multipath, dmsetup, process, or hostPath
  residue,
- repair/rebuild executor,
- failback executor,
- backup/restore executor,
- upgrade executor.

Executor responsibilities:

- accept an explicit action request,
- verify preconditions and evidence freshness,
- enforce invariants before mutation,
- emit evidence and Events that can be cold-read later,
- fail closed when evidence is missing, stale, or contradictory.

Phase 41 does not add these executors. It may only define how an executor would
be referenced in a decision.

## Action Contract

Every lifecycle action must carry these fields before it can be allowed:

```text
action_id
target_kind
target_name
side_effect_class
executor
policy_gate
preconditions
required_evidence
decision
decision_reason
mutation_allowed
evidence_ref
observed_at
```

Decision meanings:

| Decision | Meaning |
|---|---|
| allowed | Preconditions pass and the executor is permitted for this action |
| rejected | Preconditions fail or policy disables the action |
| unknown | Evidence is missing, stale, or contradictory |

`allowed` does not mean the observer executed the action. Execution requires a
component with the executor role.

## Delete-Safety Preconditions

For finalizer release or future delete-related lifecycle actions:

| Evidence | Decision |
|---|---|
| cleanup verified, no residue, evidence fresh | `allowed` |
| iSCSI, multipath, dmsetup, process, hostPath, K8s residue present | `rejected` |
| cleanup evidence missing | `unknown` |
| cleanup evidence stale or unreachable | `unknown` |
| multiple volumes with mixed state | per-volume decision; no cross-volume contamination |

Blocked decisions must include a stable reason such as:

```text
iscsi_node_records_present
iscsi_sessions_present
multipath_residue_present
dmsetup_residue_present
hostpath_residue_present
k8s_residue_present
cleanup_evidence_missing
cleanup_evidence_stale
```

## Finalizer Strategy Decision Point

Phase 41 D4 must choose one of two paths.

### Path A: lifecycle owner owns finalizers

Required before implementation:

- separate component or mode from `operator-status`,
- explicit main-object `patch swblockvolumes` permission,
- proof that only `metadata.finalizers` can be changed,
- clean and blocked delete-safety gates,
- multi-volume isolation gate,
- user docs explaining finalizer behavior.

Allowed mutation:

```text
add/remove block.seaweedfs.com/swblockvolume-protection finalizer
```

Forbidden mutation:

```text
spec changes
storage/workload/host cleanup
PVC/PV/Deployment/StorageClass mutation
data deletion
repair/rebuild/failback/backup/restore/promotion
```

### Path B: defer finalizers

Required if chosen:

- keep `operator-status` status/events-only,
- keep delete-safety as a status fact and user-visible decision,
- document that Kubernetes deletion is not protected by Seaweed Block finalizers
  in this release,
- name the future lifecycle owner that will own finalizer mutation.

Path B is acceptable if Path A cannot prove the mutation boundary in Phase 41.
Shipping a weak mutating controller is not acceptable.

## User-Facing Impact

With the v0.4 status-only foundation:

- users can see whether a volume is safe or unsafe to release,
- users can see cleanup residue and scripted verification hints,
- users do not get automatic lifecycle protection from Seaweed Block finalizers.

With a future lifecycle-owner finalizer:

- a `SwBlockVolume` delete can be held while delete-safety is blocked or
  unknown,
- a clean volume can release the finalizer,
- the system still does not automatically clean host/storage residue unless a
  separate cleanup executor is implemented and gated.

## Required Gates

Before any lifecycle mutation ships:

```text
1. real CRD schema + RBAC harness passes
2. observer status-only RBAC remains unchanged
3. lifecycle-owner RBAC is minimal and separately tested
4. forbidden spec/storage/workload mutations fail
5. blocked delete-safety rejects release
6. clean delete-safety permits release
7. stale or missing evidence returns unknown, not allowed
8. multi-volume isolation holds
9. report/dashboard/operator-snapshot/CRD/Events agree
10. cleanup verifier reports zero residue after the gate
```

## Non-Claims

This contract does not implement:

- a mutating operator,
- finalizer add/remove,
- automatic cleanup,
- repair, rebuild, failback, backup, restore, promotion, or fencing,
- NVMe ANA parity,
- production SLOs.

It defines the boundary that those features must use.
