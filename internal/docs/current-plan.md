# Current Plan: Phase 70 Frontend Publication Executor Boundary

Status: complete.

## Goal

Phase 69 introduced the typed frontend publication target:

```text
SwBlockFrontendPublication
```

Phase 70 adds the executor boundary for that target without performing frontend
publication. The executor can only write `SwBlockFrontendPublication.status`
with a disabled/blocked result.

This keeps the operation model layered:

```text
ACK eligibility -> frontend publication target -> frontend publication executor status
```

The real frontend publish mutation and failback remain outside this phase.

## Delivered

### D1: Executor Reconciler

Added:

```text
FrontendPublicationExecutorReconciler
sw-block ops frontend-publication-executor
```

For each `SwBlockFrontendPublication` target, the executor writes:

```text
state=blocked
reasonCode=frontend_publication_policy_disabled
publicationMutationAllowed=false
frontendPublished=false
failbackStarted=false
noStorageMutation=true
```

Invalid targets are also blocked rather than executed.

### D2: Kubernetes Status Writer

`KubernetesStatusClient` now supports:

```text
WriteFrontendPublicationStatus
```

It patches only:

```text
/apis/block.seaweedfs.com/v1alpha1/namespaces/<ns>/swblockfrontendpublications/<name>/status
```

The writer test verifies camelCase status payloads and forbids spec patches.

### D3: Packaging And RBAC

Added disabled-by-default Helm packaging:

```text
frontendPublicationExecutor.create=false
frontendPublicationExecutor.dryRun=true
```

RBAC permits only:

```text
get/list/watch swblockfrontendpublications
get/update/patch swblockfrontendpublications/status
```

It does not grant target create, spec patch, finalizer, Event, workload,
PVC/PV, StorageClass, Secret, or delete permissions.

### D4: Gate

Gate files:

```text
scripts/run-phase70-frontend-publication-executor-boundary-gate.sh
testops/scenarios/frontend-publication-executor-boundary-chain.yaml
```

The gate proves:

```text
frontend publication executor status writes are wired
status is blocked / frontend_publication_policy_disabled
frontendPublished=false
failbackStarted=false
frontend publication attempts remain 0
failback attempts remain 0
storage mutation remains false
RBAC is status-only
```

## Non-Claims

Phase 70 does not claim:

```text
frontend publication execution
frontend publish target update
primary authority change
failback
storage/workload mutation
NVMe ANA behavior
```

## Verification

Local:

```text
go test ./core/ops ./core/transport ./core/replication ./core/host/volume ./cmd/blockvolume ./cmd/sw-block
C:\work\swblock.exe validate testops\scenarios\frontend-publication-executor-boundary-chain.yaml
```

Live runner:

```text
20260625-104138-40b7 frontend-publication-executor-boundary-chain PASS 18/18
```

Terminal evidence:

```text
phase70_frontend_publication_executor_boundary_status=ok
frontend_publication_executor_status_writes=true
frontend_publication_executor_status=blocked
frontend_publication_executor_reason=frontend_publication_policy_disabled
frontend_publication_executor_rbac_status_only=true
frontend_publication_attempts=0
frontend_published=false
failback_attempts=0
failback_started=false
storage_mutation_allowed=false
```

## Next

Phase 71 should be the first real frontend publication mutation gate if we want
to continue this operation line before NVMe:

```text
frontend publication execution under admission/RBAC + multi-volume isolation
```

It must still avoid broad failback semantics until frontend publication itself
is proven.
