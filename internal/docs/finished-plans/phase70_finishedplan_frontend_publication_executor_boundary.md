# Phase 70 Finished Plan: Frontend Publication Executor Boundary

Status: complete.

## Problem

Phase 69 created the typed frontend publication target object, but no executor
owned that object yet. The next safe step is not to publish frontend targets; it
is to prove the executor's Kubernetes boundary first.

Without this layer, a future real publication implementation would have to add
business logic, RBAC, status, and mutation behavior at the same time. That is
exactly the kind of operation-layer shortcut that caused earlier live-only
CRD/RBAC bugs.

## Implementation

Phase 70 adds:

```text
FrontendPublicationExecutorReconciler
sw-block ops frontend-publication-executor
frontendPublicationExecutor Helm packaging
```

The executor reads `SwBlockFrontendPublication` targets and writes status:

```text
state=blocked
reasonCode=frontend_publication_policy_disabled
publicationMutationAllowed=false
frontendPublished=false
failbackStarted=false
noStorageMutation=true
```

Invalid targets are blocked with `missing_required_facts`; they are never
executed.

## Safety Boundary

The executor only writes:

```text
swblockfrontendpublications/status
```

It does not write:

```text
SwBlockFrontendPublication spec
SwBlockFrontendPublication finalizers
SwBlockFrontendPublication create/delete
SwBlockVolume
SwBlockReplicaEligibility
Events
Pods/PVCs/PVs/StorageClasses
host storage state
```

## Verification

Local:

```text
go test ./core/ops ./core/transport ./core/replication ./core/host/volume ./cmd/blockvolume ./cmd/sw-block
C:\work\swblock.exe validate testops\scenarios\frontend-publication-executor-boundary-chain.yaml
```

Runner gate:

```text
20260625-104138-40b7 frontend-publication-executor-boundary-chain PASS 18/18
```

Key evidence:

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

Phase 71 can attempt the first real frontend publication mutation only after
the admission/RBAC envelope is made explicit and tested live. Failback remains
separate.
