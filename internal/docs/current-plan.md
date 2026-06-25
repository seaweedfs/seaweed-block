# Current Plan: Phase 71 Frontend Publication Live API Boundary

Status: complete.

## Goal

Phase 70 added the frontend publication executor as a status-only controller:

```text
SwBlockFrontendPublication.status
```

Phase 71 closes the live Kubernetes API gap for that boundary. The gate proves
against a real apiserver that the executor may patch only the
`SwBlockFrontendPublication` status subresource and cannot mutate:

```text
target spec
target metadata
target finalizers
SwBlockVolume
SwBlockReplicaEligibility
workloads
PVC/PV
StorageClass
Secrets
Nodes / CSIDrivers / CSINodes
```

This is intentionally not frontend publication execution. It is the live API
proof required before we grant any broader authority in a later phase.

## Delivered

### D1: Live API Gate

Added:

```text
scripts/run-phase71-frontend-publication-live-api-boundary-gate.sh
```

The gate installs the `SwBlockFrontendPublication` CRD, creates a scoped
executor service account and RBAC, creates one target object, then verifies:

```text
patch swblockfrontendpublications/status = allowed
patch swblockfrontendpublications main object = denied
create/update/delete swblockfrontendpublications = denied
patch finalizers endpoint = denied
events/workload/storage mutation = denied
```

It writes a disabled/blocked status through the real status subresource and
checks that spec/labels/annotations remain unchanged.

### D2: Runner Scenario

Added:

```text
testops/scenarios/frontend-publication-live-api-boundary-chain.yaml
```

The scenario asserts the terminal evidence:

```text
phase71_frontend_publication_live_api_boundary_status=ok
frontend_publication_executor_status_writes=true
frontend_publication_executor_status=blocked
frontend_publication_executor_reason=frontend_publication_policy_disabled
frontend_publication_executor_rbac_status_only=true
frontend_publication_mutation_allowed=false
frontend_published=false
failback_started=false
storage_mutation_allowed=false
target_object_integrity_preserved=true
```

## Non-Claims

Phase 71 does not claim:

```text
frontend publication execution
frontend publish target update
primary authority change
failback
storage/workload mutation
NVMe ANA behavior
```

## Verification

```text
C:\work\swblock.exe validate testops\scenarios\frontend-publication-live-api-boundary-chain.yaml
20260625-112540-ca98 frontend-publication-live-api-boundary-chain PASS 32/32
```

Terminal evidence:

```text
phase71_frontend_publication_live_api_boundary_status=ok
executor_status_patch_succeeded=true
frontend_publication_executor_status_writes=true
frontend_publication_executor_status=blocked
frontend_publication_executor_reason=frontend_publication_policy_disabled
frontend_publication_executor_status_mutation_allowed=true
frontend_publication_mutation_allowed=false
frontend_published=false
failback_started=false
frontend_publication_attempts=0
failback_attempts=0
storage_mutation_allowed=false
frontend_publication_executor_rbac_status_only=true
executor_spec_patch_allowed=false
executor_label_patch_allowed=false
executor_finalizers_endpoint_allowed=false
target_object_integrity_preserved=true
```

## Next

If Phase 71 passes live QA, Phase 72 may introduce the first opt-in frontend
publication mutation. That phase must define exactly which product-owned field
or runtime endpoint changes and must include admission/RBAC and multi-volume
isolation gates before any failback claim.
