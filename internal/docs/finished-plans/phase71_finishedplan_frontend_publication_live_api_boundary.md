# Phase 71 Finished Plan: Frontend Publication Live API Boundary

## Result

Complete.

Phase 71 added the live Kubernetes API/RBAC proof for the Phase 70 frontend
publication executor. It does not add frontend publication execution. It proves
the executor identity can write only `SwBlockFrontendPublication.status` and
cannot mutate target spec, target metadata, target finalizers, workload/storage
resources, frontend publication, or failback state.

## Delivered

### Live Gate Script

```text
scripts/run-phase71-frontend-publication-live-api-boundary-gate.sh
```

The gate creates a temporary namespace, service account, RBAC, and one
`SwBlockFrontendPublication` target, then verifies against the real Kubernetes
API:

```text
patch swblockfrontendpublications/status = allowed
patch swblockfrontendpublications main object = denied
create/update/delete swblockfrontendpublications = denied
patch swblockfrontendpublications/finalizers = denied
events/workload/storage mutation = denied
```

It also writes a disabled/blocked status through the real status subresource
and verifies the target spec, labels, and annotations remain unchanged.

### Runner Scenario

```text
testops/scenarios/frontend-publication-live-api-boundary-chain.yaml
```

The scenario runs the gate on `m02` and asserts terminal evidence for status
write success, RBAC status-only scope, no frontend publication, no failback, and
object integrity preservation.

## Verification

Local:

```text
C:\work\swblock.exe validate testops\scenarios\frontend-publication-live-api-boundary-chain.yaml
bash -n scripts/run-phase71-frontend-publication-live-api-boundary-gate.sh
git diff --check
```

Runner:

```text
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

## Next

Phase 72 may introduce the first opt-in frontend publication mutation. It must
define exactly which product-owned field or runtime endpoint changes, keep the
operation disabled by default, and include admission/RBAC plus multi-volume
isolation gates before any failback claim.
