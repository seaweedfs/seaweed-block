# Current Plan: Phase 72 Frontend Publication Runtime Contract

Status: complete.

## Goal

Phase 71 proved the live Kubernetes API/RBAC boundary for the frontend
publication executor. Phase 72 adds the next code seam: a typed frontend
publication runtime contract and an explicit executor policy gate.

This phase still does not wire a real blockmaster or blockvolume frontend
publication endpoint. The purpose is to prevent the next implementation from
becoming a fake status-only publish.

## Delivered

### D1: Runtime Contract

Added:

```text
FrontendPublicationRuntime
FrontendPublicationRuntimeRequest
FrontendPublicationRuntimeResult
HTTPFrontendPublicationRuntime
```

The HTTP client posts JSON to an explicit runtime endpoint and decodes:

```text
frontendPublished
failbackStarted
noStorageMutation
noCrossVolumeIdentityChange
evidenceRefs
```

### D2: Explicit Executor Gate

`FrontendPublicationExecutorReconciler` now supports an opt-in execution path:

```text
ExecutionRequested=true
ExecutionPolicyEnabled=true
```

Without both flags, execution is blocked. Default reconciliation remains the
Phase 70/71 status-only disabled path.

The runtime path requires an explicit target shape:

```text
frontendPublicationDecision=enabled
frontendPublicationMutationAllowed=true
runtimeEndpoint=<non-empty>
ackEligibilityKnown=true
ackEligible=true
frontendFencedAfterExecution=true
primaryUnchanged=true
durableFrontierCovered=true
noCrossVolumeIdentityChange=true
```

### D3: Schema Contract

`SwBlockFrontendPublication.spec` now admits the future execution contract:

```text
frontendPublicationDecision=enabled
runtimeEndpoint
```

The existing target owner still creates only disabled targets, so default
product behavior does not change.

### D4: Gate

Added:

```text
scripts/run-phase72-frontend-publication-runtime-contract-gate.sh
testops/scenarios/frontend-publication-runtime-contract-chain.yaml
```

The gate proves:

```text
default executor remains disabled
execution policy blocks when not explicitly enabled
enabled target invokes runtime exactly through the typed contract
runtime failure does not claim frontendPublished
HTTP runtime errors surface
failback remains false
storage mutation remains false
```

## Non-Claims

Phase 72 does not claim:

```text
real frontend publication endpoint exists
blockmaster publish target update
blockvolume runtime frontend switch
primary authority change
failback execution
storage/workload mutation
NVMe ANA behavior
```

## Verification

```text
go test ./core/ops ./cmd/sw-block
C:\work\swblock.exe validate testops\scenarios\frontend-publication-runtime-contract-chain.yaml
20260625-153846-1bf7 frontend-publication-runtime-contract-chain PASS 24/24
```

Terminal evidence:

```text
phase72_frontend_publication_runtime_contract_status=ok
core_ops_frontend_publication_runtime_tests=pass
frontend_publication_runtime_contract_schema_locked=true
frontend_publication_runtime_endpoint_field=true
frontend_publication_execution_policy_gate=true
frontend_publication_runtime_invoked_only_when_enabled=true
frontend_publication_runtime_failure_no_false_publish=true
frontend_publication_runtime_invalid_terminal_evidence_no_false_publish=true
frontend_publication_attempts=1
frontend_published=true
failback_started=false
storage_mutation_allowed=false
```

## Next

Phase 73 should implement the real runtime endpoint owner. The key design
decision is still open:

```text
blockmaster authority endpoint vs blockvolume runtime endpoint
```

Do not mark a target as published until a real endpoint performs a real
product-owned side effect and returns terminal evidence.
