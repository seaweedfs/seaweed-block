# Phase 72 Finished Plan: Frontend Publication Runtime Contract

## Result

Complete.

Phase 72 added a typed runtime contract for the future frontend publication
side effect. It does not add a real blockmaster or blockvolume publish endpoint
and does not change default runtime behavior.

## Delivered

### Runtime Contract

Added:

```text
FrontendPublicationRuntime
FrontendPublicationRuntimeRequest
FrontendPublicationRuntimeResult
HTTPFrontendPublicationRuntime
```

The runtime request carries target identity and the terminal evidence required
before publication may be attempted:

```text
volumeName
volumeID
pvcName
replicaID
runtimeEndpoint
ackEligibilityKnown
ackEligible
frontendFencedAfterExecution
primaryUnchanged
durableFrontierCovered
noCrossVolumeIdentityChange
```

The runtime result must return terminal evidence:

```text
frontendPublished=true
failbackStarted=false
noStorageMutation=true
noCrossVolumeIdentityChange=true
```

If the runtime returns anything weaker, the executor writes a blocked status
and does not claim publication.

### Executor Gate

The executor now has an explicit opt-in execution path:

```text
ExecutionRequested=true
ExecutionPolicyEnabled=true
```

Execution remains blocked unless both flags are set. Default reconciliation is
unchanged from Phase 70/71: status-only blocked/disabled.

### Schema

`SwBlockFrontendPublication.spec` now admits:

```text
frontendPublicationDecision=enabled
runtimeEndpoint
```

The target owner still generates only disabled targets, so no default target is
executable.

### Gate

Added:

```text
scripts/run-phase72-frontend-publication-runtime-contract-gate.sh
testops/scenarios/frontend-publication-runtime-contract-chain.yaml
```

## Verification

Local:

```text
go test ./core/ops ./cmd/sw-block
C:\work\swblock.exe validate testops\scenarios\frontend-publication-runtime-contract-chain.yaml
bash -n scripts/run-phase72-frontend-publication-runtime-contract-gate.sh
git diff --check
```

Runner:

```text
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

## Next

Phase 73 should implement the real runtime endpoint owner. Do not mark any
target as published until a real endpoint performs a real product-owned side
effect and returns terminal evidence.
