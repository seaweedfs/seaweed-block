# Current Plan: Phase 86 Failback gRPC Runtime Endpoint Decoupling

Status: complete.

## Goal

Phase 86 removes a stale coupling between the new gRPC failback runtime and the
legacy per-target HTTP `runtimeEndpoint` field.

Before this phase, an executable failback target required:

```text
spec.runtimeEndpoint != ""
```

even when the executor was configured with:

```text
--failback-runtime-grpc-addr <blockmaster>
```

That made gRPC execution depend on an unrelated HTTP endpoint placeholder.

## Deliverables

### D1: Runtime-Aware Target Validation

Changed:

```text
failbackExecutorExecutableTarget(target)
```

to:

```text
failbackExecutorExecutableTarget(target, runtimeProvided)
```

The target now requires `runtimeEndpoint` only when the executor has no explicit
runtime and must fall back to target-provided HTTP runtime.

### D2: gRPC Override Test

Added:

```text
TestFailbackExecutorGRPCRuntimeDoesNotRequireTargetRuntimeEndpoint
```

This proves:

```text
explicit runtime provided
target runtimeEndpoint empty
runtime is called
failed_back status is written
```

### D3: Real Master Carry-Forward

Updated:

```text
TestFailbackExecutorGRPCRuntimeUsesRealMasterService
```

The test now omits `RuntimeEndpoint` from the target and still proves:

```text
executor -> gRPC runtime -> real blockmaster service -> Publisher
```

### D4: Gate

Added:

```text
scripts/run-phase86-failback-grpc-runtime-endpoint-decoupling-gate.sh
testops/scenarios/failback-grpc-runtime-endpoint-decoupling-chain.yaml
```

## Non-Claims

Phase 86 does not implement:

```text
Kubernetes live failback through deployed pods
automatic failback target selection
frontend publication after failback
storage rebuild/catch-up traffic
workload mutation
NVMe ANA behavior
```

## Verification

```text
go test ./core/ops -run "TestFailbackExecutor(GRPCRuntimeDoesNotRequireTargetRuntimeEndpoint|ExecutionInvalidTargetDoesNotCallRuntime|InvokesRuntimeWhenExplicitlyEnabled)" -count=1 -v
go test ./core/host/master -run TestFailbackExecutorGRPCRuntimeUsesRealMasterService -count=1 -v
"C:\Program Files\Git\bin\bash.exe" scripts/run-phase86-failback-grpc-runtime-endpoint-decoupling-gate.sh .
C:\work\swblock.exe validate testops\scenarios\failback-grpc-runtime-endpoint-decoupling-chain.yaml
```

Terminal evidence:

```text
phase86_failback_grpc_runtime_endpoint_decoupling_status=ok
core_ops_failback_grpc_endpoint_decoupling_tests=pass
core_host_master_failback_grpc_no_endpoint_test=pass
grpc_runtime_does_not_require_target_runtime_endpoint=true
invalid_target_still_blocks_without_runtime_call=true
http_runtime_endpoint_fallback_still_supported=true
real_master_grpc_service_without_target_endpoint=true
explicit_grpc_runtime_is_sufficient=true
legacy_http_runtime_endpoint_still_supported=true
invalid_target_writes_blocked_status=true
master_publisher_epoch_advanced=true
frontend_publication_allowed=false
storage_mutation_allowed=false
```

## Next

The next phase should validate a Kubernetes-deployed failback executor smoke
when a lab image is available, or close the failback operation-layer milestone
with a release-note/readme update that accurately states what is automatic and
what remains opt-in.
