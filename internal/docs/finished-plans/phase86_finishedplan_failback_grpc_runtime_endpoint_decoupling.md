# Phase 86 Finished Plan: Failback gRPC Runtime Endpoint Decoupling

Status: complete.

Date: 2026-06-26.

## What Changed

Phase 86 decouples explicit gRPC failback runtime execution from the older
target-local HTTP `runtimeEndpoint` field.

The executor now treats a target as executable when either:

```text
an explicit runtime is supplied by the executor process
```

or:

```text
the target contains runtimeEndpoint for HTTP fallback
```

All other execution facts are still required.

## Evidence

The gate proves:

```text
gRPC runtime does not require target runtimeEndpoint
legacy HTTP runtimeEndpoint fallback still works
invalid targets still block without runtime calls
real blockmaster gRPC service works without target runtimeEndpoint
frontend publication remains false
storage mutation remains false
```

## Verification

```text
go test ./core/ops -run "TestFailbackExecutor(GRPCRuntimeDoesNotRequireTargetRuntimeEndpoint|ExecutionInvalidTargetDoesNotCallRuntime|InvokesRuntimeWhenExplicitlyEnabled)" -count=1 -v
go test ./core/host/master -run TestFailbackExecutorGRPCRuntimeUsesRealMasterService -count=1 -v
scripts/run-phase86-failback-grpc-runtime-endpoint-decoupling-gate.sh .
swblock validate testops/scenarios/failback-grpc-runtime-endpoint-decoupling-chain.yaml
```

Result:

```text
phase86_failback_grpc_runtime_endpoint_decoupling_status=ok
explicit_grpc_runtime_is_sufficient=true
legacy_http_runtime_endpoint_still_supported=true
```

## Non-Claims

Phase 86 does not claim Kubernetes live failback, automatic target selection,
frontend publication, storage traffic, workload mutation, or NVMe ANA behavior.
