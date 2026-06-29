# Phase 82 Finished Plan: Failback Executor gRPC Runtime

Status: complete.

## Problem

Phase 81 added the disabled-by-default blockmaster FailbackService RPC. The
failback executor still knew only fake/in-process runtimes and the older HTTP
runtime test seam. Phase 82 adds the real gRPC client transport the executor can
use to call blockmaster.

## Implementation

Added:

```text
core/ops.GRPCFailbackRuntime
core/ops.NewGRPCFailbackRuntime
```

The runtime calls:

```text
control.FailbackService.ExecuteFailback
```

and maps all authority evidence fields in both directions.

Added CLI flag:

```text
sw-block ops failback-executor --failback-runtime-grpc-addr <addr>
```

Guardrails:

```text
gRPC runtime address requires --enable-execution
execution still requires --execution-policy
HTTP and gRPC runtime transports are mutually exclusive
```

## Gate

Added:

```text
scripts/run-phase82-failback-executor-grpc-runtime-gate.sh
testops/scenarios/failback-executor-grpc-runtime-chain.yaml
```

The gate checks:

```text
gRPC runtime calls FailbackService
request fields are mapped
response terminal evidence is mapped
CLI gRPC runtime writes failed_back status
gRPC runtime requires --enable-execution
HTTP and gRPC transports are mutually exclusive
HTTP runtime still works
frontend publication remains false
storage mutation remains false
```

## Verification

```text
go test ./core/ops -run "TestGRPCFailbackRuntime" -count=1 -v
go test ./cmd/sw-block -run "TestOpsFailbackExecutor(GRPCRuntimeWritesFailedBackStatus|RejectsGRPCRuntimeWithoutEnable|RejectsAmbiguousRuntimeTransports|RuntimeURLWritesFailedBackStatus)" -count=1 -v
go test ./core/authority ./core/ops ./core/host/master ./cmd/blockmaster ./cmd/sw-block
helm lint charts/seaweed-block
"C:\Program Files\Git\bin\bash.exe" scripts/run-phase82-failback-executor-grpc-runtime-gate.sh .
C:\work\swblock.exe validate testops\scenarios\failback-executor-grpc-runtime-chain.yaml
git diff --check
```

Terminal evidence:

```text
phase82_failback_executor_grpc_runtime_status=ok
core_ops_grpc_failback_runtime_tests=pass
cmd_failback_grpc_runtime_tests=pass
grpc_runtime_calls_failback_service=true
grpc_runtime_requires_address=true
cmd_grpc_runtime_writes_failed_back_status=true
cmd_grpc_runtime_requires_enable=true
cmd_rejects_ambiguous_runtime_transports=true
cmd_http_runtime_still_supported=true
grpc_runtime_request_fields_mapped=true
grpc_runtime_response_fields_mapped=true
execution_policy_still_required=true
http_grpc_runtime_mutually_exclusive=true
authority_mutation_allowed_only_with_execution_policy=true
frontend_publication_allowed=false
storage_mutation_allowed=false
```

## Non-Claims

Phase 82 does not implement:

```text
chart-enabled failback executor gRPC address
automatic failback from the deployed controller loop
default-enabled failback RPC
blockvolume frontend switching
frontend publication after failback
storage rebuild/catch-up traffic
workload mutation
NVMe ANA behavior
```

## Next

Add disabled-by-default Helm wiring for the gRPC runtime address, then run an
integrated blockmaster + failback executor smoke with both sides explicitly
enabled.
