# Phase 83 Finished Plan: Failback Runtime Chart Wiring

Status: complete.

Date: 2026-06-26.

## What Changed

Phase 83 packages the Phase 81/82 failback runtime path in Helm while keeping
default installs non-mutating.

Added Helm values:

```text
blockmaster.failbackRuntimeRPC
failbackExecutor.execution.enabled
failbackExecutor.execution.policy
failbackExecutor.execution.failbackRuntimeGrpcAddr
failbackExecutor.execution.failbackRuntimeURL
```

Default values keep the path off:

```text
blockmaster.failbackRuntimeRPC=false
failbackExecutor.create=false
failbackExecutor.dryRun=true
failbackExecutor.execution.enabled=false
failbackExecutor.execution.policy=false
```

When explicitly enabled, the chart can render:

```text
--failback-runtime-rpc
--enable-execution
--execution-policy
--failback-runtime-grpc-addr=<addr>
```

## Guardrails

Helm render fails for unsafe or incoherent configurations:

```text
execution enabled while dry-run remains true
execution enabled without policy enabled
runtime address without execution enabled
both HTTP and gRPC runtime addresses
```

The failback executor remains disabled by default and status-only unless the
operator opts into the full runtime path.

## Verification

```text
helm lint charts/seaweed-block
scripts/run-phase83-failback-chart-runtime-gate.sh .
swblock validate testops/scenarios/failback-chart-runtime-chain.yaml
```

Result:

```text
phase83_failback_chart_runtime_status=ok
helm_lint=pass
default_omits_failback_runtime_rpc=true
default_omits_failback_executor_deployment=true
enabled_renders_failback_runtime_rpc=true
enabled_renders_failback_grpc_addr=true
rejects_execution_with_dry_run=true
rejects_execution_without_policy=true
rejects_ambiguous_runtime_transports=true
chart_default_remains_non_mutating=true
frontend_publication_allowed=false
storage_mutation_allowed=false
```

## Non-Claims

Phase 83 does not claim automatic failback, frontend publication, rebuild
traffic, storage mutation, workload mutation, or NVMe ANA parity.
