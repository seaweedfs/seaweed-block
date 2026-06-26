# Current Plan: Phase 83 Failback Runtime Chart Wiring

Status: complete.

## Goal

Phase 82 added the failback executor gRPC transport. Phase 83 packages that
transport in the Helm chart without enabling it by default.

Default installs remain non-mutating:

```text
blockmaster.failbackRuntimeRPC=false
failbackExecutor.create=false
failbackExecutor.execution.enabled=false
```

The chart only renders the active failback RPC/executor path when the operator
sets all execution switches explicitly.

## Deliverables

### D1: Blockmaster RPC Flag

Added:

```text
blockmaster.failbackRuntimeRPC
```

When true, the blockmaster Deployment renders:

```text
--failback-runtime-rpc
```

The value defaults to false.

### D2: Failback Executor Runtime Values

Added:

```text
failbackExecutor.execution.enabled
failbackExecutor.execution.policy
failbackExecutor.execution.failbackRuntimeGrpcAddr
failbackExecutor.execution.failbackRuntimeURL
```

When explicitly enabled with `dryRun=false`, the failback executor can render:

```text
--enable-execution
--execution-policy
--failback-runtime-grpc-addr=<addr>
```

HTTP runtime remains available through:

```text
--failback-runtime-url=<url>
```

### D3: Render Guardrails

Helm now fails fast for incoherent execution values:

```text
execution.enabled=true with dryRun=true
execution.enabled=true without execution.policy=true
runtime address without execution.enabled=true
both HTTP and gRPC runtime addresses
```

This keeps chart behavior aligned with the CLI contract and avoids deployed
pods that immediately reject their own flags.

### D4: Gate

Added:

```text
scripts/run-phase83-failback-chart-runtime-gate.sh
testops/scenarios/failback-chart-runtime-chain.yaml
```

The gate proves:

```text
default chart omits --failback-runtime-rpc
default chart does not create failback executor
default chart omits --enable-execution and runtime address
explicit chart renders blockmaster RPC flag
explicit chart renders failback executor execution policy and gRPC address
dry-run execution is rejected
missing execution policy is rejected
ambiguous HTTP/gRPC runtime is rejected
```

## Non-Claims

Phase 83 does not implement:

```text
automatic failback from the deployed controller loop
default-enabled failback RPC
live end-to-end failback through Helm install
frontend publication after failback
storage rebuild/catch-up traffic
workload mutation
NVMe ANA behavior
```

## Verification

```text
helm lint charts/seaweed-block
"C:\Program Files\Git\bin\bash.exe" scripts/run-phase83-failback-chart-runtime-gate.sh .
C:\work\swblock.exe validate testops\scenarios\failback-chart-runtime-chain.yaml
```

Terminal evidence:

```text
phase83_failback_chart_runtime_status=ok
helm_lint=pass
default_omits_failback_runtime_rpc=true
default_omits_failback_executor_deployment=true
default_omits_enable_execution=true
default_omits_failback_grpc_addr=true
enabled_renders_failback_runtime_rpc=true
enabled_renders_failback_executor_deployment=true
enabled_renders_enable_execution=true
enabled_renders_execution_policy=true
enabled_renders_failback_grpc_addr=true
enabled_omits_dry_run=true
rejects_execution_with_dry_run=true
rejects_execution_without_policy=true
rejects_ambiguous_runtime_transports=true
execution_policy_still_required=true
runtime_transport_must_be_unambiguous=true
chart_default_remains_non_mutating=true
frontend_publication_allowed=false
storage_mutation_allowed=false
```

## Next

The next phase should run an integrated local blockmaster + failback-executor
smoke with all flags explicitly enabled, or add the missing release/README
documentation for the opt-in failback path before any public release claim.
