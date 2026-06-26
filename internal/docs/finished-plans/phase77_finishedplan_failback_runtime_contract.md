# Phase 77 Finished Plan: Returned-Replica Failback Runtime Contract

Status: complete.

## Problem

Phase 76 gave `SwBlockReplicaFailback` a status-only executor. The next missing
piece was the execution envelope: what request a future failback runtime would
receive, what evidence it must return, and how the executor refuses to claim
failback when policy or terminal evidence is missing.

## Implementation

Added executable target fields to `SwBlockReplicaFailback.spec`:

```text
failbackDecision=disabled|enabled
failbackReason
failbackMutationAllowed
runtimeEndpoint
```

The Phase 75 target owner now emits explicitly disabled targets:

```text
failbackDecision=disabled
failbackReason=failback_policy_disabled
failbackMutationAllowed=false
```

Added runtime contract:

```text
FailbackRuntime
FailbackRuntimeRequest
FailbackRuntimeResult
HTTPFailbackRuntime
```

Terminal success requires:

```text
failbackStarted=true
authorityEpochAdvanced=true
singlePrimaryAfterFailback=true
publishTargetSwappedAfterFailback=true
noStorageMutation=true
noCrossVolumeIdentityChange=true
```

Added CLI policy gate:

```text
sw-block ops failback-executor --enable-execution --execution-policy --failback-runtime-url <url>
```

Default reconciliation remains disabled/status-only.

## Gate

Added:

```text
scripts/run-phase77-failback-runtime-contract-gate.sh
testops/scenarios/failback-runtime-contract-chain.yaml
```

The gate checks:

```text
default executor still disabled
execution policy blocks without enable
explicit enabled target invokes runtime
runtime failure does not claim failback
invalid terminal evidence does not claim failback
HTTP runtime request/response shape
target writer serializes runtime fields
storage mutation remains false
```

## Verification

```text
go test ./core/ops -run "TestFailbackExecutor|TestHTTPFailbackRuntime|TestFailbackTargetOwner|TestPhase75SwBlockReplicaFailbackTargetSchema|TestKubernetesStatusClientCreatesSwBlockReplicaFailbackWithoutStatus" -count=1 -v
go test ./cmd/sw-block -run "TestOpsFailbackExecutor|TestOpsFailbackTargetOwner" -count=1 -v
go test ./core/ops ./cmd/sw-block
helm lint charts/seaweed-block
"C:\Program Files\Git\bin\bash.exe" scripts/run-phase77-failback-runtime-contract-gate.sh .
C:\work\swblock.exe validate testops\scenarios\failback-runtime-contract-chain.yaml
```

Terminal evidence:

```text
phase77_failback_runtime_contract_status=ok
core_ops_failback_runtime_tests=pass
cmd_failback_runtime_tests=pass
failback_runtime_contract_schema_locked=true
failback_runtime_endpoint_field=true
failback_enabled_target_schema=true
failback_execution_policy_gate=true
failback_runtime_invoked_only_when_enabled=true
failback_runtime_failure_no_false_failback=true
failback_runtime_invalid_terminal_evidence_no_false_failback=true
failback_attempts=1
failback_started=true
authority_epoch_advanced=true
single_primary_after_failback=true
publish_target_swapped_after_failback=true
storage_mutation_allowed=false
```

## Non-Claims

Phase 77 does not implement:

```text
real blockmaster failback endpoint
real authority epoch mutation
real primary reassignment
real publish-target swap
blockvolume frontend switching
storage/workload mutation
```

## Next

Implement the product-owned failback endpoint or authority-owner seam. It must
be gated by the runtime contract and prove epoch advance, single-primary state,
publish-target swap, and cross-volume isolation before any user-facing
`failed_back` claim is allowed outside test/fake runtime evidence.
