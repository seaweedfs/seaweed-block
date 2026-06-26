# Current Plan: Phase 85 Failback Executor Policy Safety

Status: complete.

## Goal

Phase 85 proves that deployed failback execution flags are not enough to cause
authority mutation. The executor must also see a valid executable
`SwBlockReplicaFailback` target with all required facts.

This protects the deployed-loop shape introduced by Phases 81-84:

```text
execution flags enabled + no target        -> no runtime call
execution flags enabled + invalid target   -> blocked status, no runtime call
execution flags enabled + valid target     -> runtime may be called
```

## Deliverables

### D1: No-Target Execution Safety

Added:

```text
TestFailbackExecutorExecutionNoTargetsDoesNotAttemptRuntime
```

This proves:

```text
TargetCount=0
FailbackAttempts=0
StatusWriteCount=0
runtime requests=0
authority_mutation_allowed=false
frontend_publication_allowed=false
storage_mutation_allowed=false
```

### D2: Invalid-Target Execution Safety

Added:

```text
TestFailbackExecutorExecutionInvalidTargetDoesNotCallRuntime
```

This proves a malformed executable target writes blocked status with:

```text
reason=failback_runtime_target_missing
FailbackAttempts=0
runtime requests=0
```

### D3: Positive Control

The gate keeps the valid-target success test in scope:

```text
TestFailbackExecutorInvokesRuntimeWhenExplicitlyEnabled
```

That proves the new safety checks do not break the explicit valid target path.

### D4: Gate

Added:

```text
scripts/run-phase85-failback-executor-policy-safety-gate.sh
testops/scenarios/failback-executor-policy-safety-chain.yaml
```

## Non-Claims

Phase 85 does not implement:

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
go test ./core/ops -run "TestFailbackExecutor(ExecutionPolicyBlocks|ExecutionNoTargetsDoesNotAttemptRuntime|ExecutionInvalidTargetDoesNotCallRuntime|InvokesRuntimeWhenExplicitlyEnabled)" -count=1 -v
"C:\Program Files\Git\bin\bash.exe" scripts/run-phase85-failback-executor-policy-safety-gate.sh .
C:\work\swblock.exe validate testops\scenarios\failback-executor-policy-safety-chain.yaml
```

Terminal evidence:

```text
phase85_failback_executor_policy_safety_status=ok
core_ops_failback_policy_safety_tests=pass
policy_disabled_blocks_execution=true
no_targets_no_runtime_call=true
invalid_target_no_runtime_call=true
valid_target_runtime_call_still_supported=true
execution_flags_alone_insufficient=true
runtime_requires_valid_target=true
invalid_target_writes_blocked_status=true
authority_mutation_allowed_only_for_valid_target=true
frontend_publication_allowed=false
storage_mutation_allowed=false
```

## Next

The next phase can move to a Kubernetes-deployed smoke for the failback
components, or add a release documentation update that describes the failback
runtime as opt-in and not automatic.
