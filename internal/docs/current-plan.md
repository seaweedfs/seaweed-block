# Current Plan: Phase 79 Failback Authority Call-site

Status: complete.

## Goal

Phase 78 added the authority-owned failback seam, but the failback executor did
not yet have a product call-site to invoke it. Phase 79 wires the executor's
runtime contract to that seam through an in-process adapter.

Default behavior remains disabled/status-only. The adapter is used only when
the existing explicit execution gate is satisfied:

```text
--enable-execution
--execution-policy
executable SwBlockReplicaFailback target
```

## Deliverables

### D1: Runtime Adapter

Added:

```text
core/ops.AuthorityFailbackRuntime
core/ops.NewAuthorityFailbackRuntime
```

The adapter implements the existing `ops.FailbackRuntime` interface and maps
the executor request into:

```text
authority.FailbackAuthorityRuntime.ExecuteFailback
```

### D2: Executor Call-site

The failback executor can now use the authority adapter as its runtime:

```text
FailbackExecutorReconciler{
  Runtime: NewAuthorityFailbackRuntime(publisher),
  ExecutionRequested: true,
  ExecutionPolicyEnabled: true,
}
```

The test path proves:

```text
current authority line: r2@2
target returned replica: r1
executor invokes adapter
adapter invokes authority runtime
Publisher.apply(IntentReassign) advances line to r1@3
SwBlockReplicaFailback.status.state=failed_back
```

### D3: Negative Guard

Stale expected-current evidence still fails closed:

```text
expectedCurrentReplicaID=r2
expectedCurrentEpoch=99
actual current line=r2@2
```

The executor writes blocked status and does not claim failback.

### D4: Boundary

The existing boundaries remain:

```text
execution policy is still required
dry-run writes no status
default authority_mutation_allowed=false
frontend_publication_allowed=false
storage_mutation_allowed=false
```

Only the explicit adapter call-site path can advance authority.

### D5: Gate

Added:

```text
scripts/run-phase79-failback-authority-callsite-gate.sh
testops/scenarios/failback-authority-callsite-chain.yaml
```

The gate proves:

```text
executor invokes authority runtime adapter
publisher authority line advances
stale expected-current evidence blocks the call-site
execution policy remains required
failed_back status is written only after terminal evidence
runtime failure produces no false failback
frontend publication remains false
storage mutation remains false
```

## Non-Claims

Phase 79 does not implement:

```text
automatic failback from the deployed controller loop
blockmaster HTTP/gRPC failback endpoint
blockvolume frontend switching
frontend publication after failback
storage rebuild/catch-up traffic
workload mutation
NVMe ANA behavior
```

It wires the product call-site in-process for controlled execution. The deployed
default remains disabled/status-only.

## Verification

```text
go test ./core/ops -run "TestFailbackExecutorUsesAuthorityRuntimeAdapter|TestFailbackAuthorityRuntimeAdapterRejectsStaleExpectedCurrent|TestFailbackExecutorExecutionPolicyBlocks|TestFailbackExecutorDryRunDoesNotWriteStatus" -count=1 -v
go test ./core/authority ./core/ops ./cmd/sw-block
helm lint charts/seaweed-block
"C:\Program Files\Git\bin\bash.exe" scripts/run-phase79-failback-authority-callsite-gate.sh .
C:\work\swblock.exe validate testops\scenarios\failback-authority-callsite-chain.yaml
git diff --check
```

Terminal evidence:

```text
phase79_failback_authority_callsite_status=ok
core_ops_failback_authority_callsite_tests=pass
authority_runtime_adapter_invoked_by_executor=true
stale_expected_current_blocks_callsite=true
execution_policy_still_required=true
dry_run_no_status_write=true
publisher_authority_line_advanced=true
authority_epoch_advanced=true
single_primary_after_failback=true
publish_target_swapped_after_failback=true
failed_back_status_written=true
runtime_failure_no_false_failback=true
authority_mutation_allowed_only_with_execution_policy=true
frontend_publication_allowed=false
storage_mutation_allowed=false
```

## Next

The next phase should decide how this in-process call-site becomes reachable in
the product. The safest next increment is a disabled-by-default CLI or
controller wiring gate that can construct the adapter from a real Publisher
without enabling automatic failback.
