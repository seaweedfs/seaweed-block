# Phase 79 Finished Plan: Failback Authority Call-site

Status: complete.

## Problem

Phase 78 added a product-owned authority failback seam, but it was not yet
reachable from the failback executor call-site. The executor could call fake or
HTTP runtimes, but not the in-process authority seam that mints
`IntentReassign`.

## Implementation

Added an adapter:

```text
core/ops.AuthorityFailbackRuntime
core/ops.NewAuthorityFailbackRuntime
```

The adapter implements `ops.FailbackRuntime` and delegates to:

```text
authority.FailbackAuthorityRuntime.ExecuteFailback
```

The mapping preserves:

```text
volumeID
replicaID
targetDataAddr
targetCtrlAddr
expectedCurrentReplicaID
expectedCurrentEpoch
ackEligible
frontendFencedBeforeFailback
durableFrontierCovered
noCrossVolumeIdentityChange
evidenceRefs
```

## Behavior

The executor can now run:

```text
SwBlockReplicaFailback target
  -> FailbackExecutorReconciler
  -> AuthorityFailbackRuntime adapter
  -> authority.FailbackAuthorityRuntime
  -> Publisher.apply(IntentReassign)
  -> SwBlockReplicaFailback.status=failed_back
```

The successful test seeds authority as:

```text
r1@1 -> r2@2
```

Then the executor targets returned replica `r1` with expected current `r2@2`.
The adapter advances authority to:

```text
r1@3
```

## Failure Boundary

Stale expected-current evidence fails closed:

```text
expected r2@99
actual r2@2
```

The executor writes blocked status with runtime failure and does not claim
failback.

The existing gates remain:

```text
execution policy still required
dry-run writes no status
frontend_publication_allowed=false
storage_mutation_allowed=false
```

## Gate

Added:

```text
scripts/run-phase79-failback-authority-callsite-gate.sh
testops/scenarios/failback-authority-callsite-chain.yaml
```

The gate checks:

```text
executor invokes authority runtime adapter
publisher authority line advances
stale expected-current evidence blocks call-site
execution policy remains required
failed_back status is written after terminal evidence
runtime failure produces no false failback
frontend publication remains false
storage mutation remains false
```

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

## Next

Add a disabled-by-default product wiring gate that constructs this adapter from
a real Publisher in the component that owns authority state.
