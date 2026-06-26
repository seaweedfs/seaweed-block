# Phase 78 Finished Plan: Failback Authority Runtime Seam

Status: complete.

## Problem

Phase 77 gave returned-replica failback a typed runtime request/response
contract, but the only successful runtime evidence came from fake/HTTP test
runtimes. That proved the executor envelope, not the product-owned authority
mutation.

Phase 78 adds the first authority-owned failback seam while keeping default
behavior disabled.

## Implementation

Added:

```text
core/authority.FailbackAuthorityRuntime
core/authority.FailbackRuntimeRequest
core/authority.FailbackRuntimeResult
```

The runtime validates:

```text
volumeID
replicaID
targetDataAddr
targetCtrlAddr
expectedCurrentReplicaID
expectedCurrentEpoch
ackEligible=true
frontendFencedBeforeFailback=true
durableFrontierCovered=true
noCrossVolumeIdentityChange=true
```

It rejects stale expected-current evidence before changing authority state.
When valid, it calls:

```text
Publisher.apply(AssignmentAsk{Intent: IntentReassign})
```

Terminal success requires:

```text
authorityEpochAdvanced=true
singlePrimaryAfterFailback=true
publishTargetSwappedAfterFailback=true
noStorageMutation=true
noCrossVolumeIdentityChange=true
```

## Endpoint Facts

Returned-replica endpoint facts are now preserved through the managed-volume
pipeline:

```text
VolumeInventoryReplicaInput
  -> ReplicaEvidence
  -> ReplicaFact
  -> ReturnedReplicaProjection
  -> SwBlockVolume.status.replicaReintegrations[]
  -> SwBlockReplicaFailback.spec
```

The failback target spec now includes:

```text
targetDataAddr
targetCtrlAddr
expectedCurrentReplicaID
expectedCurrentEpoch
```

The target owner still creates disabled targets by default; it does not fill the
expected-current fields or enable execution.

## Boundary

Default/dry-run behavior remains non-mutating:

```text
authority_mutation_allowed=false
storage_mutation_allowed=false
frontend_publication_allowed=false
```

Only an explicitly executable runtime target with execution policy enabled can
surface:

```text
authority_mutation_allowed=true
```

This authority mutation is limited to reassignment minted by `Publisher.apply`.
It still does not publish a frontend, mutate storage, or change workloads.

## Gate

Added:

```text
scripts/run-phase78-failback-authority-runtime-gate.sh
testops/scenarios/failback-authority-runtime-chain.yaml
```

The gate checks:

```text
authority failback reassign is minted through Publisher.apply
stale expected-current evidence is rejected
terminal preconditions are required
target endpoint fields are preserved
expected-current fields are required for executable targets
explicit runtime success is the only authority-mutating path
storage mutation remains false
frontend publication remains false
```

## Verification

```text
go test ./core/authority -run "TestFailbackAuthorityRuntime" -count=1 -v
go test ./core/ops -run "TestFailbackExecutor|TestFailbackTargetOwner|TestHTTPFailbackRuntime|TestPhase46D2SwBlockVolumeReturnedReplicaSchema|TestPhase75SwBlockReplicaFailbackTargetSchema|TestKubernetesStatusClientCreatesSwBlockReplicaFailbackWithoutStatus" -count=1 -v
go test ./cmd/sw-block -run "TestOpsFailback" -count=1 -v
go test ./core/authority ./core/ops ./cmd/sw-block
helm lint charts/seaweed-block
"C:\Program Files\Git\bin\bash.exe" scripts/run-phase78-failback-authority-runtime-gate.sh .
C:\work\swblock.exe validate testops\scenarios\failback-authority-runtime-chain.yaml
git diff --check
```

Terminal evidence:

```text
phase78_failback_authority_runtime_status=ok
core_authority_failback_runtime_tests=pass
core_ops_failback_authority_runtime_tests=pass
cmd_failback_authority_runtime_tests=pass
authority_failback_reassign_minted=true
stale_expected_current_rejected=true
terminal_preconditions_required=true
failback_target_endpoint_fields=true
failback_target_expected_current_fields=true
executable_failback_requires_authority_endpoint=true
authority_epoch_advanced=true
single_primary_after_failback=true
publish_target_swapped_after_failback=true
explicit_runtime_authority_mutation_allowed=true
storage_mutation_allowed=false
frontend_publication_allowed=false
```

## Non-Claims

Phase 78 does not implement:

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

Wire this authority seam to a product call-site. The call-site must preserve the
same expected-current guard and terminal evidence contract, and must remain
explicit-policy gated until live failback has a full QA path.
