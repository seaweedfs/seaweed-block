# Phase 76 Finished Plan: Returned-Replica Failback Executor Boundary

Status: complete.

## Problem

Phase 75 introduced `SwBlockReplicaFailback` as the handoff target for future
returned-replica failback. The target existed, but no executor identity owned
its status surface. Without that boundary, the next implementation would jump
straight from a target object to authority mutation.

## Implementation

Added:

```text
sw-block ops failback-executor
```

The executor reads `SwBlockReplicaFailback` targets and writes disabled status:

```text
state=blocked
reasonCode=failback_policy_disabled
failbackMutationAllowed=false
failbackStarted=false
authorityEpochAdvanced=false
singlePrimaryAfterFailback=false
publishTargetSwappedAfterFailback=false
```

Invalid targets are blocked with:

```text
reasonCode=missing_required_facts
```

Added Kubernetes status writer support:

```text
swblockreplicafailbacks/status
```

Added disabled-by-default Helm packaging:

```text
failbackExecutor.create=false
failbackExecutor.dryRun=true
```

RBAC is status-only:

```text
swblockreplicafailbacks: get,list,watch
swblockreplicafailbacks/status: get,update,patch
```

## Gate

Added:

```text
scripts/run-phase76-failback-executor-boundary-gate.sh
testops/scenarios/failback-executor-boundary-chain.yaml
```

The gate checks:

```text
disabled status write
dry-run no status write
invalid target blocked
Kubernetes writer uses status subresource
RBAC is status-only
failback attempts = 0
authority mutation allowed = false
frontend publication allowed = false
storage mutation allowed = false
```

## Verification

```text
go test ./core/ops -run "TestFailbackExecutor|TestPhase76|TestKubernetesStatusClientPatchesOnlyStatusSubresources" -count=1 -v
go test ./cmd/sw-block -run "TestOpsFailbackExecutor|TestOpsFailbackTargetOwner" -count=1 -v
go test ./core/ops ./cmd/sw-block
helm lint charts/seaweed-block
helm template sw-block charts/seaweed-block --set failbackExecutor.create=true
"C:\Program Files\Git\bin\bash.exe" scripts/run-phase76-failback-executor-boundary-gate.sh .
C:\work\swblock.exe validate testops\scenarios\failback-executor-boundary-chain.yaml
```

Terminal evidence:

```text
phase76_failback_executor_boundary_status=ok
core_ops_failback_executor_tests=pass
cmd_failback_executor_tests=pass
failback_executor_writes_disabled_status=true
failback_executor_dry_run_no_status_write=true
failback_executor_invalid_target_blocked=true
kubernetes_writer_failback_status_subresource=true
failback_executor_rbac_status_only=true
failback_attempts=0
authority_mutation_allowed=false
frontend_publication_allowed=false
storage_mutation_allowed=false
```

## Non-Claims

Phase 76 does not implement:

```text
failback execution
authority epoch mutation
primary reassignment
publish-target swap
frontend publication
blockvolume frontend switching
storage/workload mutation
```

## Next

The next phase should add an execution preflight/runtime contract for failback,
still disabled until it can prove authority ownership, epoch advance,
single-primary state, publish-target swap, and cross-volume isolation.
