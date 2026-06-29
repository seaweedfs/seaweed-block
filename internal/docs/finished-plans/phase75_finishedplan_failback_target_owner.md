# Phase 75 Finished Plan: Returned-Replica Failback Target Owner

Status: complete.

## Problem

Phase 74 exposed the post-ACK returned-replica state as a disabled failback
contract:

```text
authority.failback_returned_replica
```

That made the missing operation visible, but it still had no Kubernetes target
object or owner identity. Without that seam, a future failback executor would
either have to read `SwBlockVolume` directly or risk reusing the wrong
pipeline, such as ACK eligibility, rebuild traffic, or generic frontend
publication.

## Implementation

Added a narrow failback target object:

```text
SwBlockReplicaFailback
```

The target spec carries only identity and precondition facts:

```text
volumeName
volumeID
pvcName
replicaID
ackEligible
frontendFencedBeforeFailback
durableFrontierCovered
noCrossVolumeIdentityChange
```

Added:

```text
sw-block ops failback-target-owner
```

The target owner plans a failback handoff only when the visible
`authority.failback_returned_replica` contract is ready and terminal evidence is
present:

```text
ack eligible
frontend still fenced
durable frontier covers required frontier
no cross-volume identity change
```

Packaging is disabled and dry-run by default:

```text
failbackTargetOwner.create=false
failbackTargetOwner.dryRun=true
```

RBAC allows only:

```text
swblockvolumes: get,list,watch
swblockreplicafailbacks: get,list,watch,create
```

No status, finalizer, Event, workload, storage, or delete/update/patch powers
were added.

## Gate

Added:

```text
scripts/run-phase75-failback-target-owner-gate.sh
testops/scenarios/failback-target-owner-chain.yaml
```

The gate checks:

```text
target creation after terminal evidence
dry-run creates no target
non-failback contracts are rejected
missing terminal evidence blocks target creation
CRD schema is bounded and camelCase
Helm/RBAC packaging is disabled and narrow
failback attempts = 0
storage mutation allowed = false
frontend publication allowed = false
```

## Verification

```text
go test ./core/ops -run "TestFailbackTargetOwner|TestPhase75|TestPhase57D1SwBlockReplicaRebuildTargetSchema|TestPhase69SwBlockFrontendPublicationTargetSchema" -count=1 -v
go test ./cmd/sw-block -run "TestOpsFailbackTargetOwner|TestOpsRebuildTargetOwner|TestOpsFrontendPublicationTargetOwner" -count=1 -v
go test ./core/ops ./cmd/sw-block
helm lint charts/seaweed-block
helm template sw-block charts/seaweed-block --set failbackTargetOwner.create=true
"C:\Program Files\Git\bin\bash.exe" scripts/run-phase75-failback-target-owner-gate.sh .
C:\work\swblock.exe validate testops\scenarios\failback-target-owner-chain.yaml
```

Terminal evidence:

```text
phase75_failback_target_owner_status=ok
core_ops_failback_target_owner_tests=pass
cmd_failback_target_owner_tests=pass
failback_target_owner_creates_target=true
failback_target_owner_dry_run_no_create=true
failback_target_owner_rejects_non_failback_contract=true
failback_target_owner_requires_terminal_evidence=true
failback_target_crd_schema=true
failback_target_owner_chart_boundary=true
failback_attempts=0
storage_mutation_allowed=false
frontend_publication_allowed=false
```

## Non-Claims

Phase 75 does not implement:

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

The next phase should add a failback executor boundary that consumes
`SwBlockReplicaFailback` and remains disabled until the real authority mutation
can prove epoch advance, single-primary state, publish-target swap, and
cross-volume isolation.
