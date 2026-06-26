# Current Plan: Phase 75 Returned-Replica Failback Target Owner

Status: complete.

## Goal

Phase 74 named the missing returned-replica failback owner as an explicit,
disabled contract:

```text
authority.failback_returned_replica
```

Phase 75 adds the next narrow control-plane seam: a target-owner process can
materialize a `SwBlockReplicaFailback` handoff CR when the failback contract is
ready and terminal evidence is present.

This phase does not execute failback. It does not advance authority, change the
primary, publish a frontend, or mutate storage/workloads.

## Deliverables

### D1: Failback Target CRD

Added `SwBlockReplicaFailback`:

```text
kind=SwBlockReplicaFailback
plural=swblockreplicafailbacks
scope=Namespaced
status_subresource=true
```

The spec records only the target identity and precondition facts:

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

The status is reserved for a future executor:

```text
state=planned|blocked|failed_back
failbackMutationAllowed
failbackStarted
authorityEpochAdvanced
singlePrimaryAfterFailback
publishTargetSwappedAfterFailback
noCrossVolumeIdentityChange
```

### D2: Failback Target Owner

Added:

```text
sw-block ops failback-target-owner
```

The owner reads `SwBlockVolume` contracts and existing
`SwBlockReplicaFailback` targets. It creates a target only when:

```text
action=authority.failback_returned_replica
contract decision=disabled
executor policy=disabled
allowed_mutation=failback
ack_eligibility_known=true
ack_eligible=true
frontend_fenced=true
frontend_primary_ready=false
durable_frontier >= required_frontier
```

Dry-run is supported and is the Helm default.

### D3: Helm Packaging + RBAC

Added disabled-by-default packaging:

```text
failbackTargetOwner.create=false
failbackTargetOwner.dryRun=true
```

The target owner RBAC is intentionally narrow:

```text
swblockvolumes: get,list,watch
swblockreplicafailbacks: get,list,watch,create
```

It has no status, finalizer, Event, pod, PVC, PV, StorageClass, Secret, or
delete/update/patch permissions.

### D4: Gate

Added:

```text
scripts/run-phase75-failback-target-owner-gate.sh
testops/scenarios/failback-target-owner-chain.yaml
```

The gate proves:

```text
target owner creates target when terminal evidence is present
dry-run creates no target
non-failback contracts are rejected
missing terminal evidence blocks target creation
target CRD schema is camelCase and bounded
Helm/RBAC packaging is disabled and narrow
failback_attempts=0
storage_mutation_allowed=false
frontend_publication_allowed=false
```

## Non-Claims

Phase 75 does not implement:

```text
real failback execution
authority epoch mutation
primary reassignment
publish-target swap
frontend publication
blockvolume frontend switching
storage/workload mutation
NVMe ANA behavior
```

## Verification

```text
go test ./core/ops -run "TestFailbackTargetOwner|TestPhase75|TestPhase57D1SwBlockReplicaRebuildTargetSchema|TestPhase69SwBlockFrontendPublicationTargetSchema" -count=1 -v
go test ./cmd/sw-block -run "TestOpsFailbackTargetOwner|TestOpsRebuildTargetOwner|TestOpsFrontendPublicationTargetOwner" -count=1 -v
go test ./core/ops ./cmd/sw-block
helm lint charts/seaweed-block
helm template sw-block charts/seaweed-block --set failbackTargetOwner.create=true
```

Additional gate/scenario validation:

```text
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

## Next

The next implementation step is a failback executor boundary. It should consume
`SwBlockReplicaFailback`, remain disabled until the real authority mutation can
prove terminal evidence, and must not reuse the ACK-eligibility, rebuild, or
generic frontend-publication paths as a shortcut.
