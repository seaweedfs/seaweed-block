# Phase 96 Finished Plan: Failback -> Frontend Publication Target

Status: complete.

## Problem

Phase 95 proved that a deployed failback executor can drive blockmaster-owned
authority reassignment and write terminal `SwBlockReplicaFailback.status`:

```text
state=failed_back
reasonCode=failback_completed
publishTargetSwappedAfterFailback=true
```

The next operation-layer gap was that frontend publication still had no
post-failback source. Its target owner only consumed
`SwBlockReplicaEligibility`, which describes ACK eligibility before failback.
Reusing that source for post-failback publication would blur two different
control-plane meanings.

## What Changed

Phase 96 added a second source path to the frontend publication target owner:

```text
SwBlockReplicaFailback terminal status
        -> SwBlockFrontendPublication target
```

The target owner accepts only terminal failback evidence:

- `state=failed_back`
- `reasonCode=failback_completed`
- `failbackMutationAllowed=false`
- `failbackStarted=true`
- `authorityEpochAdvanced=true`
- `singlePrimaryAfterFailback=true`
- `publishTargetSwappedAfterFailback=true`
- `noCrossVolumeIdentityChange=true`

The created `SwBlockFrontendPublication` records:

- `sourceFailbackName`
- `failbackCompleted`
- `authorityEpochAdvanced`
- `singlePrimaryAfterFailback`
- `publishTargetSwappedAfterFailback`

It remains disabled:

- `frontendPublicationDecision=disabled`
- `frontendPublicationReason=frontend_publication_policy_disabled`
- `frontendPublicationMutationAllowed=false`

## Boundary

This phase does not:

- execute frontend publication;
- publish a frontend path to workloads;
- call failback again;
- mutate storage;
- write frontend publication status from the target owner.

The only mutation is target creation by the target owner.

## Verification

Scenario:

```text
testops/scenarios/failback-frontend-publication-target-chain.yaml
```

Gate:

```text
scripts/run-phase96-failback-frontend-publication-target-gate.sh
```

Validated:

```text
swblock run testops/scenarios/failback-frontend-publication-target-chain.yaml
run=20260626-154640-206b
result=PASS 16/16
```

Key evidence:

```text
phase96_failback_frontend_publication_target_status=ok
terminal_failed_back_creates_frontend_publication_target=true
non_terminal_failback_rejected=true
executor_accepts_failback_target_as_disabled=true
frontend_publication_target_created_from_failback=true
frontend_publication_target_source_failback_name=true
frontend_publication_attempts=0
failback_attempts=0
storage_mutation_allowed=false
```

## Next

Phase 97 should explicitly gate frontend publication execution after failback.
That phase must stay separate because it is the first boundary that can publish
a post-failback frontend path. Workload-visible I/O after publication should
remain a later gate.
