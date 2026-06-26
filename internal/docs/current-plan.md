# Current Plan: Phase 90 Failback Target Authority Gate

Status: complete.

## Goal

Phase 90 consumes the Phase 89 authority facts in the failback target-owner
path. A target owner may create a `SwBlockReplicaFailback` handoff target only
when the source `SwBlockVolume.status` contains current authority evidence:

```text
primaryReplicaID
authorityEpoch
```

The created target records those values as:

```text
spec.expectedCurrentReplicaID
spec.expectedCurrentEpoch
```

This prevents a later executor from acting against an ambiguous or stale current
primary.

## Deliverables

### D1: Missing-Authority Hold

`FailbackTargetOwnerReconciler` now refuses to create a failback target when the
source volume lacks `PrimaryReplicaID` or a non-zero `AuthorityEpoch`.

### D2: Expected-Current Target Fields

Created `SwBlockReplicaFailback` targets now copy:

```text
SwBlockVolume.status.primaryReplicaID -> spec.expectedCurrentReplicaID
SwBlockVolume.status.authorityEpoch   -> spec.expectedCurrentEpoch
```

### D3: Gate

Added:

```text
scripts/run-phase90-failback-target-authority-gate.sh
testops/scenarios/failback-target-authority-chain.yaml
```

The gate proves:

```text
ready target creation carries expected-current replica and epoch
missing current authority facts create zero targets
created target remains failbackDecision=disabled
created target remains failbackMutationAllowed=false
no failback runtime call is attempted
frontend publication remains unclaimed
```

## Verification

```text
bash scripts/run-phase90-failback-target-authority-gate.sh .
swblock validate testops/scenarios/failback-target-authority-chain.yaml
```

Expected terminal evidence:

```text
phase90_failback_target_authority_status=ok
expected_current_replica_from_swblockvolume_status=true
expected_current_epoch_from_swblockvolume_status=true
missing_current_authority_target_create_count=0
created_target_failback_decision=disabled
created_target_failback_mutation_allowed=false
failback_runtime_call_attempted=false
```

## Next

Phase 91 can decide whether to:

```text
enable target activation only under an explicit policy, or
run a live Kubernetes failback target-owner smoke with the deployed suite
```

Do not claim frontend publication after failback until a separate publication
target/executor gate exists.
