# Phase 90 QA Sign-off: Failback Target Authority Gate

Status: pending QA.

## Scope

Validate that failback target creation is gated by current authority facts on
`SwBlockVolume.status`, and that created targets carry expected-current replica
and epoch without enabling runtime execution.

## Gate

Run:

```text
bash scripts/run-phase90-failback-target-authority-gate.sh .
swblock validate testops/scenarios/failback-target-authority-chain.yaml
```

## Required Evidence

The summary must contain:

```text
phase90_failback_target_authority_status=ok
go_test_failback_target_owner=pass
target_owner_requires_current_authority=true
target_spec_expected_current_replica=true
target_spec_expected_current_epoch=true
missing_authority_blocks_creation=true
expected_current_replica_from_swblockvolume_status=true
expected_current_epoch_from_swblockvolume_status=true
missing_current_authority_target_create_count=0
created_target_failback_decision=disabled
created_target_failback_mutation_allowed=false
failback_runtime_call_attempted=false
frontend_publication_after_failback_claimed=false
storage_mutation_allowed=false
```

## Pass Criteria

- A ready failback contract creates a target only when current authority facts
  are present.
- Missing `primaryReplicaID` or `authorityEpoch` creates zero targets.
- Created targets include `expectedCurrentReplicaID` and `expectedCurrentEpoch`.
- Created targets remain disabled and non-mutating.
- No failback runtime call or frontend publication is claimed.

## Non-Claims

Phase 90 does not prove live failback execution. It only ensures the handoff
target carries the expected-current authority guard the executor/runtime must
enforce later.
