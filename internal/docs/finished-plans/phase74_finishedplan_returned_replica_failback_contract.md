# Phase 74 Finished Plan: Returned-Replica Failback Contract

Status: complete.

## Problem

Phase 73 blocked a false-positive frontend publication path:

```text
ACK eligibility -> SwBlockFrontendPublication -> frontendPublished=true
```

That was necessary because a returned replica cannot become frontend-active
without an authority/failback owner. But after Phase 73 the product still did
not name the missing step. Operators could see ACK eligibility and a blocked
publication path, but not the next contract that must exist before frontend
publication can be enabled.

## Implementation

Added explicit action:

```text
authority.failback_returned_replica
```

It appears only when the returned replica already has:

```text
frontend_fenced=true
ack_eligibility_known=true
ack_eligible=true
durable_frontier_covered=true
```

The action is intentionally rejected by policy:

```text
decision=rejected
reason=policy_disabled
mutation_allowed=false
```

The executor preflight and contract surface the future failback envelope:

```text
action=authority.failback_returned_replica
preflight=ready
contract=disabled
allowed_mutation=failback
forbidden_mutation=ack_eligibility,frontend_publication,rebuild_traffic
execution_enabled=false
mutation_allowed=false
```

Required terminal evidence:

```text
ack_eligible_true
frontend_fenced_before_failback
failback_authority_owner
authority_epoch_advanced
single_primary_after_failback
publish_target_swapped_after_failback
no_cross_volume_identity_change
```

This keeps the control model explicit without adding mutation.

## Gate

Added:

```text
scripts/run-phase74-returned-replica-failback-contract-gate.sh
testops/scenarios/returned-replica-failback-contract-chain.yaml
```

The gate checks:

```text
failback action policy-disabled
failback preflight ready after ACK
failback contract disabled
CRD/operator-status surface carries the failback contract
report surface carries the failback contract
ACK and rebuild contracts preserved
frontend publication attempts = 0
failback runtime invocations = 0
```

## Verification

```text
go test ./core/ops -run "TestEvaluateManagedVolumeAction|TestReturnedReplicaExecutor|TestManagedVolumeProjection_ReturnedReplica|TestOperatorStatusReconcilerWritesReturnedReplica|TestObservationReportSummary_IncludesReturnedReplica" -count=1
go test ./core/ops ./cmd/sw-block
C:\work\swblock.exe validate testops\scenarios\returned-replica-failback-contract-chain.yaml
"C:\Program Files\Git\bin\bash.exe" scripts/run-phase74-returned-replica-failback-contract-gate.sh .
```

Terminal evidence:

```text
phase74_returned_replica_failback_contract_status=ok
core_ops_failback_contract_tests=pass
failback_action_policy_disabled=true
failback_preflight_ready_after_ack=true
failback_contract_disabled=true
failback_projection_visible_after_ack=true
failback_crd_contract_surface=true
failback_report_surface=true
ack_eligibility_contract_preserved=true
rebuild_contract_preserved=true
failback_allowed_mutation_class=failback
forbidden_mutation_classes=ack_eligibility,frontend_publication,rebuild_traffic
failback_mutation_allowed=false
failback_runtime_invocations=0
frontend_publication_attempts=0
phase74_returned_replica_failback_contract_status=ok
```

## Non-Claims

Phase 74 does not implement:

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

Implement the real authority/failback owner as a bounded executor only after it
can prove the terminal evidence named by this phase.
