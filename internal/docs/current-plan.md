# Current Plan: Phase 74 Returned-Replica Failback Contract

Status: complete.

## Goal

Phase 73 correctly blocked returned-replica frontend publication unless a real
authority/failback owner exists. Phase 74 names that missing owner as an
explicit product contract:

```text
authority.failback_returned_replica
```

This phase does not execute failback. It makes the ACK-after state visible and
bounded so future implementation cannot reuse the ACK-eligibility or generic
frontend-publication path as a fake failback.

## Deliverables

### D1: Explicit Failback Action

When a returned replica is:

```text
frontend_fenced=true
ack_eligibility_known=true
ack_eligible=true
durable_frontier_covered=true
```

the managed-volume projection now surfaces:

```text
managed_volume_action=authority.failback_returned_replica
mode=dry_run
side_effect=authority_mutating
executor=authority_recovery_executor
decision=rejected
reason=policy_disabled
```

The action is visible, but not executable.

### D2: Failback Preflight + Executor Contract

The returned-replica executor handoff now uses the ACK-after state to produce:

```text
managed_volume_executor_preflight=authority.failback_returned_replica
decision=ready
mutation_allowed=false
```

and a disabled executor contract:

```text
managed_volume_executor_contract=authority.failback_returned_replica
decision=disabled
reason=executor_policy_disabled
execution_enabled=false
mutation_allowed=false
allowed_mutation=failback
forbidden_mutation=ack_eligibility,frontend_publication,rebuild_traffic
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

### D3: Preserve Existing Returned-Replica Contracts

Phase 74 preserves:

```text
authority.reintegrate_returned_replica -> allowed_mutation=ack_eligibility
authority.rebuild_returned_replica     -> allowed_mutation=rebuild_traffic
```

Failback is only surfaced after ACK eligibility is already true. It is not
shown during the pre-ACK reintegration path.

### D4: Gate

Added:

```text
scripts/run-phase74-returned-replica-failback-contract-gate.sh
testops/scenarios/returned-replica-failback-contract-chain.yaml
```

The gate proves:

```text
failback action is policy-disabled
failback preflight is ready after ACK eligibility
failback executor contract is disabled
allowed mutation class is failback only
ACK and rebuild contracts are preserved
frontend publication attempts = 0
failback runtime invocations = 0
```

## Non-Claims

Phase 74 does not implement:

```text
real failback execution
authority epoch mutation
primary reassignment
publish target swap
blockmaster failback endpoint
blockvolume frontend switch
storage/workload mutation
NVMe ANA behavior
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
```

## Next

The next implementation step is not NVMe. It is the real authority/failback
owner: a bounded executor path that can advance authority, switch publish
target, prove single-primary terminal evidence, and only then unblock returned
replica frontend publication.
