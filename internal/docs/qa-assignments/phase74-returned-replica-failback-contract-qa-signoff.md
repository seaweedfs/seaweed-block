# Phase 74 Returned-Replica Failback Contract QA Sign-off

Verdict: PASS.

## Scope

Phase 74 validates the ACK-after returned-replica state:

```text
ack_eligibility_known=true
ack_eligible=true
frontend_fenced=true
durable_frontier_covered=true
```

must surface an explicit, disabled failback contract instead of silently
falling through to generic frontend publication.

This is a local/runner contract gate. It does not install Kubernetes resources.

## Evidence

Local checks:

```text
go test ./core/ops -run "TestEvaluateManagedVolumeAction|TestReturnedReplicaExecutor|TestManagedVolumeProjection_ReturnedReplica|TestOperatorStatusReconcilerWritesReturnedReplica|TestObservationReportSummary_IncludesReturnedReplica" -count=1
go test ./core/ops ./cmd/sw-block
C:\work\swblock.exe validate testops\scenarios\returned-replica-failback-contract-chain.yaml
"C:\Program Files\Git\bin\bash.exe" scripts/run-phase74-returned-replica-failback-contract-gate.sh .
```

Gate summary:

```text
phase74_returned_replica_failback_contract_status=ok
phase74_scope=returned_replica_failback_contract
storage_mutation_allowed=false
frontend_publication_allowed=false
failback_execution_enabled=false
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
terminal_evidence_required=ack_eligible_true,frontend_fenced_before_failback,failback_authority_owner,authority_epoch_advanced,single_primary_after_failback,publish_target_swapped_after_failback,no_cross_volume_identity_change
failback_mutation_allowed=false
failback_runtime_invocations=0
frontend_publication_attempts=0
phase74_returned_replica_failback_contract_status=ok
```

## Result

PASS:

- ACK-after returned replica surfaces `authority.failback_returned_replica`.
- Failback preflight is ready only after ACK eligibility is true.
- Executor contract is disabled with `allowed_mutation=failback`.
- ACK eligibility and rebuild contracts are preserved.
- No frontend publication, failback runtime, storage, workload, or
  cross-volume mutation is attempted.

## Non-Claims

Phase 74 does not claim real failback, authority epoch mutation, primary
reassignment, publish-target swap, blockvolume frontend switch, or NVMe ANA
parity.
