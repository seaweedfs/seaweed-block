# Phase 52 Finished Plan: Returned-Replica Executor Contract

Status: complete.

Branch: `phase52-returned-replica-executor-contract`

## Summary

Phase 52 publishes the future returned-replica executor boundary as a
machine-readable, non-mutating contract.

The phase does not execute returned-replica reintegration. It names what the
future executor is allowed to own, what remains forbidden, and what terminal
evidence must exist before any later phase can enable mutation.

## What Changed

- Added `ReturnedReplicaExecutorContract`.
- Derived executor contracts from the existing returned-replica executor
  preflight.
- Published the contract through:
  - report summary,
  - explain text,
  - operator-snapshot/dashboard JSON,
  - SwBlockVolume `.status.executorContracts[]`.
- Added the SwBlockVolume OpenAPI schema for `executorContracts[]` using
  camelCase fields.
- Extended the live returned-replica status/RBAC gate so Kubernetes
  server-side dry-run accepts the valid contract and rejects schema drift.
- Kept `executionEnabled=false` and `mutationAllowed=false`.

## Closed Acceptance

```text
ready preflight -> executor contract decision=disabled
held preflight -> executor contract decision=blocked
execution_enabled=false
mutation_allowed=false
allowed mutation class is ack_eligibility only
frontend_publication/rebuild_traffic/failback remain forbidden
terminal evidence is projected
SwBlockVolume.status.executorContracts[] validates against real CRD schema
snake_case drift is rejected
operator-status RBAC remains status/events-only
```

## Validation

```text
go test -count=1 ./core/ops ./cmd/sw-block
bash -n scripts/run-phase47-returned-replica-status-schema-rbac-gate.sh
swblock validate testops/scenarios/returned-replica-status-schema-rbac-chain.yaml
swblock run testops/scenarios/returned-replica-status-schema-rbac-chain.yaml
```

All passed. Live run:

```text
run: 20260621-171227-5362
actions: 24/24 PASS
```

Key live summary:

```text
valid_returned_replica_status_server_dry_run=true
valid_executor_preflight_status_server_dry_run=true
executor_preflight_ack_eligibility_known_projected=true
executor_preflight_forbidden_mutation_class_projected=true
valid_executor_contract_status_server_dry_run=true
executor_contract_execution_disabled_projected=true
executor_contract_terminal_evidence_projected=true
snake_case_action_rejected=true
unsupported_action_mode_rejected=true
main_object_patch_rejected=true
server_dry_run_status_mutated=false
phase47_returned_replica_status_schema_rbac_status=ok
```

## Non-Claims

- No returned-replica rebuild execution.
- No automatic reintegration or failback.
- No frontend publication.
- No authority mutation.
- No storage write or rebuild traffic.
- No lifecycle-owner/operator-status RBAC expansion.
- No release-image claim.

## Next Step

The next returned-replica phase may decide whether to implement an executor,
but it must reuse this contract. A mutating implementation must prove
admission/RBAC confinement, terminal evidence, failure handling, and
multi-volume isolation before any execution is enabled.
