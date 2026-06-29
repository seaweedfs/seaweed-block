# Phase 51 Returned-Replica ACK Evidence Gate QA

Status: PASS. See
`internal/docs/qa-assignments/phase51-returned-replica-ack-evidence-gate-qa-signoff.md`.

Branch: `phase51-returned-replica-ack-evidence-gate`

## Purpose

Validate that returned-replica executor preflight no longer treats missing ACK
eligibility evidence as known-safe evidence.

This phase remains non-mutating. It only changes status/preflight semantics.

## Required Checks

```text
go test -count=1 ./core/ops
go test -count=1 ./cmd/sw-block
swblock validate testops/scenarios/returned-replica-status-schema-rbac-chain.yaml
swblock run testops/scenarios/returned-replica-status-schema-rbac-chain.yaml
```

## Gate Criteria

The returned-replica status schema/RBAC live gate must show:

```text
valid_returned_replica_status_server_dry_run=true
valid_executor_preflight_status_server_dry_run=true
executor_preflight_forbidden_mutation_class_projected=true
snake_case_action_rejected=true
unsupported_action_mode_rejected=true
main_object_patch_rejected=true
server_dry_run_status_mutated=false
phase47_returned_replica_status_schema_rbac_status=ok
```

In addition, inspect the valid status payload or dry-run output and confirm:

```text
replicaReintegrations[0].ackEligibilityKnown=true
executorPreflights[0].ackEligibilityKnown=true
executorPreflights[0].ackEligible=false
executorPreflights[0].decision=ready
```

## Negative-First Rule

If ACK eligibility is absent in product evidence, the executor preflight should
hold with:

```text
decision=hold
reason=returned_replica_ack_eligibility_unknown
mutationAllowed=false
```

The human-facing dry-run action may still be visible, but a future executor must
not treat the preflight as ready.

## Non-Claims

- No returned-replica mutation.
- No ACK eligibility change.
- No frontend publication.
- No rebuild traffic.
- No failback.
- No RBAC expansion.
