# Phase 51 Finished Plan: Returned-Replica ACK Evidence Gate

Status: complete.

Branch: `phase51-returned-replica-ack-evidence-gate`

## Summary

Phase 51 tightens the returned-replica executor preflight. A missing ACK
eligibility fact is no longer treated as a known `ack_eligible=false` fact.

The existing dry-run action remains visible and non-mutating, but executor
preflight readiness now requires:

```text
ack_eligibility_known=true
ack_eligible=false
frontend_fenced=true
durable_lsn >= required_lsn
```

If ACK eligibility is unknown, preflight holds with:

```text
reason=returned_replica_ack_eligibility_unknown
mutation_allowed=false
```

## What Changed

- Added `ack_eligibility_known` to:
  - `ReplicaFact`,
  - `ReplicaEvidence`,
  - `ReturnedReplicaProjection`,
  - `ReturnedReplicaExecutorPreflight`.
- Required `AckEligibilityKnown` before executor preflight can become `ready`.
- Kept the existing returned-replica dry-run action non-mutating and visible.
- Added `ackEligibilityKnown` to SwBlockVolume CRD status DTO/schema for:
  - `status.replicaReintegrations[]`,
  - `status.executorPreflights[]`.
- Extended report/explain text to render `ack_eligibility_known`.
- Extended the live returned-replica status/RBAC gate to assert
  `executor_preflight_ack_eligibility_known_projected=true`.

## Closed Acceptance

```text
known ACK non-eligible evidence can produce preflight ready
unknown ACK eligibility produces preflight hold
hold reason is returned_replica_ack_eligibility_unknown
dry-run action remains visible and mutation_allowed=false
operator-snapshot carries ack_eligibility_known
SwBlockVolume.status carries ackEligibilityKnown
CRD schema rejects snake_case drift
live status/RBAC gate projects ackEligibilityKnown through server-side dry-run
```

## Validation

```text
go test -count=1 ./core/ops
go test -count=1 ./cmd/sw-block
bash -n scripts/run-phase47-returned-replica-status-schema-rbac-gate.sh
swblock validate testops/scenarios/returned-replica-status-schema-rbac-chain.yaml
swblock run testops/scenarios/returned-replica-status-schema-rbac-chain.yaml
```

All passed. Live run:

```text
run: 20260621-003502-a3ce
actions: 18/18 PASS
```

Key live summary:

```text
operator_status_patch_status_allowed=yes
operator_status_update_status_allowed=yes
operator_status_create_events_allowed=yes
operator_status_main_patch_allowed=no
operator_status_finalizers_patch_allowed=no
valid_returned_replica_status_server_dry_run=true
valid_executor_preflight_status_server_dry_run=true
executor_preflight_ack_eligibility_known_projected=true
executor_preflight_forbidden_mutation_class_projected=true
snake_case_action_rejected=true
unsupported_action_mode_rejected=true
main_object_patch_rejected=true
server_dry_run_status_mutated=false
phase47_returned_replica_status_schema_rbac_status=ok
```

## Non-Claims

- No ACK eligibility mutation.
- No frontend publication.
- No rebuild traffic.
- No automatic failback.
- No lifecycle-owner/operator-status RBAC expansion.
- No release-image claim.

## Next Step

The next phase may design the real returned-replica executor boundary, but it
must define exact mutation ownership, admission/RBAC, terminal evidence,
failure handling, and multi-volume isolation before any mutation is enabled.
