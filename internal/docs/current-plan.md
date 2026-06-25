# Current Plan: Phase 68 Frontend Publication Preflight

Status: complete.

## Goal

Phase 67 published ACK eligibility after returned-replica rebuild/catch-up
reached terminal `caught_up` evidence.

Phase 68 adds the next decision surface without enabling the next mutation:
`SwBlockReplicaEligibility.status` now states whether frontend publication is
allowed.

For this slice, the answer remains explicitly disabled:

```text
frontendPublicationDecision=disabled
frontendPublicationReason=frontend_publication_policy_disabled
frontendPublicationMutationAllowed=false
```

This keeps the operation layer honest: ACK eligibility is necessary, but not
sufficient, for frontend publication or failback.

## Delivered

### D1: Eligibility Status Frontend Publication Fields

`SwBlockReplicaEligibility.status` now includes:

```text
frontendPublicationDecision
frontendPublicationReason
frontendPublicationMutationAllowed
```

The CRD schema locks `frontendPublicationDecision` to:

```text
blocked
disabled
```

### D2: Authority Executor Projection

Both ACK eligibility publication paths now populate the frontend publication
preflight:

```text
authority.reintegrate_returned_replica terminal evidence
authority.rebuild_returned_replica caught_up evidence
```

Both write:

```text
frontendPublicationDecision=disabled
frontendPublicationReason=frontend_publication_policy_disabled
frontendPublicationMutationAllowed=false
```

### D3: Writer / Schema Guard

The Kubernetes status writer test verifies camelCase serialization:

```text
frontendPublicationDecision
frontendPublicationReason
frontendPublicationMutationAllowed
```

and rejects snake_case leaks such as:

```text
frontend_publication_decision
frontend_publication_mutation_allowed
```

### D4: Gate

Gate files:

```text
scripts/run-phase68-frontend-publication-preflight-gate.sh
testops/scenarios/frontend-publication-preflight-chain.yaml
```

The gate proves:

```text
frontend publication decision schema is locked
ACK eligibility status carries frontend publication preflight
frontend publication remains disabled
frontend publication attempts remain 0
failback attempts remain 0
storage mutation remains false
```

## Non-Claims

Phase 68 does not claim:

```text
frontend publication mutation
frontend target update
primary authority change
failback
storage/workload mutation
NVMe ANA behavior
```

## Verification

Local:

```text
go test ./core/ops ./cmd/sw-block
C:\work\swblock.exe validate testops\scenarios\frontend-publication-preflight-chain.yaml
```

Live:

```text
20260625-101523-4fec frontend-publication-preflight-chain PASS 12/12
```

Terminal evidence:

```text
phase68_frontend_publication_preflight_status=ok
core_ops_frontend_preflight_tests=pass
eligibility_status_schema_has_frontend_preflight=true
kubernetes_writer_serializes_frontend_preflight=true
rebuild_ack_status_carries_frontend_preflight=true
legacy_ack_status_carries_frontend_preflight=true
frontend_publication_decision_schema_locked=true
frontend_publication_decision=disabled
frontend_publication_reason=frontend_publication_policy_disabled
frontend_publication_mutation_allowed=false
frontend_publication_attempts=0
failback_attempts=0
storage_mutation_allowed=false
```

## Next

Phase 69 should define the first bounded frontend publication target contract.
It should still avoid automatic failback. The next useful slice is:

```text
frontend publication target CR/spec + RBAC/admission boundary
```

Only after the target contract and admission boundary pass should the executor
attempt a real frontend publication mutation.
