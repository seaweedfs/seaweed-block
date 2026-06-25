# Current Plan: Phase 67 ACK Eligibility Publication

Status: complete.

## Goal

Phase 64-65 proved the runtime rebuild endpoint can start returned-replica
catch-up and report terminal durable-frontier evidence. Phase 66 consumed that
terminal state as a publication preflight but kept all publication mutations
disabled.

Phase 67 adds the narrowest next mutation: publish ACK eligibility as a
`SwBlockReplicaEligibility.status` update after a matching
`SwBlockReplicaRebuild.status` is terminal `caught_up`.

This is not frontend publication, failback, primary swap, or automatic authority
promotion. It only records that the returned replica is eligible to ACK again
after rebuild/catch-up evidence has converged.

## Delivered

### D1: Rebuild Caught-up as ACK Publication Precondition

`authority-executor --allowed-mutation-class ack_eligibility --enable-execution
--execution-policy` now also reads `SwBlockReplicaRebuild` targets. For a
`authority.rebuild_returned_replica` contract it writes ACK eligibility only
when the matching rebuild target is:

```text
state=caught_up
reasonCode=rebuild_runtime_caught_up
rebuildTrafficStarted=true
durableFrontierCaughtUp=true
publicationDecision=disabled
publicationReason=publication_policy_disabled
publicationMutationAllowed=false
noFrontendPublication=true
noCrossVolumeIdentityChange=true
```

If the rebuild is still running, missing, not caught up, or carries an
unexpected publication mutation flag, ACK publication stays held.

### D2: Status-only Mutation

The only product write is:

```text
SwBlockReplicaEligibility.status
```

The published status records:

```text
reasonCode=ack_eligibility_recorded
ackEligibilityKnown=true
ackEligible=true
frontendFencedAfterExecution=true
primaryUnchanged=true
durableFrontierCovered=true
noCrossVolumeIdentityChange=true
```

Evidence refs are merged from the executor contract, returned-replica evidence,
and runtime terminal rebuild evidence.

### D3: Non-Claims

Phase 67 explicitly does not claim:

```text
frontend publication
frontend target change
primary authority change
failback
storage/workload mutation
NVMe ANA behavior
```

### D4: Gate

Gate files:

```text
scripts/run-phase67-ack-eligibility-publication-gate.sh
testops/scenarios/ack-eligibility-publication-chain.yaml
```

The gate proves the positive caught-up publication path and the negative hold
paths before caught-up / unexpected publication mutation.

## Verification

Local:

```text
go test ./core/ops ./cmd/sw-block
C:\work\swblock.exe validate testops\scenarios\ack-eligibility-publication-chain.yaml
```

Live:

```text
20260625-020908-a6ed ack-eligibility-publication-chain PASS 14/14
```

Terminal evidence:

```text
phase67_ack_eligibility_publication_status=ok
core_ops_ack_publication_tests=pass
eligibility_status_schema_locked=true
rebuild_status_schema_locked=true
ack_publication_after_caught_up=true
ack_publication_holds_before_caught_up=true
ack_eligibility_status_mutation_allowed=true
ack_publication_requires_rebuild_caught_up=true
ack_publication_rejects_running_rebuild=true
ack_publication_rejects_unexpected_publication_allowed=true
rebuild_status_mutation_attempts=0
frontend_publication_attempts=0
failback_attempts=0
storage_mutation_allowed=false
```

## Next

Phase 68 should not jump to failback. The next operation-layer slice should be
frontend publication preflight:

```text
ACK eligibility published -> frontend publication decision surface
```

The first Phase 68 deliverable should surface the exact evidence required to
publish the frontend, while still keeping the frontend mutation disabled. Only
after that gate passes should a bounded frontend publication mutation be
considered.
