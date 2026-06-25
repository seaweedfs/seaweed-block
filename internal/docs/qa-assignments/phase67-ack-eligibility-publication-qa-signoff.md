# Phase 67 ACK Eligibility Publication QA Sign-off

Status: PASS.

Validated source tree: local Phase67 working tree synced to m02
`/tmp/seaweed_block`.

## Scope

Phase 67 validates the first bounded publication mutation after returned-replica
runtime catch-up:

```text
SwBlockReplicaRebuild.status caught_up -> SwBlockReplicaEligibility.status
```

It does not validate or claim frontend publication, failback, primary authority
change, or storage/workload mutation.

## Result

```text
Scenario: ack-eligibility-publication-chain.yaml
Run:      20260625-020908-a6ed
Result:   14/14 PASS
```

## Terminal Evidence

```text
phase67_ack_eligibility_publication_status=ok
phase67_scope=caught_up_to_ack_eligibility_status_publication
core_ops_ack_publication_tests=pass
eligibility_status_schema_locked=true
rebuild_status_schema_locked=true
ack_publication_after_caught_up=true
ack_publication_holds_before_caught_up=true
rebuild_terminal_source_still_caught_up=true
runtime_transition_terminal_source=true
ack_eligibility_status_mutation_allowed=true
ack_publication_requires_rebuild_caught_up=true
ack_publication_rejects_running_rebuild=true
ack_publication_rejects_unexpected_publication_allowed=true
rebuild_status_mutation_attempts=0
frontend_publication_attempts=0
failback_attempts=0
storage_mutation_allowed=false
frontend_publication_allowed=false
failback_allowed=false
```

## Verified Contract

ACK eligibility status is written only after the matching rebuild target is
terminal:

```text
state=caught_up
reasonCode=rebuild_runtime_caught_up
durableFrontierCaughtUp=true
publicationDecision=disabled
publicationReason=publication_policy_disabled
publicationMutationAllowed=false
noFrontendPublication=true
noCrossVolumeIdentityChange=true
```

The written `SwBlockReplicaEligibility.status` records:

```text
reasonCode=ack_eligibility_recorded
ackEligibilityKnown=true
ackEligible=true
frontendFencedAfterExecution=true
primaryUnchanged=true
durableFrontierCovered=true
noCrossVolumeIdentityChange=true
```

## Negative Checks

The gate keeps ACK publication held when:

```text
rebuild is still running
publicationMutationAllowed unexpectedly becomes true
terminal rebuild evidence is missing
```

The gate also proves there were no rebuild-status writes, frontend publication
attempts, failback attempts, or storage/workload mutations in this phase.

## Verdict

Phase 67 PASS. The operation layer now has a bounded ACK eligibility
publication step after rebuild catch-up, still separated from frontend
publication and failback.

Next recommended gate: Phase 68 frontend publication preflight surface, still
non-mutating.
