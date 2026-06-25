# Phase 67 Finished Plan: ACK Eligibility Publication

Status: complete.

## Problem

Phase 66 showed that returned-replica rebuild/catch-up can become terminal
`caught_up`, but it intentionally stopped at:

```text
publicationDecision=disabled
publicationMutationAllowed=false
```

That was the correct hold point: caught-up is necessary evidence, not itself a
license to publish frontend authority or fail back.

The next narrow operation-layer step was to publish only ACK eligibility, so the
system can distinguish "rebuilt and eligible to ACK" from "frontend published".

## Implementation

`AuthorityExecutorReconciler` now handles `ack_eligibility` execution for two
sources:

1. Existing terminal `authority.reintegrate_returned_replica` evidence.
2. New terminal `authority.rebuild_returned_replica` evidence, gated by a
   matching `SwBlockReplicaRebuild.status=caught_up`.

For the rebuild source, the executor requires:

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

Only then does it write `SwBlockReplicaEligibility.status` with
`reasonCode=ack_eligibility_recorded`.

## Safety Boundary

Phase 67 writes only:

```text
SwBlockReplicaEligibility.status
```

It does not write:

```text
SwBlockReplicaRebuild.status
SwBlockVolume spec/status/finalizers
Pods/PVCs/PVs/StorageClasses
frontend publish targets
primary authority
```

## Verification

Local:

```text
go test ./core/ops ./cmd/sw-block
C:\work\swblock.exe validate testops\scenarios\ack-eligibility-publication-chain.yaml
```

Live gate:

```text
20260625-020908-a6ed ack-eligibility-publication-chain PASS 14/14
```

Key evidence:

```text
phase67_ack_eligibility_publication_status=ok
ack_publication_after_caught_up=true
ack_publication_holds_before_caught_up=true
ack_publication_rejects_running_rebuild=true
ack_publication_rejects_unexpected_publication_allowed=true
frontend_publication_attempts=0
failback_attempts=0
storage_mutation_allowed=false
```

## Next

The next operation-layer phase should be frontend publication preflight. It
should define the exact evidence that would be needed to publish frontend
authority, while still keeping the frontend mutation disabled.

Do not jump directly to failback or NVMe from this point if the goal is to close
returned-replica operations first.
