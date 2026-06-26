# Phase 73 Finished Plan: Frontend Publication Authority Owner Guard

Status: complete.

## Problem

Phase 72 added a generic HTTP runtime contract for future frontend publication.
That contract was too permissive for the current returned-replica pipeline:

```text
ACK eligibility target
-> SwBlockFrontendPublication target
-> runtime reports frontendPublished=true
-> failbackStarted=false
```

For a returned replica, making it frontend-active is not a standalone local
runtime action. It is an authority/failback decision. If the product keeps
`primaryUnchanged=true`, it must not claim returned-replica frontend
publication.

## Implementation

Added a guard in `FrontendPublicationExecutorReconciler`:

```text
sourceEligibilityName != ""
frontendPublicationDecision=enabled
frontendPublicationMutationAllowed=true
primaryUnchanged=true
```

now writes:

```text
state=blocked
reasonCode=frontend_publication_requires_authority_owner
frontendPublished=false
failbackStarted=false
noStorageMutation=true
```

and does not call the runtime.

The generic Phase 72 runtime seam remains covered for non-returned targets.
This preserves the typed HTTP request/result contract while preventing the
returned-replica path from using it as a fake product side effect.

## Gate

Added:

```text
scripts/run-phase73-frontend-publication-authority-owner-gate.sh
testops/scenarios/frontend-publication-authority-owner-chain.yaml
```

The gate checks:

```text
returned replica runtime invocations = 0
returned replica frontend_published=false
generic runtime seam preserved
runtime failure has no false publish
invalid terminal evidence has no false publish
failback attempts = 0
storage mutation allowed = false
```

## Verification

```text
go test ./core/ops -run "TestFrontendPublicationExecutorBlocksReturnedReplicaRuntimeWithoutAuthorityOwner|TestFrontendPublicationExecutorInvokesRuntimeWhenExplicitlyEnabled|TestFrontendPublicationExecutorRuntimeFailureWritesBlockedStatus|TestFrontendPublicationExecutorRejectsInvalidRuntimeTerminalEvidence|TestHTTPFrontendPublicationRuntime" -count=1 -v
go test ./core/ops ./cmd/sw-block
C:\work\swblock.exe validate testops\scenarios\frontend-publication-authority-owner-chain.yaml
"C:\Program Files\Git\bin\bash.exe" scripts/run-phase73-frontend-publication-authority-owner-gate.sh .
```

Terminal evidence:

```text
phase73_frontend_publication_authority_owner_status=ok
core_ops_frontend_publication_authority_owner_tests=pass
returned_replica_frontend_publication_blocked=true
generic_runtime_contract_still_wired=true
runtime_failure_no_false_publish=true
runtime_invalid_terminal_evidence_no_false_publish=true
frontend_publication_requires_authority_owner=true
returned_replica_runtime_invocations=0
returned_replica_frontend_published=false
generic_runtime_seam_preserved=true
frontend_publication_attempts=0
failback_attempts=0
```

## Non-Claims

Phase 73 does not implement:

```text
blockmaster authority/failback endpoint
blockvolume frontend switch
real returned-replica frontend publication
automatic failback
storage/workload mutation
NVMe ANA parity
```

## Next

Returned-replica frontend publication should remain blocked until a future
authority/failback owner owns the side effect and proves terminal evidence
through a live gate.
