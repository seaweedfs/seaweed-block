# Current Plan: Phase 73 Frontend Publication Authority Owner Guard

Status: complete.

## Goal

Phase 72 added a generic typed runtime seam for future frontend publication.
Reviewing the actual product topology exposed a tighter rule:

```text
returned-replica frontend publication cannot be a standalone runtime status
success while primaryUnchanged=true.
```

For a returned replica, making it the active frontend is an authority/failback
operation. If the product does not move authority, then claiming
`frontendPublished=true` is a semantic false-positive. If the product does move
authority, that must be owned by a future authority/failback executor, not by a
status-only frontend-publication executor.

## Deliverables

### D1: Returned-Replica Guard

When a `SwBlockFrontendPublication` target is sourced from ACK eligibility:

```text
sourceEligibilityName != ""
frontendPublicationDecision=enabled
frontendPublicationMutationAllowed=true
primaryUnchanged=true
```

the executor must:

```text
write status=blocked
reason=frontend_publication_requires_authority_owner
not call runtime
not claim frontendPublished
not start failback
not mutate storage/workload state
```

### D2: Preserve Generic Runtime Seam

The generic Phase 72 HTTP runtime contract remains covered for non-returned
targets. This keeps the typed request/result seam available while preventing
the returned-replica pipeline from using it as a fake publication.

### D3: Gate

Added:

```text
scripts/run-phase73-frontend-publication-authority-owner-gate.sh
testops/scenarios/frontend-publication-authority-owner-chain.yaml
```

The gate proves:

```text
returned replica runtime invocations = 0
returned replica frontend_published=false
generic runtime seam preserved
runtime failure still does not false-publish
invalid terminal evidence still does not false-publish
```

## Non-Claims

Phase 73 does not implement:

```text
real frontend publication endpoint
authority reassign/failback
blockmaster runtime HTTP/gRPC endpoint
blockvolume frontend switch
storage/workload mutation
NVMe ANA behavior
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

## Next

The next real product capability must choose and implement an authority/failback
owner before enabling returned-replica frontend publication. Until then, the
safe product behavior is to keep frontend publication blocked with explicit
reason `frontend_publication_requires_authority_owner`.
