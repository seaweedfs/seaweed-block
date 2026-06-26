# Current Plan: Phase 96 Failback -> Frontend Publication Target

Status: complete.

## Goal

Phase 96 connects the terminal failback evidence from Phase 95 into the next
control object:

```text
SwBlockReplicaFailback.status.state=failed_back
reasonCode=failback_completed
publishTargetSwappedAfterFailback=true
        |
        v
SwBlockFrontendPublication target CR
frontendPublicationDecision=disabled
frontendPublicationMutationAllowed=false
```

This is still **not** a workload-visible frontend publication claim. The phase
only creates a disabled target from terminal failback evidence and proves the
executor continues to block publication by policy.

## Deliverables

### D1: Target-Owner Input Expansion

`FrontendPublicationTargetOwnerReconciler` now reads two input streams:

- `SwBlockReplicaEligibility` for the older ACK-eligibility target path;
- `SwBlockReplicaFailback` for the returned-replica failback terminal path.

Only terminal failback evidence is accepted:

```text
state=failed_back
reasonCode=failback_completed
failbackMutationAllowed=false
failbackStarted=true
authorityEpochAdvanced=true
singlePrimaryAfterFailback=true
publishTargetSwappedAfterFailback=true
noCrossVolumeIdentityChange=true
```

Non-terminal failback targets are rejected and do not create publication
targets.

### D2: Frontend Publication Target Schema

`SwBlockFrontendPublication.spec` now has explicit failback-source fields:

```text
sourceFailbackName
failbackCompleted
authorityEpochAdvanced
singlePrimaryAfterFailback
publishTargetSwappedAfterFailback
```

The older `sourceEligibilityName` path remains unchanged. The new fields avoid
pretending a post-failback target is the same thing as an ACK-eligibility target.

### D3: Executor Boundary

The frontend publication executor recognizes disabled targets from either
source:

- ACK-eligibility target;
- terminal failback target.

It still writes blocked/disabled status only unless explicit publication
execution is separately enabled in a later phase.

### D4: Runner Gate

Added:

```text
scripts/run-phase96-failback-frontend-publication-target-gate.sh
testops/scenarios/failback-frontend-publication-target-chain.yaml
```

The gate asserts:

- terminal failback creates exactly one frontend publication target;
- non-terminal failback is rejected;
- the target records `sourceFailbackName`;
- frontend publication remains disabled;
- frontend publication attempts stay `0`;
- failback attempts stay `0`;
- storage mutation stays disallowed.

## Verification

Local checks:

```text
go test ./core/ops ./cmd/sw-block ./core/host/master -count=1
helm lint charts/seaweed-block
swblock validate testops/scenarios/failback-frontend-publication-target-chain.yaml
git diff --check
```

Runner check:

```text
swblock run testops/scenarios/failback-frontend-publication-target-chain.yaml
run=20260626-154640-206b
result=PASS 16/16
```

Terminal evidence:

```text
phase96_failback_frontend_publication_target_status=ok
terminal_failed_back_creates_frontend_publication_target=true
non_terminal_failback_rejected=true
executor_accepts_failback_target_as_disabled=true
frontend_publication_target_created_from_failback=true
frontend_publication_target_source_failback_name=true
frontend_publication_decision=disabled
frontend_publication_reason=frontend_publication_policy_disabled
frontend_publication_mutation_allowed=false
frontend_publication_attempts=0
failback_attempts=0
storage_mutation_allowed=false
```

## Next

The next boundary is explicit-policy frontend publication execution after
failback. It should remain separate because it is the first step that can make a
post-failback frontend path visible to workloads.

Expected next scope:

```text
enabled SwBlockFrontendPublication target
executor call-site with explicit policy
terminal frontend publication evidence
no failback re-entry
no storage mutation
then a later workload I/O gate
```
