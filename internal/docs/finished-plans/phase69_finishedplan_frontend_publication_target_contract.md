# Phase 69 Finished Plan: Frontend Publication Target Contract

Status: complete.

## Problem

Phase 68 made frontend publication visible as an explicit disabled preflight on
ACK eligibility status. That was still only a status field.

The next operation layer needs a Kubernetes target object so a future executor
has a typed, bounded handoff point. Without that target, the next mutation would
again risk being an ad hoc call-site instead of a controllable product
operation.

## Implementation

Phase 69 adds:

```text
SwBlockFrontendPublication
sw-block ops frontend-publication-target-owner
frontendPublicationTargetOwner Helm packaging
```

The target owner creates a `SwBlockFrontendPublication` object from terminal
ACK eligibility evidence. It requires the complete safety set:

```text
ackEligibilityKnown=true
ackEligible=true
frontendFencedAfterExecution=true
primaryUnchanged=true
durableFrontierCovered=true
noCrossVolumeIdentityChange=true
frontendPublicationDecision=disabled
frontendPublicationReason=frontend_publication_policy_disabled
frontendPublicationMutationAllowed=false
```

If any of those facts are missing or the source claims frontend mutation is
already allowed, the target owner rejects the source eligibility and creates no
target.

## Safety Boundary

This phase creates only target CRs. It does not write:

```text
SwBlockFrontendPublication.status
frontend publish target
primary authority
failback state
SwBlockVolume spec/status/finalizers
Pods/PVCs/PVs/StorageClasses
Events
host storage state
```

RBAC grants only:

```text
get/list/watch swblockreplicaeligibilities
get/list/watch/create swblockfrontendpublications
```

## Verification

Local:

```text
go test ./core/ops ./core/transport ./core/replication ./core/host/volume ./cmd/blockvolume ./cmd/sw-block
C:\work\swblock.exe validate testops\scenarios\frontend-publication-target-contract-chain.yaml
```

Runner gate:

```text
20260625-102857-00c9 frontend-publication-target-contract-chain PASS 18/18
```

Key evidence:

```text
phase69_frontend_publication_target_contract_status=ok
frontend_publication_target_schema_locked=true
frontend_publication_target_owner_creates_target=true
frontend_publication_target_owner_dry_run_no_create=true
frontend_publication_target_owner_rejects_enabled_publication=true
frontend_publication_target_owner_rbac_narrow=true
frontend_publication_attempts=0
failback_attempts=0
storage_mutation_allowed=false
```

## Next

Phase 70 should build the frontend publication executor boundary. It should
prove the mutation envelope before enabling any failback or authority-swap
semantics.
