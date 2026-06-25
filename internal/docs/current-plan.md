# Current Plan: Phase 69 Frontend Publication Target Contract

Status: complete.

## Goal

Phase 68 added the frontend publication preflight to ACK eligibility status and
kept it explicitly disabled:

```text
frontendPublicationDecision=disabled
frontendPublicationReason=frontend_publication_policy_disabled
frontendPublicationMutationAllowed=false
```

Phase 69 defines the next target object without executing the next mutation.
It introduces a narrow `SwBlockFrontendPublication` target CR and a disabled-by
default `frontend-publication-target-owner` that can create target CRs only
when terminal ACK eligibility evidence is present.

This phase still does not publish frontend paths, change primary authority,
fail back, or mutate storage.

## Delivered

### D1: Target CRD

Added:

```text
SwBlockFrontendPublication
plural: swblockfrontendpublications
```

The spec carries the evidence copied from
`SwBlockReplicaEligibility.status`:

```text
volumeName
volumeID
pvcName
replicaID
sourceEligibilityName
ackEligibilityKnown
ackEligible
frontendFencedAfterExecution
primaryUnchanged
durableFrontierCovered
noCrossVolumeIdentityChange
frontendPublicationDecision
frontendPublicationReason
frontendPublicationMutationAllowed
```

The schema deliberately does not include publish target, authority epoch,
failback execution, or storage mutation fields.

### D2: Target Owner

Added:

```text
sw-block ops frontend-publication-target-owner
```

It creates `SwBlockFrontendPublication` target CRs only when the source
eligibility shows:

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

Dry-run mode plans targets without creating them.

### D3: Packaging And RBAC

Added disabled-by-default Helm packaging:

```text
frontendPublicationTargetOwner.create=false
frontendPublicationTargetOwner.dryRun=true
```

RBAC permits only:

```text
get/list/watch swblockreplicaeligibilities
get/list/watch/create swblockfrontendpublications
```

It does not grant status, finalizer, Event, workload, PVC/PV,
StorageClass, Secret, or delete permissions.

### D4: Gate

Gate files:

```text
scripts/run-phase69-frontend-publication-target-contract-gate.sh
testops/scenarios/frontend-publication-target-contract-chain.yaml
```

The gate proves:

```text
frontend publication target schema locked
target-owner creates exactly the target CR when eligibility is terminal
dry-run does not create
enabled/mutating frontend publication evidence is rejected
RBAC remains narrow
frontend publication attempts remain 0
failback attempts remain 0
storage mutation remains false
```

## Non-Claims

Phase 69 does not claim:

```text
frontend publication execution
frontend publish target update
primary authority change
failback
storage/workload mutation
NVMe ANA behavior
```

## Verification

Local:

```text
go test ./core/ops ./core/transport ./core/replication ./core/host/volume ./cmd/blockvolume ./cmd/sw-block
C:\work\swblock.exe validate testops\scenarios\frontend-publication-target-contract-chain.yaml
```

Live runner:

```text
20260625-102857-00c9 frontend-publication-target-contract-chain PASS 18/18
```

Terminal evidence:

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

Phase 70 should add the real frontend publication executor boundary before any
failback semantics:

```text
frontend publication executor status/admission/RBAC gate
```

That next phase may be allowed to update only a bounded frontend publication
target status or a narrowly admitted frontend target field, but it must still
not claim failback until frontend publication is real-API-proven and isolated.
