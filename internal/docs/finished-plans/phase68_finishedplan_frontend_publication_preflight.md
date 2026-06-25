# Phase 68 Finished Plan: Frontend Publication Preflight

Status: complete.

## Problem

Phase 67 allowed the authority executor to publish ACK eligibility after
returned-replica rebuild/catch-up reached terminal `caught_up` evidence.

That still must not imply frontend publication or failback. A replica being
eligible to ACK is not the same as changing frontend authority, publish target,
or primary ownership.

Phase 68 adds the explicit next decision surface so the product states this
boundary instead of relying on documentation.

## Implementation

`SwBlockReplicaEligibility.status` now carries frontend publication preflight:

```text
frontendPublicationDecision
frontendPublicationReason
frontendPublicationMutationAllowed
```

The current status is deliberately disabled:

```text
frontendPublicationDecision=disabled
frontendPublicationReason=frontend_publication_policy_disabled
frontendPublicationMutationAllowed=false
```

Both authority-executor ACK eligibility paths populate this state:

```text
terminal reintegrate_returned_replica evidence
caught_up rebuild_returned_replica evidence
```

## Safety Boundary

Phase 68 is status-only. It does not write:

```text
frontend publish target
primary authority
failback state
SwBlockVolume spec/finalizers
Pods/PVCs/PVs/StorageClasses
```

The CRD schema locks the decision vocabulary and the writer tests verify
camelCase payload shape.

## Verification

Local:

```text
go test ./core/ops ./cmd/sw-block
C:\work\swblock.exe validate testops\scenarios\frontend-publication-preflight-chain.yaml
```

Live gate:

```text
20260625-101523-4fec frontend-publication-preflight-chain PASS 12/12
```

Key evidence:

```text
phase68_frontend_publication_preflight_status=ok
frontend_publication_decision=disabled
frontend_publication_mutation_allowed=false
frontend_publication_attempts=0
failback_attempts=0
storage_mutation_allowed=false
```

## Next

Phase 69 should define the frontend publication target contract plus RBAC /
admission boundary. Do not implement automatic failback before the frontend
publication target and its safety envelope are proven.
