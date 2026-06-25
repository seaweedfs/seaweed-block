# Current Plan: Phase 66 Caught-up Publication Preflight

Status: complete.

## Goal

Phase 65 proved runtime rebuild/catch-up can reach terminal caught-up evidence.
Phase 66 consumes that evidence without enabling publication.

The product now exposes the next operation decision on
`SwBlockReplicaRebuild.status`:

```text
not caught_up -> publicationDecision=blocked
caught_up     -> publicationDecision=disabled
```

`disabled` means the precondition is satisfied but the policy for publication
mutation remains off. This keeps the control model honest: caught-up is
necessary, but not sufficient, for ACK eligibility, frontend publication, or
failback.

## Delivered

### D1: Publication Decision Surface

`SwBlockReplicaRebuild.status` now includes:

```text
publicationDecision
publicationReason
publicationMutationAllowed
```

The CRD schema locks the allowed decision values:

```text
blocked
disabled
```

### D2: Status Semantics

Rebuild states project publication readiness as:

```text
planned/running/blocked:
  publicationDecision=blocked
  publicationReason=rebuild_caught_up_required
  publicationMutationAllowed=false

caught_up:
  publicationDecision=disabled
  publicationReason=publication_policy_disabled
  publicationMutationAllowed=false
```

### D3: Gate

Gate files:

```text
scripts/run-phase66-caught-up-publication-preflight-gate.sh
testops/scenarios/caught-up-publication-preflight-chain.yaml
```

The gate proves schema, Kubernetes writer casing, blocked-before-caught-up, and
disabled-after-caught-up behavior.

## Non-Claims

Phase 66 does not claim:

```text
ACK eligibility mutation
frontend publication
failback
automatic publish target change
NVMe/ANA behavior
```

## Verification

Local:

```text
go test ./core/ops ./core/transport ./core/replication ./core/host/volume ./cmd/blockvolume
C:\work\swblock.exe validate testops\scenarios\caught-up-publication-preflight-chain.yaml
```

Live:

```text
20260625-014356-978b caught-up-publication-preflight-chain PASS 12/12
```

Terminal evidence:

```text
phase66_caught_up_publication_preflight_status=ok
publication_decision_schema_locked=true
publication_decision_camel_case=true
publication_blocked_until_caught_up=true
publication_disabled_after_caught_up=true
publication_mutation_allowed=false
frontend_publication_allowed=false
failback_allowed=false
ack_eligibility_mutation_allowed=false
```

## Next

Phase 67 can either:

```text
1. add the first bounded ACK-eligibility publication mutation, with admission/RBAC/evidence gates; or
2. stop the returned-replica executor line here and start NVMe ANA using the same status/action model.
```

If continuing operations, do not jump directly to frontend/failback. ACK
eligibility is the narrowest next mutation.
