# Phase 66 Finished Plan: Caught-up Publication Preflight

Status: complete.

QA: PASS.

## Goal

Phase 66 consumes Phase 65 terminal caught-up evidence as a precondition for a
future publication decision, without performing publication.

## Delivered

Code and schema:

```text
core/ops/authority_executor_controller.go
core/ops/operator_status_controller.go
charts/seaweed-block/crds/swblockreplicarebuilds.block.seaweedfs.com.yaml
```

Gate:

```text
scripts/run-phase66-caught-up-publication-preflight-gate.sh
testops/scenarios/caught-up-publication-preflight-chain.yaml
```

Docs:

```text
internal/docs/current-plan.md
internal/docs/qa-assignments/phase66-caught-up-publication-preflight-qa-signoff.md
internal/docs/product-roadmap.md
```

## Behavior

`SwBlockReplicaRebuild.status` now carries:

```text
publicationDecision
publicationReason
publicationMutationAllowed
```

States:

```text
running/planned/blocked:
  publicationDecision=blocked
  publicationReason=rebuild_caught_up_required

caught_up:
  publicationDecision=disabled
  publicationReason=publication_policy_disabled
```

In all cases:

```text
publicationMutationAllowed=false
```

## Non-Claims

Phase 66 does not claim:

```text
ACK eligibility mutation
frontend publication
failback
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

The narrow next operation mutation is ACK eligibility publication, not frontend
publication or failback. If the team wants to stop Operation work here, NVMe ANA
can start next using the same status/action/evidence model.
