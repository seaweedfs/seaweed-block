# Phase 59 Finished Plan: Rebuild Planning Close Gate

Status: complete.

Completed on: 2026-06-23.

Branch: `phase54-returned-replica-reintegration-executor`.

## What Changed

Phase 59 closed the returned-replica rebuild planning loop:

```text
SwBlockVolume.status.executorContracts[]
  -> rebuild-target-owner creates SwBlockReplicaRebuild.spec
  -> authority-executor writes SwBlockReplicaRebuild.status
```

Before this phase, the executor could write planned rebuild status only if a
`SwBlockReplicaRebuild` target had been manually created. The live gate now
proves the target owner creates that target automatically from the ready rebuild
contract.

## Product Boundary

This phase is planning/status only.

Non-claims:

- no rebuild data movement;
- no WAL/block copy;
- no frontend publication;
- no failback;
- no primary authority change;
- no ACK eligibility mutation;
- no cross-volume mutation.

## Deliverables

- `scripts/run-phase59-rebuild-planning-close-gate.sh`
  - compiles the current `sw-block` binary on the lab node;
  - runs real in-cluster `sw-block ops rebuild-target-owner`;
  - runs real in-cluster `sw-block ops authority-executor`;
  - waits for CRD storage readiness before launching the Jobs.
- `testops/scenarios/rebuild-planning-close-chain.yaml`
  - asserts target creation;
  - asserts target-owner idempotency;
  - asserts planned rebuild status;
  - asserts no rebuild traffic/frontend/failback claims.
- Roadmap update:
  - documents Phases 56-59 as the rebuild planning train.

## Verification

Local:

```text
go test ./core/ops ./cmd/sw-block ./scripts
helm lint charts/seaweed-block
swblock validate testops/scenarios/rebuild-planning-close-chain.yaml
```

Live:

```text
20260623-174546-3054 rebuild-planning-close-chain PASS 22/22
```

Key evidence:

```text
rebuild_target_owner=target_mutation ... targets_created=1 ...
rebuild_target_owner=target_mutation ... targets_existing=1 targets_created=0 ...
authority_executor=executed ... rebuild_progress_mutation_attempts=1 ...
rebuild_status_state_after_executor=planned
rebuild_traffic_started_after_executor=false
no_frontend_publication_after_executor=true
swblockvolume_reason_unchanged=candidate_frontier_behind
swblockvolume_finalizers_unchanged=
```

## Result

The rebuild planning control-plane chain is now closed and automated. The next
major delivery can move from planned status toward a real, bounded
rebuild/catch-up data-path primitive, but that must be treated as a new
capability with its own evidence, status, failure, and cleanup gates.
