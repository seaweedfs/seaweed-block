# Current Plan: Phase 59 Rebuild Planning Close Gate

Status: complete.

Branch target: `phase54-returned-replica-reintegration-executor`

## Goal

Phases 56-58 built the returned-replica rebuild planning chain in pieces:

```text
Phase 56: SwBlockVolume.status exposes rebuild_returned_replica contract
Phase 57: SwBlockReplicaRebuild.status target exists for planned progress
Phase 58: rebuild-target-owner creates SwBlockReplicaRebuild target CRs
```

Phase 59 closes that planning loop as one product path:

```text
SwBlockVolume.status.executorContracts[]
  -> sw-block ops rebuild-target-owner
  -> SwBlockReplicaRebuild.spec target exists
  -> sw-block ops authority-executor --allowed-mutation-class=rebuild_traffic
  -> SwBlockReplicaRebuild.status.state=planned
```

This phase still does not move data. It proves the target no longer has to be
manually stubbed before the executor can write planned rebuild status.

## Scope

In scope:

- Add an integrated TestRunner gate using real `sw-block` CLI commands.
- Compile the current `sw-block` binary on the lab node.
- Run `rebuild-target-owner` inside Kubernetes with its own ServiceAccount.
- Run `authority-executor` inside Kubernetes with its own ServiceAccount.
- Verify the target-owner creates exactly one `SwBlockReplicaRebuild` target.
- Verify a second target-owner run is idempotent.
- Verify the executor writes planned rebuild status to that target.
- Verify `SwBlockVolume.status` and finalizers remain unchanged.

Out of scope:

- No rebuild data movement.
- No WAL/block copy.
- No frontend publication.
- No failback.
- No primary authority change.
- No ACK eligibility mutation.
- No cross-volume mutation.

## Deliverables

### D1: Integrated Gate Script

Status: implemented locally.

Added:

```text
scripts/run-phase59-rebuild-planning-close-gate.sh
```

The script creates synthetic CRD evidence, runs product CLI commands inside
Kubernetes jobs, and records terminal evidence.

### D2: TestRunner Scenario

Status: implemented locally.

Added:

```text
testops/scenarios/rebuild-planning-close-chain.yaml
```

The scenario asserts:

- target-owner first run creates one target;
- target-owner second run detects the existing target and creates zero more;
- target status is empty before executor;
- executor writes `state=planned`;
- `rebuildTrafficStarted=false`;
- `noFrontendPublication=true`;
- the source `SwBlockVolume` reason and finalizers do not change.

### D3: Roadmap Alignment

Status: complete.

Update roadmap wording so Phase 56-59 are visible as the rebuild planning
train, not a shipped real rebuild data path.

### D4: Live QA Gate

Status: QA PASS.

Run:

```text
swblock run testops/scenarios/rebuild-planning-close-chain.yaml
```

Expected terminal evidence:

```text
phase59_rebuild_planning_close_status=ok
rebuild_target_owner=target_mutation ... targets_created=1 ...
rebuild_target_owner=target_mutation ... targets_existing=1 targets_created=0 ...
rebuild_target_status_before_executor=
authority_executor=executed ... rebuild_progress_mutation_attempts=1 ...
rebuild_status_state_after_executor=planned
rebuild_traffic_started_after_executor=false
no_frontend_publication_after_executor=true
swblockvolume_reason_unchanged=candidate_frontier_behind
```

## Verification

Local:

```text
go test ./core/ops ./cmd/sw-block ./scripts
helm lint charts/seaweed-block
swblock validate testops/scenarios/rebuild-planning-close-chain.yaml
```

Live:

```text
swblock run testops/scenarios/rebuild-planning-close-chain.yaml
```

## Exit

Phase 59 closes when the live gate proves the returned-replica rebuild planning
chain works end-to-end without manual target stubs and without claiming rebuild
data movement.

Result:

```text
20260623-174546-3054 rebuild-planning-close-chain PASS 22/22
```

Sign-off:

```text
internal/docs/qa-assignments/phase59-rebuild-planning-close-qa-signoff.md
```
