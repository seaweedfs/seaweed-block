# Current Plan: Phase 56 Returned Replica Rebuild/Catch-up Contract

Status: in progress.

Branch target: `phase54-returned-replica-reintegration-executor`

## Goal

Move from Phase 54's narrow ACK-eligibility executor to the next returned
replica milestone without starting release work.

Phase 54 proved this bounded mutation:

```text
SwBlockReplicaEligibility.status ACK eligibility
```

Phase 56 defines the next executor boundary for a returned replica whose durable
frontier is behind the required frontier. The product must say, in machine
readable status:

```text
this replica needs rebuild/catch-up traffic,
the future executor envelope is rebuild_traffic,
execution is still disabled,
no frontend publication/failback/ACK mutation is allowed by this contract.
```

## Scope

In scope:

- Action/preflight/contract split between:
  - `authority.reintegrate_returned_replica` for ACK-eligible fenced replicas
    with frontier coverage.
  - `authority.rebuild_returned_replica` for fenced replicas whose durable
    frontier is behind the required frontier.
- SwBlockVolume status projection through existing `executorPreflights[]` and
  `executorContracts[]`.
- Report/explain/dashboard compatibility through the existing managed-volume
  rendering path.
- Unit and status-writer tests that prove the contract reaches CRD-shaped
  status without widening mutation authority.

Out of scope:

- No rebuild data movement.
- No frontend publication.
- No failback.
- No broad `SwBlockVolume.status` rewrite by the authority executor.
- No release smoke or published-image work before the next feature milestone.

## Deliverables

### D1: Rebuild/Catch-up Contract Projection

Status: implemented locally.

When a returned replica is fenced but its durable frontier is behind the
required frontier:

- project `authority.rebuild_returned_replica`;
- mark executor preflight `ready` only when the frontier gap is known;
- keep `execution_enabled=false` and `mutation_allowed=false`;
- name future allowed mutation class as `rebuild_traffic`;
- forbid `ack_eligibility`, `frontend_publication`, and `failback`;
- require terminal evidence:
  - `frontend_fenced_before_rebuild`;
  - `primary_unchanged`;
  - `durable_frontier_caught_up`;
  - `no_frontend_publication`;
  - `no_cross_volume_identity_change`.

Durable frontier missing remains a hold state, because the product cannot
classify the gap precisely enough to hand off rebuild execution.

### D2: Surface And CRD Status Gate

Status: implemented locally.

Prove the D1 contract reaches all user-visible status surfaces:

- `summary.txt`;
- `ops explain`;
- dashboard/operator-snapshot;
- SwBlockVolume `.status.executorPreflights[]`;
- SwBlockVolume `.status.executorContracts[]`.

Implementation:

- Added `TestOpsReturnedReplicaRebuildFromBundleSurfacesAcrossReportExplainDashboard`.
- Added `scripts/run-phase56-returned-replica-rebuild-contract-gate.sh`.
- Added `testops/scenarios/returned-replica-rebuild-contract-chain.yaml`.

### D3: Executor Non-Execution Gate

Status: implemented locally.

Prove the existing authority executor does not act on
`authority.rebuild_returned_replica` yet:

- no `SwBlockReplicaEligibility.status` ACK mutation for rebuild actions;
- no rebuild traffic;
- no frontend publication;
- no failback;
- no cross-volume mutation.

Implementation:

- `AuthorityExecutorReconciler` ignores disabled non-ACK contracts without
  mutation.
- If any unsupported/non-ACK contract is incorrectly marked
  `executionEnabled=true` or `mutationAllowed=true`, the reconciler fails
  closed through `UnsafeExecutionContractCount`.

## Verification

Current local checks:

```text
go test ./core/ops ./cmd/sw-block
helm lint charts/seaweed-block
swblock validate testops/scenarios/returned-replica-rebuild-contract-chain.yaml
```

Before Phase 56 close:

```text
go test ./core/ops ./cmd/sw-block
helm lint charts/seaweed-block
swblock validate <new phase56 scenario>
```

## Exit

Phase 56 closes when rebuild/catch-up is represented as a precise disabled
executor contract with status-surface agreement and a gate proving no rebuild
execution occurs yet.
