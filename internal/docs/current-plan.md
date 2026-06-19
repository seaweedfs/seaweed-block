# Current Plan: Phase 46 Returned-Replica Rebuild / Reintegration Productization

Status: active.

Working branch: `phase46-returned-replica-reintegration`

Decision note: the v0.5 Operation Layer release smoke is intentionally skipped
for now. Phase 44 code and QA proved the bounded `SwBlockVolume` lifecycle, but
v0.5 is **not** marked released until matching public images are published and
the pinned-image smoke is run. Do not use this branch to claim a v0.5 release.

Previous product phase: Phase 44 is closed in
`internal/docs/finished-plans/phase44_finishedplan_delete_lifecycle_close_gate.md`.

## Product Goal

Turn returned-replica recovery from lower-level engine behavior into a
Kubernetes-visible lifecycle:

```text
replica returns after authority moved
-> replica is observed as returned/stale/behind
-> frontend and ACK eligibility remain fenced
-> catch-up versus rebuild is decided from frontier evidence
-> progress and blockers are visible in CRD/report/dashboard/explain
-> replica becomes eligible only after terminal evidence
-> multi-volume isolation and cleanup hygiene hold
```

The near-term claim is **status and decision productization first**. Automatic
rebuild execution, automatic failback, backup/restore, NVMe ANA parity, and
production HA/SLO claims are out of scope unless a later deliverable explicitly
adds an executor and QA gate.

## Why This Is Next

The Operation Layer created the pattern Seaweed Block should now reuse:

```text
live facts -> judgment -> action decision -> bounded owner -> evidence -> QA
```

Returned replicas are the next high-value storage lifecycle risk because a
returned process can be reachable and durable-ready while still unsafe for
frontend/ACK use. The existing low-level gates already prove parts of this, but
the product still needs a user-visible loop that prevents false `Ready=True` and
explains what is required before reintegration.

## Existing Evidence To Reuse

- `testops/scenarios/returned-replica-component-gate.yaml`
- `testops/scenarios/iscsi-returned-replica-chain.yaml`
- `core/ops/volume_status_report_collector_test.go`
- `core/host/master/promotion_probe.go`
- `core/host/volume/projection_bridge.go`
- `docs/wiki/deep-dives/returned-replica-rebuild.md`
- `docs/wiki/deep-dives/wal-frontier-recovery.md`

These prove safety seams exist. Phase 46 must make those seams visible and
auditable at the product layer.

## Scope Contract

| In | Out |
|---|---|
| returned-replica state projection | automatic failback |
| frontend/ACK fencing status | broad automatic rebuild executor unless explicitly gated |
| catch-up versus rebuild decision model | backup/snapshot/restore |
| CRD/report/dashboard/operator-snapshot agreement | NVMe ANA parity |
| action-model entries for reintegrate/rebuild as dry-run/rejected where executor is absent | production HA/SLO |
| TestOps live gate using existing returned-replica scenarios | release-image publication |
| multi-volume isolation and cleanup verifier | unrelated operator cleanup automation |

## D1: Contract And Inventory

Goal: freeze the product contract before code changes.

Acceptance:

```text
[x] returned-replica lifecycle states are named
[x] required facts are listed: authority, frontend readiness, replication role,
    durable frontier, retained/head LSN, peer readiness, fencing reason
[x] catch-up versus rebuild decision inputs are documented
[x] non-claims are explicit: no automatic failback, no broad rebuild execution
[x] existing component/live scenarios are mapped to the new product gates
```

Implementation note: `bd92d65` adds
`internal/docs/ref/phase46-returned-replica-reintegration-contract.md`.

## D2: Returned-Replica Status Projection

Goal: expose returned-replica safety state without changing data-plane behavior.

Acceptance:

```text
[x] non-primary returned replica can be durable-ready but frontend-fenced
[x] frontend-fenced returned replica never makes the volume Ready by itself
[x] unsafe non-primary frontend readiness is classified as a blocker
[x] CRD/operator-snapshot/report summary share the same state/reason
[x] unit tests cover returned, fenced, recovering, and blocked shapes
[x] live bundle/dashboard gate confirms the projection from returned-replica
    scenario evidence
```

Implementation note: `bd92d65` adds `replicaReintegrations` to
`SwBlockVolume.status`, operator-snapshot, and report summary. This is status
projection only; it does not change data-plane behavior.

## D3: Action Decision Model

Goal: add product actions without pretending an executor exists.

Acceptance:

```text
[x] authority.reintegrate_returned_replica has preconditions and evidence refs
[x] authority.rebuild_returned_replica has preconditions and evidence refs
[x] missing executor or missing frontier evidence rejects/fails closed
[x] safe/dry-run suggestions are visible but mutation_allowed=false unless a
    later executor gate explicitly enables them
[x] report summary and operator-snapshot render the decision consistently
[x] explain/dashboard HTML/live API gate confirms the same rendering
```

## D4: Bundle / Replay Integration

Goal: make existing returned-replica evidence cold-readable.

Acceptance:

```text
[x] synthetic returned-replica bundle replays into returned/fenced state
[x] stale/behind evidence does not produce false Ready=True in projection tests
[x] durable frontier facts are preserved in summary and operator-snapshot
[ ] missing or stale evidence becomes Unknown/EvidenceStale, not Ready
[ ] support bundle relocation preserves the interpretation
[ ] existing live returned-replica scenario bundle replay is validated
```

Implementation note: `BuildObservationFromBundle` now has a Phase 46 replay
test for returned-replica projection. Live TestOps bundle replay remains D5.

## D5: Live Returned-Replica Gate

Goal: run the real returned-replica scenario through the product surfaces.

Minimum gate:

```text
[x] run returned-replica component gate
[x] run iscsi-returned-replica chain
[x] after r1 returns and r2 remains primary, r1 is non-primary and
    frontend_primary_ready=false
[x] r2 remains the only primary/frontend-ready replica
[x] CRD/report/dashboard/explain show returned/fenced/recovering facts
[x] no false Ready=True for stale or insufficient returned-replica evidence
[x] cleanup verifier reports zero residue
```

Implementation note: `f28ecf3` closes the host/live safety portion of D5.
The live chain exposed and fixed a product gap where assignments blocked during
durable recovery were not replayed after recovery, leaving returned replicas
safe but under-projected. See
`internal/docs/qa-assignments/phase46-d5-returned-replica-live-gate-qa-signoff.md`.
The K8s product-surface gate is covered by
`TestOpsReturnedReplicaFromBundleSurfacesAcrossReportExplainDashboard`, which
checks report summary, operator-snapshot/dashboard, explain, and CRD status DTO
rendering from the same returned-replica bundle.

## D6: Multi-Volume / Close Gate

Goal: prove the returned-replica lifecycle is per-volume and release-shaped.

Acceptance:

```text
[x] returned replica on volume A does not affect volume B/C status
[x] action decisions and reason codes remain volume-scoped
[x] Events are bounded and stable
[x] cleanup hygiene remains clean
[x] finished plan records what is now claimed and what remains future work
```

Implementation note: `TestObservationBundle_ReturnedReplicaProjectionIsVolumeScoped`
guards the multi-volume projection and action isolation. The returned-replica
projection does not add an event emitter in Phase 46; existing event identity
rules remain unchanged.

## Release Position

Phase 46 passed for status/decision productization. The product can claim that
returned replicas are Kubernetes-visible and safely fenced until reintegration
evidence is sufficient. It still must not claim automatic failback or production
rebuild automation unless an executor phase lands and is separately
QA-validated.
