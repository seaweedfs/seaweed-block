# Current Plan: Phase 47 Returned-Replica Executor Admission

Status: active.

Working branch: `phase47-returned-replica-executor-gate`

Decision note: v0.5 release smoke remains intentionally skipped. Do not mark
v0.5 released until matching public images are published and the pinned-image
smoke is run.

Previous product phase: Phase 46 is closed in
`internal/docs/finished-plans/phase46_finishedplan_returned_replica_reintegration_productization.md`.

## Product Goal

Move returned-replica reintegration from a purely rejected product hint to an
admitted dry-run executor decision when, and only when, the evidence proves the
returned replica is safe to consider:

```text
returned replica observed
-> non-primary frontend/ACK path remains fenced
-> durable frontier is known
-> durable frontier covers the required frontier
-> product surfaces show a dry-run allowed action
-> no data-plane mutation executes in this phase
```

This phase is the executor-admission bridge. It does **not** implement automatic
failback, rebuild traffic, replica ACK eligibility mutation, or production HA
claims.

## Why This Is Next

Phase 46 made returned replicas visible and fenced. The next risk is turning
that visibility into an action without repeating the earlier operation-layer
failure mode:

```text
semantic model looks correct
but the live code lacks the exact evidence or boundary
```

Phase 47 therefore starts by tightening the action evaluator and schema/API
regression gates before any executor is allowed to mutate storage state.

## Scope Contract

| In | Out |
|---|---|
| dry-run admission for `authority.reintegrate_returned_replica` | automatic failback |
| exact returned-replica safety facts | broad rebuild executor |
| rejection on missing frontier coverage or unsafe frontend readiness | ACK eligibility mutation |
| CRD/report/dashboard/explain agreement | backup/snapshot/restore |
| schema/RBAC regression gate for status/action DTOs | NVMe ANA parity |
| TestOps returned-replica rerun | release-image publication |

## D1: Dry-Run Admission Contract

Goal: replace blanket policy-disabled rejection with an evidence-gated dry-run
decision for returned-replica reintegration.

Acceptance:

```text
[x] `authority.reintegrate_returned_replica` remains mode=dry_run
[x] mutation_allowed remains false
[x] action is allowed only when returned replica is frontend fenced
[x] action is allowed only when durable frontier covers required frontier
[x] frontend-ready returned replica is rejected
[x] behind/missing frontier is rejected
[x] report/explain/dashboard/CRD surfaces show the same decision
```

Implementation note: the first slice changes only the evaluator/contract. It
does not execute catch-up/rebuild/failback.

Evidence: `TestOpsReturnedReplicaFromBundleSurfacesAcrossReportExplainDashboard`
and `go test ./core/ops` validate the dry-run allowed decision and rejection
paths.

## D2: Schema / RBAC Regression Gate

Goal: stop repeating live-only CRD/RBAC failures from Phases 35-42.

Acceptance:

```text
[x] schema-aware API validates SwBlockVolume status payloads
[x] returned-replica action DTO uses the same enum/casing as the CRD
[x] operator-status remains status/events-only
[x] lifecycle-owner remains finalizer-only where enabled
[x] no main-object/spec/storage/workload mutation is added
```

Implementation note: `TestPhase40D1KubernetesStatusClientConformsToCRDSchemaAndRBAC`
now includes the Phase 47 `authority.reintegrate_returned_replica` allowed
dry-run payload and validates it against the CRD schema-aware mock. QA should
still run a real-apiserver server-side-dry-run gate before release or before a
later mutating executor phase.

## D3: Product Surface Rerun

Goal: ensure D1's dry-run admission is visible and not overstated.

Acceptance:

```text
[ ] report summary shows `decision=allowed` for the safe fenced returned replica
[ ] explain shows the same allowed dry-run action
[ ] dashboard/operator-snapshot shows the same action contract
[ ] CRD status allowedActions uses camelCase and validates against the CRD
[ ] unsafe/behind returned replica still has no false Ready=True
```

## D4: Live Returned-Replica Gate

Goal: rerun the real returned-replica scenario against the new action decision.

Acceptance:

```text
[ ] component gate passes
[ ] iSCSI returned-replica chain passes
[ ] r2 remains sole primary/frontend-ready replica
[ ] returned r1 remains frontend/ACK fenced
[ ] action decision is dry-run allowed only for safe reintegration evidence
[ ] cleanup verifier reports zero residue
```

## D5: Close / Future Executor Decision

Goal: decide whether the next phase may wire a real executor.

Acceptance:

```text
[ ] QA sign-off states exactly what is allowed and what remains non-claim
[ ] finished plan records the evidence gate and no-mutation boundary
[ ] roadmap names the next possible executor slice separately
```

Only after D5 should a later phase consider a real mutating executor for
catch-up/rebuild/failback.
