# Current Plan: Phase 48 Returned-Replica Live Evidence Close

Status: complete; QA PASS.

Working branch: `phase48-returned-replica-live-evidence`

Decision note: v0.5 release smoke remains intentionally skipped. Do not mark
v0.5 released until matching public images are published and the pinned-image
smoke is run.

Previous product phase: Phase 47 is closed in
`internal/docs/finished-plans/phase47_finishedplan_returned_replica_executor_admission.md`.

Finished plan:
`internal/docs/finished-plans/phase48_finishedplan_returned_replica_live_evidence.md`.

## Product Goal

Close the remaining returned-replica evidence gap before any mutating executor:

```text
real iSCSI returned-replica run
-> r2 remains the sole frontend-ready primary
-> returned r1 remains frontend/ACK fenced
-> r1 durable frontier is observed
-> required frontier is derived from the live primary evidence
-> report/explain/dashboard can replay the same run as dry-run allowed
-> no rebuild/failback/ACK/frontend mutation executes
```

Phase 47 admitted `authority.reintegrate_returned_replica` only as a dry-run
action, but the live iSCSI scenario did not emit the managed-volume evidence
needed to prove the dry-run action from the same run. Phase 48 makes the live
gate produce and validate that evidence.

## Why This Is Next

Skipping directly from Phase 47 to a mutating executor would repeat the failure
mode we have been removing:

```text
product surface says an action is allowed
but the live scenario that drove the storage path did not carry the exact facts
```

The next executor phase must start from same-run live evidence, not a synthetic
bundle plus a separate storage smoke.

## Scope Contract

| In | Out |
|---|---|
| live returned-replica evidence bundle from the iSCSI scenario | automatic failback |
| required frontier derived from live durable primary evidence | rebuild traffic |
| returned replica durable frontier compared to required frontier | ACK eligibility mutation |
| report/operator-snapshot/dashboard/explain replay from same run | frontend publication change |
| TestOps assertions for dry-run allowed action from live evidence | release-image publication |

## D1: Scenario Evidence Emission

Goal: extend `iscsi-returned-replica-chain.yaml` so the live run emits a
`product-observation/cluster-evidence.json` bundle after r1 returns.

Acceptance:

```text
[x] sw-block binary is built with blockmaster/blockvolume
[x] bundle contains r2 as primary/frontend-ready
[x] bundle contains returned r1 as frontend-fenced/non-healthy
[x] bundle contains required_frontier_known=true
[x] required_frontier_lsn comes from live r2 durable status
[x] returned r1 durable_lsn >= required_frontier_lsn
```

## D2: Same-Run Report Replay

Goal: run `sw-block ops report --from-bundle` against the same live scenario
artifacts and assert the Phase 47 action decision.

Acceptance:

```text
[x] summary shows returned r1 state=fenced reason=returned_replica_frontend_fenced
[x] summary shows authority.reintegrate_returned_replica decision=allowed
[x] action remains mode=dry_run mutation_allowed=false
[x] report is produced from the live run directory, not a synthetic fixture
[x] cleanup verifier reports zero residue
```

## D3: Product Surface Parity

Goal: ensure the live-derived bundle remains consumable by product surfaces.

Acceptance:

```text
[x] operator-snapshot carries replica_reintegrations[] for r1
[x] operator-snapshot carries the dry-run allowed action
[x] dashboard replay returns the same operator-snapshot
[x] explain output includes returned-replica state and action decision
```

Implementation may be either scenario-level assertions or a small wrapper gate
that replays the collected bundle after the iSCSI scenario.

## D4: Close / Executor Readiness Decision

Goal: decide whether the next phase is allowed to design a real mutating
executor.

Acceptance:

```text
[x] QA sign-off states same-run live evidence status
[x] finished plan records the no-mutation boundary
[x] roadmap separates "executor admission" from "executor execution"
[x] next phase is explicitly scoped if mutation is proposed
```

Only after D4 should a later phase consider a bounded executor for
catch-up/rebuild/failback. That later phase must define owner, admission/RBAC,
preconditions, terminal evidence, rollback behavior, and multi-volume isolation
before enabling any mutation.
