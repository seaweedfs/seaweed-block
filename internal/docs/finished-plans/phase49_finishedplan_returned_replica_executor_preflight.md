# Phase 49 Finished Plan: Returned-Replica Executor Preflight

Status: complete.

Branch: `phase49-returned-replica-executor-preflight`

## Summary

Phase 49 adds a non-mutating executor preflight for returned-replica
reintegration. It converts the Phase 48 evidence and the Phase 47 dry-run action
decision into a typed handoff contract for a future authority executor.

The preflight can say `ready`, but it still reports `mutation_allowed=false`.
This is intentional: Phase 49 is the final planning/evidence bridge before a
future mutating executor, not the executor itself.

## What Changed

- Added `ReturnedReplicaExecutorPreflight` in `core/ops`.
- Added fail-closed reasons for:
  - action not allowed,
  - ambiguous returned replica,
  - unsafe frontend,
  - ACK eligibility already enabled,
  - missing required frontier,
  - missing durable frontier,
  - returned replica frontier behind,
  - wrong executor/mode/mutation posture.
- Rendered preflight lines in:
  - `sw-block ops report --from-bundle` summary,
  - `sw-block ops explain volume --from-bundle`.
- Kept CRD/operator-snapshot schema unchanged for this phase.

## Closed Acceptance

```text
ready requires dry-run action decision=allowed
ready requires exactly one returned-replica target
ready requires frontend_fenced=true
ready requires ack_eligible=false
ready requires required_frontier_known=true
ready requires durable_frontier_known=true
ready requires durable_lsn >= required_lsn
all decisions keep mutation_allowed=false
```

## Validation

```text
go test -count=1 ./core/ops
go test -count=1 ./cmd/sw-block -run TestOpsReturnedReplicaFromBundleSurfacesAcrossReportExplainDashboard
```

Both passed.

## Non-Claims

- No ACK eligibility mutation.
- No frontend publication change.
- No rebuild traffic.
- No automatic failback.
- No lifecycle-owner or operator-status RBAC expansion.
- No release-image claim.

## Next Step

A future executor phase may use this preflight as its input contract, but must
add its own admission/RBAC boundary, exact mutation set, terminal evidence,
multi-volume isolation, and live QA gate before enabling any mutation.
