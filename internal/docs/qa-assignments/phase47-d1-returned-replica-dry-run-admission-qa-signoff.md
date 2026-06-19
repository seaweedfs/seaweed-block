# Phase 47 D1 Returned-Replica Dry-Run Admission QA Sign-off

Status: DEV VALIDATED. Pending independent QA rerun.

Validated source: local branch `phase47-returned-replica-executor-gate`.

## Gate

Phase 47 D1 changes only the action decision contract:

```text
authority.reintegrate_returned_replica
```

It is now `decision=allowed` only as a dry-run, non-mutating action when the
returned replica is:

- observed as a returned replica,
- non-primary and frontend fenced,
- not ACK eligible,
- durable-frontier known,
- required-frontier known,
- durable frontier covers the required frontier.

No executor is wired. No catch-up, rebuild, failback, ACK eligibility, or
frontend mutation executes in D1.

## Evidence

Targeted tests:

```text
go test -count=1 ./core/ops
go test -count=1 ./cmd/sw-block -run TestOpsReturnedReplicaFromBundleSurfacesAcrossReportExplainDashboard
go test -count=1 ./core/host/volume ./cmd/blockvolume
```

All passed.

## Checks

| Check | Result |
|---|---|
| Safe returned replica admits reintegration action | PASS, `decision=allowed` |
| Action remains dry-run | PASS, `mode=dry_run` |
| Mutation remains forbidden | PASS, `mutation_allowed=false` |
| Missing/behind required frontier rejects | PASS, `missing_required_facts` |
| Frontend-ready returned replica rejects | PASS, `missing_required_facts` |
| Report summary shows the same decision | PASS |
| Explain/dashboard/operator-snapshot carry the action | PASS |
| Host/blockvolume safety gates still pass | PASS |

## Non-Claims

- no automatic failback,
- no automatic rebuild,
- no data-plane catch-up execution,
- no ACK eligibility mutation,
- no frontend promotion,
- no release-image claim.

## Remaining Phase 47 Work

D2 must still add or run a real API/schema/RBAC gate for the status/action DTOs
before any later executor phase can claim Kubernetes admission safety.
