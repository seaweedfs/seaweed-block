# Phase 48 Finished Plan: Returned-Replica Live Evidence Close

Status: complete; QA PASS.

Branch: `phase48-returned-replica-live-evidence`

## Goal

Close the live-evidence gap left by Phase 47. The returned-replica dry-run
action must be proven from the same live iSCSI scenario that exercises the
returned replica, not only from a synthetic bundle.

## What Changed

`testops/scenarios/iscsi-returned-replica-chain.yaml` now:

- builds `sw-block` along with `blockmaster` and `blockvolume`,
- derives a `product-observation/cluster-evidence.json` from live r1/r2 status
  and durable-status artifacts after r1 returns,
- sets `required_frontier_lsn` from r2's live durable frontier,
- asserts r1's durable frontier covers that required frontier,
- runs `sw-block ops report --from-bundle` against the same run directory,
- runs `sw-block ops explain volume --from-bundle` against the same evidence,
- runs `sw-block ops dashboard --from-bundle` and captures
  `/operator-snapshot.json`,
- asserts the dry-run returned-replica reintegration action appears on all
  replayed surfaces.

## Evidence

| Gate | Evidence | Result |
|---|---|---|
| Local YAML validation | `swblock validate testops/scenarios/iscsi-returned-replica-chain.yaml` | PASS |
| Local product-surface test | `go test -count=1 ./cmd/sw-block -run TestOpsReturnedReplicaFromBundleSurfacesAcrossReportExplainDashboard` | PASS |
| Local ops tests | `go test -count=1 ./core/ops` | PASS |
| Live same-run report replay | `20260620-111650-7d43` | PASS, 64/64 |
| Live report/explain/dashboard replay | `20260620-111821-99ce` | PASS, 68/68 |

## Product Contract

The live run proves:

```text
r2 remains primary and frontend-ready
r1 returns as non-primary and frontend-fenced
r1 is not ACK/frontend eligible
r1 durable frontier is known
required frontier is known from r2 durable evidence
r1 durable frontier covers required frontier
authority.reintegrate_returned_replica is decision=allowed only as dry_run
```

The phase does not execute:

```text
automatic failback
rebuild traffic
ACK eligibility mutation
frontend publication mutation
storage repair
backup/restore
```

## Next Possible Phase

A future mutating returned-replica executor is now eligible for design, but not
implementation-by-default. It must define:

- owner executor and admission/RBAC boundary,
- exact preconditions and terminal evidence,
- failure rollback/hold behavior,
- multi-volume isolation,
- no false Ready/Recovered claims during mutation,
- QA gates that prove both hold and release paths.
