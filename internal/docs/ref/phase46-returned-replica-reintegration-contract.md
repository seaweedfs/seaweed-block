# Phase 46 Returned-Replica Reintegration Contract

Status: active contract draft for Phase 46.

This contract defines the product surface for returned replicas. It exists to
avoid treating lower-level recovery mechanics as a user-facing rebuild claim.

## Problem

A returned replica is a replica that was absent, stale, or behind, then becomes
observable again after authority has moved to another replica. The unsafe
shortcut is:

```text
replica heartbeat/status endpoint is reachable -> mark it ready
```

That is not sufficient for a block storage product. A returned replica may be
durable-ready locally while still unsafe for frontend use, ACK eligibility, or
promotion. Reintegration must require explicit evidence.

## Product States

| State | Meaning |
|---|---|
| `fenced` | returned replica is observed and intentionally not frontend/ACK eligible |
| `recovering` | returned replica needs catch-up or rebuild evidence |
| `ready` | future state: terminal evidence allows normal replica eligibility |
| `blocked` | safety invariant is violated or recovery evidence is impossible/failed |
| `unknown` | evidence is missing or stale |

Phase 46 starts with `fenced`, `recovering`, `blocked`, and `unknown`
projection. `ready` requires a later terminal-evidence gate before it is used as
a product claim.

## Required Facts

| Fact | Authority |
|---|---|
| current primary replica | blockmaster authority line |
| previous primary replica | authority/recovery event evidence |
| replica observed | blockvolume status endpoint or bundle |
| replica frontend-primary readiness | blockvolume status projection |
| replica durable frontier | durable/WAL status |
| required frontier | authority/recovery decision |
| stale-primary fenced | host/frontend stale-path probe |
| peer health/session | replication peer status |

## Safety Rules

1. A non-primary returned replica must not be frontend-primary-ready.
2. Durable readiness alone is not reintegration readiness.
3. Missing frontier evidence is `unknown` or `recovering`, not `Ready=True`.
4. A returned previous primary stays fenced until terminal evidence says it can
   rejoin placement/ACK policy.
5. A returned-replica state must be volume-scoped; it must not contaminate
   sibling volumes.

## Action Contract

The first action entries are decision-only:

| Action | Initial policy | Reason |
|---|---|---|
| `authority.reintegrate_returned_replica` | disabled | no product executor has been admitted yet |
| `authority.rebuild_returned_replica` | disabled | rebuild executor and terminal evidence are future work |

Both actions may appear as rejected/dry-run guidance with
`mutation_allowed=false`. They must not execute data-plane recovery in Phase 46.

## QA Shape

Minimum live gate:

```text
r1 primary -> r2 promoted -> r1 returns
-> r2 remains only primary/frontend-ready replica
-> r1 is visible as returned/fenced
-> no false Ready from r1's heartbeat or durable readiness
-> report/dashboard/CRD/operator-snapshot agree
-> cleanup verifier is clean
```

