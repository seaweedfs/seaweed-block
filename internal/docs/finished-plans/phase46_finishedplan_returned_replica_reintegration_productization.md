# Phase 46 Finished Plan: Returned-Replica Reintegration Productization

Status: complete.

Branch: `phase46-returned-replica-reintegration`

## Goal

Make returned-replica recovery visible as a product lifecycle without claiming
automatic failback or rebuild execution.

The product now distinguishes:

```text
returned replica is reachable
!=
returned replica is frontend/ACK eligible
```

## What Landed

- Returned-replica state projection:
  - `fenced`
  - `recovering`
  - `blocked`
  - `unknown`
- `SwBlockVolume.status.replicaReintegrations[]` schema and CRD DTO mapping.
- Operator snapshot/report/explain/dashboard rendering for returned replicas.
- Action-model entries:
  - `authority.reintegrate_returned_replica`
  - `authority.rebuild_returned_replica`
- Both actions are decision-only in this phase:
  - `mode=dry_run`
  - `mutation_allowed=false`
  - `decision=rejected reason=policy_disabled`
- Host fix: assignments blocked during durable recovery are replayed after
  local readiness clears and replication is installed.
- TestOps live gate proves:
  - `r2` remains the only primary/frontend-ready replica,
  - returned `r1` is non-primary/frontend-fenced,
  - returned `r1` becomes visible as a healthy peer after recovery,
  - data remains readable after failover and return,
  - cleanup is clean.
- Multi-volume gate proves returned-replica projection and actions are
  volume-scoped.

## QA Evidence

| Gate | Evidence | Result |
|---|---|---|
| Component returned-replica gate | `20260619-083445-124f` | PASS, 16/16 |
| iSCSI returned-replica live chain | `20260619-083410-a413` | PASS, 57/57 |
| Product surfaces | `TestOpsReturnedReplicaFromBundleSurfacesAcrossReportExplainDashboard` | PASS |
| Multi-volume isolation | `TestObservationBundle_ReturnedReplicaProjectionIsVolumeScoped` | PASS |

## Fixed Product Gap

The live D5 gate exposed a real gap:

```text
assignment arrived while local readiness was blocked for durable recovery
-> assignment was skipped
-> recovery completed
-> returned replica stayed safe but under-projected
```

Fix:

```text
store last blocked assignment
-> clear local readiness after recovery
-> install replication volume
-> replay assignment
-> returned replica is admitted as SUPPORTING
-> frontend remains gated
```

## Product Claim After Phase 46

Seaweed Block can now claim:

```text
Returned replicas are Kubernetes-visible and product-surface visible.
They remain frontend/ACK fenced until explicit reintegration evidence exists.
Returned-replica action hints are dry-run/rejected unless a future executor is admitted.
```

## Non-Claims

Phase 46 does not claim:

- automatic failback,
- automatic returned-replica rebuild execution,
- production HA/SLO,
- NVMe ANA parity,
- backup/snapshot/restore.

## Next Work

The natural next phase is the real returned-replica executor decision:

```text
catch-up vs rebuild policy
-> executor admission boundary
-> evidence and progress reporting
-> terminal eligibility before ACK/frontend use
```

Until that lands, returned-replica reintegration remains status/action
productization, not automated repair.
