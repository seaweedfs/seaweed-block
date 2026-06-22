# Returned Replica Rebuild Readiness

This page explains why returned-replica rebuild/reintegration is the natural
next major storage lifecycle feature, and why it should not be treated as
"already done" just because lower-level recovery pieces exist.

## Reader Orientation

A returned replica is a replica that was absent, stale, or behind, then comes
back after the volume has continued elsewhere. The product question is:

```text
when can that replica safely rejoin placement, ACK eligibility, or frontend use?
```

This is harder than detecting that a pod is Running.

## Domain Background

Distributed block systems need a rebuild or catch-up path for stale replicas.
Common industry concepts:

| Term | Meaning |
|---|---|
| catch-up | replica is behind but WAL window can bring it current |
| rebuild | replica is too far behind; base data plus WAL is needed |
| durable frontier | progress boundary the replica must cover |
| pin floor | WAL retention lower bound needed by active recovery |
| barrier | terminal evidence that a recovery stream closed safely |
| reintegration | allowing a recovered replica back into normal serving/ACK policy |
| failback | moving primary role back after recovery; a separate policy decision |

The older design notes describe this as:

```text
recover(a) != merely recover(a,b)
```

Meaning: enumerating data up to a target is not enough. The product must know
whether the replica is safe for the role being claimed.

## Product Problem

Seaweed Block already has important lower-level pieces:

- WAL/frontier concepts,
- recovery/catch-up/rebuild mechanics,
- peer readiness facts,
- promotion-ready evidence,
- stale-primary fencing gates.

But productized returned-replica lifecycle requires a Kubernetes-visible loop:

```text
returned replica observed
-> fenced from frontend
-> classified catch-up or rebuild
-> progress visible
-> barrier/terminal evidence observed
-> readiness changes
-> placement/ACK eligibility updated
```

Without that loop, claiming returned-replica rebuild would be too broad.

## Methodology

Returned-replica rebuild must answer:

```text
what facts prove the replica is stale?
what frontier must it cover?
who decides catch-up vs rebuild?
who executes transfer?
what barrier proves completion?
who changes readiness/ACK eligibility?
where does the user see progress and failure?
```

## State Machine

```mermaid
stateDiagram-v2
  [*] --> Absent
  Absent --> ReturnedObserved: replica pod/status endpoint returns
  ReturnedObserved --> Fenced: frontend/ACK eligibility blocked
  Fenced --> CatchingUp: WAL window sufficient
  Fenced --> Rebuilding: WAL window insufficient
  CatchingUp --> Verifying: required frontier covered
  Rebuilding --> Verifying: base + WAL complete
  Verifying --> Ready: barrier and readiness evidence valid
  Verifying --> Blocked: evidence missing, stale, or failed
  Ready --> [*]: eligible by policy
  Blocked --> Fenced: retry with new evidence
```

## Implementation Areas

| Responsibility | Code / doc area |
|---|---|
| promotion readiness facts | `core/host/master/promotion_probe.go`, product loop tests |
| frontend fail-closed projection | `core/host/volume/projection_bridge.go`, `core/frontend/durable/` |
| recovery mechanics | recovery/transport code and older recovery tutorial |
| ManagedVolume status | `core/ops` future returned-replica projection |
| action ownership | operation-layer action model and future executor |

## What Exists Today

| Existing piece | Why it is not enough alone |
|---|---|
| recovery/catch-up mechanics | not yet a full Kubernetes-visible lifecycle claim |
| promotion-ready gates | prove candidate readiness for promotion, not returned-replica reintegration |
| stale primary fencing | prevents old writer, but does not rebuild old replica |
| ManagedVolume model | can expose facts, but needs returned-replica states/actions |
| operation-layer action model | can host future rebuild action contracts, but no executor yet |

## Executor Preflight Boundary

The current product exposes a dry-run returned-replica reintegration action, an
executor preflight, and a still-disabled executor contract. The distinction
matters:

```text
dry-run action visible
  means: the product can explain the candidate and required evidence

executor preflight ready
  means: a future bounded executor has enough typed evidence to consider work

executor contract disabled
  means: the future executor boundary is named, but execution is still off
```

Phase 51 added an explicit ACK eligibility gate to avoid a common control-plane
mistake: treating a missing fact as a safe false value.

The preflight may be `ready` only when all of these are true:

```text
exactly one returned replica target
frontend_fenced=true
ack_eligibility_known=true
ack_eligible=false
required_frontier_known=true
durable_frontier_known=true
durable_lsn >= required_lsn
mutation_allowed=false
```

If `ack_eligibility_known=false`, the preflight must hold with:

```text
reason=returned_replica_ack_eligibility_unknown
```

This is still not rebuild execution. It is a machine-readable contract that a
future executor can consume without parsing report text or inferring safety from
absence.

Phase 52 adds the next non-mutating boundary:

```text
executor_contracts[].decision=disabled
execution_enabled=false
mutation_allowed=false
allowed_mutation_class=ack_eligibility
forbidden_mutation_class=frontend_publication,rebuild_traffic,failback
```

The terminal evidence required by that contract is:

```text
ack_eligibility_known
ack_eligible_true
frontend_fenced_after_execution
primary_unchanged
durable_frontier_covered
no_cross_volume_identity_change
```

This prevents the next implementation from silently turning "mark replica
eligible for ACKs" into a broad rebuild, failback, or frontend-publication path.

Phase 53 adds the disabled executor process boundary:

```text
sw-block ops authority-executor
  reads: SwBlockVolume.status.executorContracts[]
  writes: nothing
  events: none
  mutation_allowed=false
  --enable-execution: rejected
```

Its Helm packaging is disabled by default and grants only `get/list/watch` on
`swblockvolumes`. This is deliberately still not a rebuild implementation; it is
the place where a later ACK-eligibility mutation must be inserted if and only if
the contract and admission gates prove it safe.

## QA Gate Shape Needed

A real returned-replica gate should prove:

```text
replica returns stale
frontend remains fenced
catch-up/rebuild path starts with named reason
progress is visible in CRD/report/dashboard
dirty/stale evidence does not become Ready
barrier/completion evidence arrives
replica becomes ready only after frontier is covered
multi-volume isolation holds
cleanup verifier remains clean
```

## Non-Claims

- No automated returned-replica rebuild is currently claimed.
- No failback policy is claimed.
- No backup/restore is implied.
- The executor contract is visible, but `execution_enabled=false` and
  `mutation_allowed=false`.
- The authority executor process can be packaged, but it is read-only and
  rejects execution.
- No broad HA production claim follows from lower-level recovery code alone.

## Why This Is Next After Operation Layer

Operation-layer v0.5 created the shape needed for this work:

```text
facts -> judgment -> action -> boundary -> evidence
```

Returned-replica rebuild should be the next major storage feature only if it
uses that shape. Otherwise it will recreate the old problem: engine semantics
that look coherent internally but are not a complete product capability.
