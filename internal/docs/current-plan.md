# Current Plan: Phase 49 Returned-Replica Executor Preflight

Status: complete; local validation PASS.

Working branch: `phase49-returned-replica-executor-preflight`

Decision note: this phase does not execute reintegration, rebuild, ACK
eligibility, failback, or frontend publication. It adds the executable
preflight contract a future authority executor must satisfy before any storage
or authority mutation can be considered.

Previous product phase: Phase 48 is closed in
`internal/docs/finished-plans/phase48_finishedplan_returned_replica_live_evidence.md`.

Finished plan:
`internal/docs/finished-plans/phase49_finishedplan_returned_replica_executor_preflight.md`.

## Product Goal

Turn Phase 48 same-run returned-replica evidence into a precise executor
preflight:

```text
returned replica evidence
-> dry-run action admitted
-> exactly one target replica
-> frontend/ACK fenced
-> durable frontier covers required frontier
-> preflight=ready, mutation_allowed=false
```

Anything incomplete or unsafe must produce `preflight=hold` with a stable reason.

## Why This Is Next

Phase 47 admitted `authority.reintegrate_returned_replica` as a dry-run action.
Phase 48 proved the live iSCSI scenario can carry the required evidence. The
remaining risk before a real executor is ambiguity: a future mutating component
should not infer readiness from free-form report lines.

Phase 49 adds a typed preflight layer that makes the handoff explicit while
keeping the no-mutation boundary.

## Scope Contract

| In | Out |
|---|---|
| `ReturnedReplicaExecutorPreflight` model | ACK eligibility mutation |
| fail-closed decision reasons | frontend publication mutation |
| report and explain visibility | rebuild traffic |
| unit tests for ready/hold states | automatic failback |
| Phase 48 bundle surface regression | CRD schema change |

## D1: Preflight Model

Goal: create a pure `core/ops` preflight contract for
`authority.reintegrate_returned_replica`.

Acceptance:

```text
[x] ready only when the dry-run action is allowed
[x] ready only for exactly one returned replica target
[x] ready only when frontend_fenced=true and ack_eligible=false
[x] ready only when durable_lsn >= required_lsn
[x] mutation_allowed=false in all states
```

## D2: Fail-Closed Reasons

Goal: make unsafe or incomplete evidence explain why a future executor must not
run.

Acceptance:

```text
[x] missing frontier -> hold
[x] unsafe frontend -> hold
[x] frontier behind -> hold
[x] ambiguous returned replica -> hold
[x] action rejected -> hold
```

## D3: Product Surface Visibility

Goal: show the preflight in the surfaces that already carry returned-replica
evidence.

Acceptance:

```text
[x] report summary renders managed_volume_executor_preflight=...
[x] ops explain renders managed_volume_executor_preflight ...
[x] existing returned-replica bundle test asserts both surfaces
[x] operator-snapshot/CRD remain unchanged in this phase
```

## D4: Validation

Validation completed:

```text
[x] go test -count=1 ./core/ops
[x] go test -count=1 ./cmd/sw-block -run TestOpsReturnedReplicaFromBundleSurfacesAcrossReportExplainDashboard
```

## Next Phase Candidate

The next storage-executor phase, if started, must define:

```text
owner executor
admission/RBAC boundary
exact mutation set
terminal evidence after mutation
hold/retry behavior
multi-volume isolation
rollback/fail-closed behavior
live QA gate
```

Do not enable a mutating returned-replica executor from Phase 49 alone.
