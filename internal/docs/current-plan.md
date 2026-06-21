# Current Plan: Phase 51 Returned-Replica ACK Evidence Gate

Status: complete; local validation PASS; live gate PASS.

Working branch: `phase51-returned-replica-ack-evidence-gate`

Decision note: Phase 51 keeps returned-replica reintegration non-mutating. It
tightens the executor preflight so a missing ACK-eligibility fact is not treated
as `ack_eligible=false`.

Previous product phase: Phase 50 is closed in
`internal/docs/finished-plans/phase50_finishedplan_returned_replica_preflight_status_schema.md`.

## Product Goal

Prevent the next executor phase from confusing "no evidence of ACK eligibility"
with "known not ACK eligible":

```text
returned replica observed
-> frontend fenced
-> durable frontier covers required frontier
-> ACK eligibility explicitly known false
-> executor preflight may be ready
```

If ACK eligibility is unknown, the human-facing dry-run action can still be
shown, but the executor preflight must hold with a stable reason:

```text
returned_replica_ack_eligibility_unknown
```

## Why This Is Next

Phases 46-50 made returned-replica state, action, preflight, and CRD status
visible. While reviewing the next mutating executor step, one gap remained:
`ack_eligible=false` was projected from absence, not from a live ACK-admission
fact. That is safe for a read-only status surface, but not enough for an
executor handoff.

Phase 51 closes that semantic gap before adding any mutation.

## Scope Contract

| In | Out |
|---|---|
| `ack_eligibility_known` model field | ACK eligibility mutation |
| executor preflight holds on unknown ACK evidence | frontend publication |
| CRD/operator-snapshot/report expose the known bit | rebuild traffic |
| schema and writer tests | automatic failback |
| live status/RBAC gate payload update | lifecycle-owner/RBAC expansion |

## D1: Model Gate

Goal: make ACK eligibility evidence explicit in returned-replica facts and
projection.

Acceptance:

```text
[x] ReplicaFact carries ack_eligibility_known and ack_eligible
[x] ReturnedReplicaProjection carries ack_eligibility_known
[x] missing ACK evidence does not become a known false value
```

## D2: Executor Preflight Gate

Goal: require explicit ACK non-eligibility before preflight `ready`.

Acceptance:

```text
[x] known false ACK eligibility can produce ready
[x] unknown ACK eligibility produces hold
[x] hold reason is returned_replica_ack_eligibility_unknown
[x] existing dry-run action remains visible and non-mutating
```

## D3: Status / Schema Contract

Goal: carry ACK evidence through every machine-readable status surface.

Acceptance:

```text
[x] report/explain text includes ack_eligibility_known
[x] operator-snapshot uses ack_eligibility_known
[x] SwBlockVolume.status uses ackEligibilityKnown
[x] CRD schema requires camelCase ackEligibilityKnown
[x] snake_case ack_eligibility_known remains rejected in CRD payloads
```

Finished plan:
`internal/docs/finished-plans/phase51_finishedplan_returned_replica_ack_evidence_gate.md`.

## D4: Validation

Required before close:

```text
[x] go test -count=1 ./core/ops
[x] go test -count=1 ./cmd/sw-block
[x] bash -n scripts/run-phase47-returned-replica-status-schema-rbac-gate.sh
[x] swblock validate testops/scenarios/returned-replica-status-schema-rbac-chain.yaml
[x] swblock run testops/scenarios/returned-replica-status-schema-rbac-chain.yaml
    run: 20260621-003502-a3ce, 18/18 PASS
```

## QA Assignment

QA/live validation re-ran the returned-replica status schema/RBAC chain and
verified:

```text
valid status payload includes ackEligibilityKnown=true
missing/snake_case ack_eligibility_known is rejected by the CRD schema
executorPreflights[].decision remains ready only for known ACK non-eligible evidence
operator-status RBAC remains status/events-only
server_dry_run_status_mutated=false
```

## Non-Claims

- No ACK eligibility mutation.
- No frontend publication.
- No rebuild traffic.
- No automatic failback.
- No lifecycle-owner/operator-status RBAC expansion.
- No release-image claim.

## Next Phase Candidate

Only after Phase 51 is validated should the project design the real returned
replica executor boundary: exact mutation set, admission/RBAC, terminal
evidence, failure handling, and multi-volume isolation.
