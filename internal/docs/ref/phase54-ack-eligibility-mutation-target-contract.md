# Phase 54 ACK Eligibility Mutation Target Contract

Status: design blocker documented.

## Problem

Phase 54's first intended executor mutation is deliberately narrow:

```text
set returned replica ACK eligibility only
```

That mutation must not publish a frontend, start rebuild traffic, change
primary authority, perform failback, or affect another volume.

The current codebase does not yet expose a durable, narrow ACK eligibility
mutation target for the authority executor. Existing fields such as
`ack_eligibility_known` and `ack_eligible` are projected observation/status
facts. Writing those fields directly from the executor would make status look
like product state without changing any authority-owned eligibility decision.

## Required Target Properties

A valid ACK eligibility target must satisfy all of these:

- It is durable enough to survive controller restart.
- It is scoped to one volume and one replica.
- It records the executor identity and observed evidence generation.
- It is not the same broad status object owned by `operator-status`.
- It cannot publish frontend paths or change primary authority.
- It gives `operator-status` a read path to project terminal evidence:
  `ack_eligibility_known=true`, `ack_eligible=true`,
  `frontend_fenced_after_execution=true`, `primary_unchanged=true`,
  `durable_frontier_covered=true`, and
  `no_cross_volume_identity_change=true`.

## Candidate Targets

### Separate evidence CR

Example shape:

```text
SwBlockReplicaEligibility
  spec.volumeRef
  spec.replicaID
  status.ackEligible
  status.executor
  status.evidenceGeneration
  status.conditions
```

Pros:

- Narrow RBAC and admission boundary.
- Clear owner: authority executor writes this object only.
- `operator-status` remains the owner of broad `SwBlockVolume.status`.

Cons:

- New CRD and lifecycle rules.
- Requires garbage-collection policy tied to SwBlockVolume/PVC lifecycle.

### Authority-store state

Pros:

- Close to the authority model that will eventually consume eligibility.
- Avoids using Kubernetes status as command state.

Cons:

- Needs an audited write API and persistence contract.
- Harder to prove with Kubernetes RBAC/admission alone.

### SwBlockVolume broad status

Not recommended.

Pros:

- Minimal new objects.

Cons:

- Conflicts with `operator-status` ownership.
- Broad status patch access is too large for the authority executor.
- Risks a semantic loop where the executor writes the same surface the report
  uses as proof.

## Current Phase 54 Behavior

Until a valid target exists, the executor must fail closed:

```text
--enable-execution without policy -> executor_policy_disabled
--enable-execution with policy    -> ack_eligibility_mutation_target_missing
unsupported mutation class        -> unsupported_mutation_class
```

All failure paths must keep:

```text
mutation_attempts=0
ack_eligibility_mutation_attempts=0
mutation_allowed=false
```

## Recommendation

Prefer a separate, narrow eligibility evidence CR unless the authority-store
owner provides a concrete persisted ACK eligibility API first.

Do not implement Phase 54 D3-D7 execution gates until D2 chooses one target and
defines its RBAC/admission boundary.
