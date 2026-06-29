# Phase 54 ACK Eligibility Mutation Target Contract

Status: target selected; execution remains blocked until RBAC/admission and
writer logic are added.

## Problem

Phase 54's first intended executor mutation is deliberately narrow:

```text
set returned replica ACK eligibility only
```

That mutation must not publish a frontend, start rebuild traffic, change
primary authority, perform failback, or affect another volume.

Existing fields such as `ack_eligibility_known` and `ack_eligible` are
projected observation/status facts. Writing those fields directly from the
executor would make status look like product state without changing any
authority-owned eligibility decision.

Phase 54 selects a separate narrow CRD target:

```text
SwBlockReplicaEligibility
```

The CRD is introduced as an API target only. The authority executor still has
no write RBAC and still fails closed when execution is requested.

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

### Selected: separate evidence CR

Example shape:

```text
SwBlockReplicaEligibility
  spec.volumeName
  spec.volumeID
  spec.pvcName
  spec.replicaID
  status.ackEligibilityKnown
  status.ackEligible
  status.executor
  status.evidenceGeneration
  status.conditions
```

Pros:

- Narrow RBAC and admission boundary.
- Clear owner: authority executor writes this object only.
- `operator-status` remains the owner of broad `SwBlockVolume.status`.

Open follow-ups:

- D3 must prove the narrow RBAC/admission boundary live.
- D4 must teach `operator-status` to consume this evidence and project
  terminal status.
- Later lifecycle work must define garbage collection tied to SwBlockVolume/PVC
  lifecycle.

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

Until the writer path is implemented and D4 consumes the target, the executor
must fail closed:

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

Use `SwBlockReplicaEligibility.status` as the first bounded ACK eligibility
target.

Do not enable executor writes until D3 proves that the authority executor can
patch only this target and cannot patch SwBlockVolume status/spec/finalizers,
workloads, storage, or other volumes.
