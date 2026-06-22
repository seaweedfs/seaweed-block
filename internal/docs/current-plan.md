# Current Plan: Phase 54 Returned-Replica Reintegration Executor Milestone

Branch target: `phase54-returned-replica-reintegration-executor`

## Why This Is a Milestone, Not a Microphase

Phases 46-53 intentionally built the returned-replica path one safety latch at
a time:

- visible returned-replica facts,
- dry-run action admission,
- live evidence,
- typed preflight,
- SwBlockVolume status schema,
- explicit ACK eligibility evidence,
- executor contract,
- disabled executor process/RBAC.

Those small phases were useful while crossing from read-only status into a
mutating control-plane boundary. Continuing with one tiny phase per latch would
now create process noise. Phase 54 should be one larger milestone with several
deliverables and one close gate.

## Product Goal

Turn returned-replica reintegration from a fully non-mutating status contract
into the first bounded executor capability.

The first mutation is deliberately narrow:

```text
set returned replica ACK eligibility only
```

It must not:

```text
publish a frontend
start rebuild/catch-up traffic
change primary authority
perform failback
touch another volume
```

## Scope

In scope:

- Teach `authority-executor` a disabled-by-default execution path for the
  `ack_eligibility` mutation class.
- Add explicit enable flags and policy gates.
- Add admission/RBAC confinement for only the required mutation target.
- Add terminal evidence projection after execution.
- Add failure projection for rejected, stale, missing, or unsafe contracts.
- Add multi-volume isolation gates.
- Add one live returned-replica close gate.

Out of scope:

- No frontend publication.
- No rebuild traffic.
- No automatic failback.
- No broad returned-replica rebuild claim.
- No NVMe ANA, backup/restore, or general repair executor.

## Deliverables

### D1: Executor Policy and Command Gate

Status: implemented on branch. The command and Helm chart now expose the policy
gate while keeping execution disabled by default.

Add explicit command-line and Helm policy:

```text
authorityExecutor.create=false by default
authorityExecutor.execution.enabled=false by default
--enable-execution still rejected unless the policy flag is present
--allowed-mutation-class=ack_eligibility is the only accepted class
```

Acceptance:

- default install remains read-only,
- enabling process without execution remains Phase 53 behavior,
- unsupported mutation classes fail closed,
- no RBAC expansion yet.

Current behavior:

```text
--allowed-mutation-class accepts only ack_eligibility
--enable-execution without --execution-policy -> executor_policy_disabled
--enable-execution with --execution-policy -> ack_eligibility_mutation_target_missing
mutation_attempts remains 0 on every blocked path
```

### D2: Mutation Target Contract

Status: target selected in
`internal/docs/ref/phase54-ack-eligibility-mutation-target-contract.md`.

Define the exact target the executor is allowed to mutate.

Preferred target shape:

```text
SwBlockVolume.status.executorContracts[] remains the input
executor writes a narrow executor evidence object/status field
operator-status remains the owner of broad readiness/status
```

Design question to resolve before code:

```text
Should ACK eligibility live in SwBlockVolume status, a separate evidence CR,
or authority-store state?
```

Acceptance:

- one owner writes the ACK eligibility fact,
- user-visible status can show who wrote it and when,
- old-primary/frontend publication remains impossible from this path.

Current conclusion:

```text
SwBlockReplicaEligibility is the narrow ACK eligibility evidence target.
SwBlockVolume status is a projection surface owned by operator-status and must
not be used as fake executor state.
```

Execution still fails closed until D3 proves RBAC/admission and the writer path
is implemented.

### D3: Admission/RBAC Boundary

Status: chart RBAC target and Kubernetes status writer path added; live proof
and executor call-site still pending.

Add real Kubernetes proof for the chosen mutation target.

Acceptance:

- executor can mutate only the selected ACK eligibility target,
- executor cannot patch SwBlockVolume spec,
- executor cannot patch finalizers,
- executor cannot patch broad status unless the selected design explicitly
  requires a narrow subresource/status field,
- executor cannot create pods/PVC/PV/storageclasses/secrets,
- live `kubectl auth can-i` gate proves the boundary.

Current chart boundary:

```text
default authorityExecutor.execution.enabled=false -> no target write RBAC
enabled -> get/list/watch swblockreplicaeligibilities
enabled -> get/update/patch swblockreplicaeligibilities/status
still no swblockvolumes/status, swblockvolumes/finalizers, Events, pods, PVCs,
PVs, storageclasses, secrets, or delete/create verbs
```

Execution still fails closed until the executor call-site is implemented and
the live RBAC/admission gate passes.

Writer path now exists:

```text
KubernetesStatusClient.WriteReplicaEligibilityStatus(...)
PATCH /apis/block.seaweedfs.com/v1alpha1/namespaces/<ns>/
  swblockreplicaeligibilities/<name>/status
```

The executor does not call it yet.

### D4: Terminal Evidence Projection

After execution, project terminal evidence:

```text
ack_eligibility_known=true
ack_eligible=true
frontend_fenced_after_execution=true
primary_unchanged=true
durable_frontier_covered=true
no_cross_volume_identity_change=true
```

Acceptance:

- report, explain, dashboard, operator-snapshot, and CRD agree,
- no false `Ready=True` if terminal evidence is missing,
- no claim of rebuild/failback.

### D5: Failure and Hold States

Cover negative cases:

- contract missing,
- preflight hold,
- ACK eligibility unknown,
- terminal evidence stale,
- primary changed during execution,
- frontend no longer fenced,
- multi-volume identity mismatch.

Acceptance:

- each case is blocked or unknown with stable reason codes,
- no mutation is attempted for unsafe cases,
- failure evidence is visible to a cold reviewer.

### D6: Multi-Volume Isolation

Exercise at least three volumes:

```text
A: eligible returned-replica contract
B: blocked contract
C: no returned-replica contract
```

Acceptance:

- A's executor result does not affect B/C,
- B's blocked state does not block A/C status publication,
- no cross-volume executor evidence contamination,
- cleanup remains zero-residue.

### D7: Live Close Gate

Run the returned-replica live chain with the executor policy enabled only for
the bounded ACK eligibility path.

Acceptance:

- previous primary remains frontend-fenced,
- current primary remains unchanged,
- durable frontier still covers required frontier,
- ACK eligibility transition is visible,
- no frontend publication/rebuild/failback occurs,
- final report/dashboard/CRD agree,
- TestRunner bundle captures enough evidence for QA review.

## Validation Plan

Minimum local checks:

```text
go test -count=1 ./core/ops ./cmd/sw-block
helm lint charts/seaweed-block
swblock validate testops/scenarios/authority-executor-rbac-chain.yaml
swblock validate testops/scenarios/iscsi-returned-replica-chain.yaml
```

Required live checks:

```text
authority-executor RBAC/admission gate
returned-replica executor negative gate
returned-replica multi-volume isolation gate
returned-replica live close gate
```

## Non-Claims Until D7 Passes

- No productized returned-replica rebuild.
- No automatic failback.
- No frontend publication.
- No rebuild/catch-up traffic.
- No production HA/SLO claim.

## Exit

Phase 54 closes only when the bounded ACK eligibility mutation is proven end to
end with live evidence and multi-volume isolation. If D2 concludes the correct
ACK eligibility target is not ready, the phase should stop at a documented
design blocker rather than implement a fake mutation path.
