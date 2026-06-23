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

Execution remains disabled by default. The bounded writer path now exists; D4
must prove the executor calls it only with complete terminal evidence.

### D3: Admission/RBAC Boundary

Status: **QA PASS**. The live m02 k3s gate passed through
`testops/scenarios/authority-executor-target-rbac-chain.yaml`.

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

Writer path now exists:

```text
KubernetesStatusClient.WriteReplicaEligibilityStatus(...)
PATCH /apis/block.seaweedfs.com/v1alpha1/namespaces/<ns>/
  swblockreplicaeligibilities/<name>/status
```

The D4 call-site uses this writer only for the matching
`SwBlockReplicaEligibility.status` object.

D3 close evidence:

```text
phase54_authority_executor_target_rbac_status=ok
executor SA: get/list/watch swblockvolumes and swblockreplicaeligibilities
executor SA: update/patch swblockreplicaeligibilities/status
executor SA: denied for SwBlockVolume status/finalizers, main object, Events,
pods, PVCs, PVs, storageclasses, secrets, and delete/create verbs
default SA: denied for swblockreplicaeligibilities/status
```

### D4: Terminal Evidence Projection

Status: **QA PASS** on live m02 k3s run `20260623-110832-6b9c`
(`authority-executor-callsite-chain`, 36/36 actions).

Runner gate:

```text
testops/scenarios/authority-executor-callsite-chain.yaml
scripts/run-phase54-authority-executor-callsite-gate.sh
```

After execution, project terminal evidence:

```text
ack_eligibility_known=true
ack_eligible=true
frontend_fenced_after_execution=true
primary_unchanged=true
durable_frontier_covered=true
no_cross_volume_identity_change=true
```

Implementation boundary:

```text
authority-executor --enable-execution --execution-policy
  -> reads SwBlockVolume executorContracts + returned-replica status
  -> reads existing SwBlockReplicaEligibility targets
  -> patches only matching SwBlockReplicaEligibility.status
```

The executor does **not** create the target CR. A missing target is a hold with
`ack_eligibility_mutation_target_missing` and zero mutation attempts. This keeps
object identity ownership outside the executor.

Terminal evidence required before any patch:

```text
contract.actionType=authority.reintegrate_returned_replica
contract.decision=disabled
contract.reason=executor_policy_disabled
contract.preflightDecision=ready
contract.preflightReason=preconditions_satisfied
contract.allowedMutationClass includes ack_eligibility
returned replica exists for contract.replicaID
frontendFenced=true
frontendPrimaryReady=false
ackEligibilityKnown=true
ackEligible=false before execution
requiredFrontierKnown=true
durableFrontierKnown=true
durableFrontierLsn >= requiredFrontierLsn
exactly one SwBlockReplicaEligibility target matches volume identity + replicaID
```

`primary_unchanged` is intentionally bounded: the call-site can prove the
returned replica did not become frontend-primary-ready because it remains
frontend-fenced. It does not claim a broad cluster primary proof from this
executor path.

Acceptance:

- report, explain, dashboard, operator-snapshot, and CRD agree,
- no false `Ready=True` if terminal evidence is missing,
- no claim of rebuild/failback.

### D5: Failure and Hold States

Status: **QA PASS** on live m02 k3s run `20260623-112339-a395`
(`authority-executor-negative-chain`, 26/26 actions).

Runner gate:

```text
testops/scenarios/authority-executor-negative-chain.yaml
scripts/run-phase54-authority-executor-negative-gate.sh
```

D5 broadened the D4 holds to stale/frontier-behind evidence, unsafe frontend
state, ambiguous targets, cross-volume identity mismatch, blocked preflight, and
partial multi-contract behavior.

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

Status: **QA PASS** (`20260623-113753-d07f`, 32/32 actions). D5 included a
mixed partial reconcile smoke; D6 made multi-volume isolation the primary gate
with explicit no-contamination assertions across identities, target statuses,
and cleanup.

Exercise at least three volumes:

```text
A: eligible returned-replica contract
B: blocked contract
C: no returned-replica contract
```

Acceptance:

- A/B eligible executor results did not affect blocked/no-contract/mismatched
  volumes,
- blocked state did not block eligible status publication,
- no cross-volume executor evidence contamination
  (`cross_contamination_count=0`),
- cleanup remained zero-residue.

### D7: Live Close Gate

Status: **QA PASS** (`20260623-114709-aa80`, 34/34 actions).

Ran the returned-replica live chain with the executor policy enabled only for
the bounded ACK eligibility path.

Acceptance:

- previous primary remains frontend-fenced,
- current primary remains unchanged,
- durable frontier still covers required frontier,
- ACK eligibility transition is visible,
- no frontend publication/rebuild/failback occurs,
- final report/dashboard/CRD agree,
- TestRunner bundle captures enough evidence for QA review.

Closed evidence:

```text
previous_primary_frontend_fenced=true
current_primary_unchanged=true
durable_frontier_covered=true
executor_ack_mutation_attempts=1
target_reason=ack_eligibility_recorded
target_ack_eligible=true
target_frontend_fenced=true
target_primary_unchanged=true
target_frontier_covered=true
target_no_cross_volume=true
source_ack_still_false=false
target_nonclaims_ok=true
```

## Validation Plan

Minimum local checks:

```text
go test -count=1 ./core/ops ./cmd/sw-block
helm lint charts/seaweed-block
swblock validate testops/scenarios/authority-executor-rbac-chain.yaml
swblock validate testops/scenarios/authority-executor-target-rbac-chain.yaml
swblock validate testops/scenarios/iscsi-returned-replica-chain.yaml
```

Required live checks:

```text
authority-executor RBAC/admission gate
returned-replica executor negative gate
returned-replica multi-volume isolation gate
returned-replica live close gate
```

## Remaining Non-Claims After D7

- No automatic failback.
- No frontend publication.
- No rebuild/catch-up traffic.
- No broad returned-replica rebuild claim.
- No production HA/SLO claim.

## Exit

Phase 54 is closed. The bounded ACK eligibility mutation is proven end to end
with live returned-replica evidence and multi-volume isolation. The only
executor-owned mutation is `SwBlockReplicaEligibility.status`; broader rebuild,
frontend publication, and failback remain explicitly out of scope.
