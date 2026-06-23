# Phase 54 D4 QA: Authority Executor Call-Site And Terminal Evidence

Status: ready for QA after D3 RBAC PASS.

## Goal

Validate that `sw-block ops authority-executor --enable-execution
--execution-policy` performs exactly one bounded mutation:

```text
PATCH SwBlockReplicaEligibility.status
```

It must not create targets, patch `SwBlockVolume`, emit Events, publish a
frontend, start rebuild/catch-up, perform failback, or touch another volume.

## Preconditions

- Phase 54 D3 RBAC gate is PASS.
- CRDs are installed, including `SwBlockReplicaEligibility`.
- `authorityExecutor.execution.enabled=true` so the executor ServiceAccount has
  only:
  - get/list/watch `swblockvolumes`
  - get/list/watch `swblockreplicaeligibilities`
  - get/update/patch `swblockreplicaeligibilities/status`

## Terminal Evidence Required

The executor may patch target status only when all are true:

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
exactly one SwBlockReplicaEligibility target matches volumeName/volumeID/pvcName
and replicaID
```

`primaryUnchanged=true` is a bounded claim: the returned replica remains
frontend-fenced and did not become frontend-primary-ready. D4 must not claim a
broader cluster-primary transition.

## G1: Target Missing Holds

Create a `SwBlockVolume` with complete ready returned-replica contract, but do
not create a matching `SwBlockReplicaEligibility`.

Run the executor as the authority-executor ServiceAccount.

Expected:

```text
authority_executor=blocked
ack_eligibility_target_missing=1
mutation_attempts=0
ack_eligibility_mutation_attempts=0
```

No `SwBlockReplicaEligibility` object is created.

## G2: Terminal Evidence Missing Holds

Create a matching `SwBlockReplicaEligibility`, but make one terminal evidence
fact unsafe, for example:

```text
frontendFenced=false
```

or:

```text
durableFrontierLsn < requiredFrontierLsn
```

Expected:

```text
authority_executor=blocked
terminal_evidence_missing=1
mutation_attempts=0
ack_eligibility_mutation_attempts=0
```

Target status must remain unchanged or absent.

## G3: Complete Evidence Writes Target Status

Create:

- one `SwBlockVolume` with a complete ready returned-replica contract,
- one matching `SwBlockReplicaEligibility` target.

Run the executor as the authority-executor ServiceAccount.

Expected command output:

```text
authority_executor=executed
mutation_attempts=1
ack_eligibility_mutation_attempts=1
storage_mutation_allowed=false
```

Expected target status:

```text
reasonCode=ack_eligibility_recorded
ackEligibilityKnown=true
ackEligible=true
frontendFencedAfterExecution=true
primaryUnchanged=true
durableFrontierCovered=true
noCrossVolumeIdentityChange=true
conditions[Ready].status=True
conditions[Ready].reason=ack_eligibility_recorded
nonClaims includes no_frontend_publication
nonClaims includes no_rebuild_traffic
nonClaims includes no_failback
nonClaims includes no_primary_authority_change
```

## G4: Boundary Carry-Forward

Re-run or spot-check the D3 boundary:

- executor can patch `swblockreplicaeligibilities/status`,
- executor cannot patch `swblockvolumes/status`,
- executor cannot patch `swblockvolumes/finalizers`,
- executor cannot patch `swblockvolumes` main object,
- executor cannot create Events,
- executor cannot create/update/patch/delete pods, PVCs, PVs, secrets, or
  storageclasses.

## G5: Cleanup

Delete all synthetic CRs and chart artifacts. Final lab state must show:

```text
0 SwBlockVolume
0 SwBlockReplicaEligibility
0 helm releases
0 sw-block pods
0 PVC/PV
```

## Blockers

Block D4 if:

- any target status patch occurs with missing terminal evidence,
- the executor creates a target CR,
- the executor patches `SwBlockVolume` or any workload/storage resource,
- target status claims frontend publication, rebuild, failback, or broad primary
  movement,
- target status is written to the wrong volume/replica identity.
