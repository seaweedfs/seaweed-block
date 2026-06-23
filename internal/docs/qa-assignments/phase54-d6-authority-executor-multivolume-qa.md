# Phase 54 D6 QA: Authority Executor Multi-Volume Isolation

Status: implemented and validated live.

Runner scenario:

```text
testops/scenarios/authority-executor-multivolume-chain.yaml
```

Gate script:

```text
scripts/run-phase54-authority-executor-multivolume-gate.sh
```

## Goal

Validate that the authority executor can reconcile multiple volumes in one pass
without leaking ACK eligibility results across volume identities.

This is a stricter version of the D5 mixed reconcile. D5 proved unsafe cases
hold. D6 makes isolation the primary claim: eligible volumes may write their
own `SwBlockReplicaEligibility.status`, while blocked, mismatched, and
no-contract volumes remain untouched.

## Live Cases

- `eligible-a`: complete terminal evidence and matching target; writes
  `ack_eligibility_recorded`.
- `eligible-b`: complete terminal evidence and matching target; writes
  `ack_eligibility_recorded`.
- `blocked-c`: returned-replica preflight is not ready; target status remains
  absent.
- `no-contract-d`: no executor contract; no target object exists or is created.
- `mismatch-e`: complete-looking contract but target identity belongs to a
  different volume; target status remains absent.

## Required Properties

- The executor reports `authority_executor=partial`.
- Exactly four executor contracts are observed.
- Exactly two ACK eligibility status patches are attempted.
- Exactly two targets gain `reasonCode=ack_eligibility_recorded`.
- `blocked-c`, `mismatch-e`, and `no-contract-d` have zero ACK eligibility
  contamination.
- Source `SwBlockVolume.status.replicaReintegrations[].ackEligible` remains
  unchanged; the executor writes only the target CRD status.
- RBAC boundary remains narrow:
  - can patch `swblockreplicaeligibilities/status`,
  - cannot patch `swblockreplicaeligibilities` main objects,
  - cannot patch `SwBlockVolume.status`,
  - cannot create Events.
- Cleanup leaves no namespace, CR, CRD, job, pod, PVC, or PV residue.
