# Phase 54 D7 QA: Authority Executor Live Close Gate

Status: implemented and validated live.

Runner scenario:

```text
testops/scenarios/authority-executor-live-close-chain.yaml
```

Gate script:

```text
scripts/run-phase54-authority-executor-live-close-gate.sh
```

## Goal

Validate the full Phase 54 close path with live storage evidence:

1. Run the real iSCSI/ALUA returned-replica failover path.
2. Confirm the old primary returns as a fenced, non-primary replica.
3. Confirm the current primary remains primary.
4. Confirm the returned replica covers the required durable frontier.
5. Enable only the bounded `ack_eligibility` executor mutation.
6. Write exactly one matching `SwBlockReplicaEligibility.status`.

This gate is intentionally narrow. It proves ACK eligibility can be recorded
after terminal evidence is complete. It does not claim frontend publication,
rebuild traffic, automatic failback, or production HA/SLO behavior.

## Required Properties

- Previous primary remains non-primary and frontend-fenced.
- Current primary remains unchanged after the old primary returns.
- Returned replica durable frontier covers the required frontier.
- Report, explain, and dashboard all show the same returned-replica action as
  allowed but dry-run on the storage evidence surface.
- The executor performs exactly one ACK eligibility status mutation on the
  matching `SwBlockReplicaEligibility`.
- The target status records:
  - `reasonCode=ack_eligibility_recorded`,
  - `ackEligibilityKnown=true`,
  - `ackEligible=true`,
  - `frontendFencedAfterExecution=true`,
  - `primaryUnchanged=true`,
  - `durableFrontierCovered=true`,
  - `noCrossVolumeIdentityChange=true`.
- The source `SwBlockVolume.status.replicaReintegrations[].ackEligible`
  remains `false`; the executor does not rewrite broad volume status.
- Target non-claims explicitly preserve no frontend publication, no rebuild
  traffic, and no failback.
- RBAC boundary remains narrow:
  - can patch `swblockreplicaeligibilities/status`,
  - cannot patch `swblockreplicaeligibilities` main objects,
  - cannot patch `SwBlockVolume.status`,
  - cannot create Events, pods, or patch PVCs.
- Cleanup leaves no iSCSI, process, namespace, CR, CRD, job, pod, PVC, or PV
  residue.
