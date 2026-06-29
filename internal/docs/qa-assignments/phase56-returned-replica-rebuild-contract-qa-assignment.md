# Phase 56 Returned Replica Rebuild/Catch-up Contract QA Assignment

## Goal

Validate that a returned replica whose durable frontier is behind the required
frontier surfaces a disabled rebuild/catch-up executor contract, without
claiming or attempting rebuild execution.

This is not a data-movement gate. It is a status/action/executor-boundary gate.

## Source

Use the current Phase 56 branch tip.

Required scenario:

```text
testops/scenarios/returned-replica-rebuild-contract-chain.yaml
```

Required script:

```text
scripts/run-phase56-returned-replica-rebuild-contract-gate.sh
```

Before running, sync the product root on the lab node as described in:

```text
internal/docs/qa-assignments/QA-AGENT-RUNBOOK.md
```

## Command

```text
swblock run testops/scenarios/returned-replica-rebuild-contract-chain.yaml
```

## PASS Criteria

The scenario must pass and the summary must contain:

```text
phase56_returned_replica_rebuild_contract_status=ok
summary_rebuild_preflight_ready=1
summary_rebuild_contract_disabled=1
summary_rebuild_action_disabled=1
operator_snapshot_rebuild_contract=ok
dashboard_rebuild_contract=ok
```

Product-surface expectations:

- returned replica state is `recovering` with reason
  `candidate_frontier_behind`;
- executor preflight is
  `authority.rebuild_returned_replica decision=ready`;
- executor contract is
  `authority.rebuild_returned_replica decision=disabled`;
- `execution_enabled=false`;
- `mutation_allowed=false`;
- future allowed mutation class is only `rebuild_traffic`;
- forbidden mutation classes include `ack_eligibility`,
  `frontend_publication`, and `failback`;
- action remains rejected with `policy_disabled`;
- dashboard/operator-snapshot agrees with report and explain.

## FAIL Conditions

Fail if any surface implies:

- rebuild traffic executed;
- frontend publication happened;
- failback happened;
- ACK eligibility was written for the rebuild contract;
- `execution_enabled=true`;
- `mutation_allowed=true`;
- the dashboard/operator-snapshot disagrees with report or explain.

## Expected Scope

This gate is replay/surface based. It should not install Helm, create PVCs, or
touch iSCSI/multipath state. Lab cleanup should still confirm no unexpected
residue.
