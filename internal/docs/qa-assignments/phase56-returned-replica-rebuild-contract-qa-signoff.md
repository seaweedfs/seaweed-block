# Phase 56 Returned Replica Rebuild/Catch-up Contract QA Sign-off

Verdict: PASS.

Validated commit: `3cea6da phase56: fix rebuild contract gate action assertion`

Scenario:

```text
testops/scenarios/returned-replica-rebuild-contract-chain.yaml
```

QA run:

```text
20260623-144531-00ee
```

Result:

```text
14/14 PASS
```

## Terminal Evidence

From
`/mnt/smb/work/share/g15d-k8s/20260623-144531-00ee-phase56-returned-replica-rebuild-contract/phase56-returned-replica-rebuild-contract-summary.txt`:

```text
phase56_returned_replica_rebuild_contract_status=running
summary_rebuild_preflight_ready=1
summary_rebuild_contract_disabled=1
summary_rebuild_action_disabled=1
explain_rebuild_contract_disabled=1
operator_snapshot_rebuild_contract=ok
dashboard_rebuild_contract=ok
phase56_returned_replica_rebuild_contract_status=ok
```

## Verified Contract

- `authority.rebuild_returned_replica` is surfaced for a fenced returned
  replica whose durable frontier is behind the required frontier.
- Executor preflight is `ready` only because the frontier gap is explicit:
  durable LSN `4240`, required LSN `4241`.
- Executor contract is `disabled`.
- `execution_enabled=false`.
- `mutation_allowed=false`.
- Future allowed mutation envelope is exactly `rebuild_traffic`.
- Forbidden mutation classes include `ack_eligibility`,
  `frontend_publication`, and `failback`.
- The user action remains rejected with `policy_disabled`.
- Report, explain, operator-snapshot, and dashboard agree.

## Residue

Clean. The gate is replay/surface-only and created no Helm release, PVC/PV, pod,
namespace, or SwBlock CRD residue.

## Finding

Initial run `20260623-144435-d0fc` failed because the gate asserted that the
report `managed_volume_action` summary line contained `mutation_allowed=false`.
That summary line does not print mutation state for actions; the
operator-snapshot does, and the gate already checks it there. The gate assertion
was narrowed in `3cea6da`, then the scenario passed.

This was a gate assertion issue, not a product defect.
