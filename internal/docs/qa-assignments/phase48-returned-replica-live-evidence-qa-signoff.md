# Phase 48 Returned-Replica Live Evidence QA Sign-off

Status: PASS.

Validated branch: `phase48-returned-replica-live-evidence`.

## Gate

Phase 48 closes the Phase 47 carry-forward: the live iSCSI returned-replica
scenario now emits same-run managed-volume evidence and replays it through the
product surfaces. The phase does not add rebuild, failback, frontend
publication, ACK eligibility, or storage mutation.

## Runs

| Gate | Run | Result |
|---|---:|---|
| D1/D2 live evidence + report replay | `20260620-111650-7d43` | PASS, 64/64 |
| D1-D3 live evidence + report/explain/dashboard replay | `20260620-111821-99ce` | PASS, 68/68 |

## Evidence

The final run extends `iscsi-returned-replica-chain.yaml` after the live
failover/return path:

```text
r2 remains AuthorityRole=primary
r2 remains FrontendPrimaryReady=true
r1 returns as authority_non_primary=true
r1 FrontendPrimaryReady=false
r1 Healthy=false
r1 durable recovered and stays supporting/fenced
required_frontier_lsn is derived from r2 durable status
r1_durable_lsn >= required_frontier_lsn
```

The generated same-run bundle contains:

```text
product-observation/cluster-evidence.json
returned-replica-live-evidence-summary.txt
report/summary.txt
report/operator-snapshot.json
explain.txt
dashboard-operator-snapshot.json
```

Required assertions passed:

```text
frontier_covered=true
managed_volume_returned_replica=v1 replica=r1 state=fenced reason=returned_replica_frontend_fenced
managed_volume_action=authority.reintegrate_returned_replica mode=dry_run side_effect=authority_mutating executor=authority_recovery_executor decision=allowed
```

The explain surface also carries:

```text
managed_volume_action authority.reintegrate_returned_replica mode=dry_run
```

The dashboard operator snapshot carries:

```text
"type": "authority.reintegrate_returned_replica"
```

## Boundary

The action remains:

```text
mode=dry_run
mutation_allowed=false
owner_executor=authority_recovery_executor
```

No automatic returned-replica failback, rebuild traffic, ACK eligibility change,
or frontend publication is executed.

## Cleanup

The final run ended clean:

```text
active iSCSI sessions: 0
matched iSCSI node records: 0
blockmaster/blockvolume processes: 0
```

## Verdict

Phase 48 PASS. The live returned-replica storage path now produces the same
evidence/action decision that Phase 47 previously proved through synthetic
product-surface replay. A future mutating executor can use this as its evidence
starting point, but must still be separately scoped and gated.
