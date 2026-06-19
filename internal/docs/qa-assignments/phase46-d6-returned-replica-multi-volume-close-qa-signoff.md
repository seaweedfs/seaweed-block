# Phase 46 D6 Returned-Replica Multi-Volume Close QA Sign-off

Status: PASS.

Validated source: `1311ae6` plus D6 test changes.

## Gate

`TestObservationBundle_ReturnedReplicaProjectionIsVolumeScoped`

The gate builds one bundle with two volumes:

- `pvc-a`: returned previous primary `a-r1`, current primary `a-r2`
- `pvc-b`: healthy independent primary `b-r1`

## Evidence

The report summary contains the returned-replica projection only for `pvc-a`:

```text
managed_volume_returned_replica=pvc-a replica=a-r1 state=fenced reason=returned_replica_frontend_fenced
```

The same summary does not contain:

```text
managed_volume_returned_replica=pvc-b
authority.reintegrate_returned_replica target=b-
```

The operator snapshot has two volumes. Only `pvc-a` has
`status.replica_reintegrations[]`; `pvc-b` has no returned-replica projection
and no returned-replica reintegration action.

## Boundary

Phase 46 adds status/action projection only:

- no automatic failback
- no automatic rebuild executor
- no ACK eligibility mutation
- no frontend promotion
- no new event emitter

## Verdict

D6 PASS. Returned-replica state and action decisions are volume-scoped and do
not contaminate sibling volumes.
