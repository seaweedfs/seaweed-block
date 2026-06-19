# Phase 47 D3/D4 Returned-Replica Surface + Live QA Sign-off

Status: DEV VALIDATED. Pending independent QA rerun.

Validated source: `41b496b` on branch
`phase47-returned-replica-executor-gate`.

## D3 Product Surfaces

Targeted gate:

```text
go test -count=1 ./cmd/sw-block -run TestOpsReturnedReplicaFromBundleSurfacesAcrossReportExplainDashboard
```

Result: PASS.

The test drives one returned-replica bundle through:

- `ops report --from-bundle`,
- `ops operator-status --from-bundle`,
- `ops explain volume --from-bundle`,
- `ops dashboard --from-bundle`.

Expected Phase 47 action surface:

```text
authority.reintegrate_returned_replica
decision=allowed
mode=dry_run
mutation_allowed=false
```

The bundle includes required-frontier evidence, so the dry-run action is
admitted. Separate evaluator tests reject missing/behind frontier and unsafe
frontend-ready returned replicas.

## D4 Live Gates

The live gates ran against `/tmp/seaweed_block_phase47` on m02, synced from
commit `41b496b`.

| Gate | Run ID | Result |
|---|---|---|
| `returned-replica-component-gate.yaml` | `20260619-155251-ac1e` | PASS, 16/16 |
| `iscsi-returned-replica-chain.yaml` | `20260619-155300-ba6d` | PASS, 57/57 |

Live iSCSI evidence:

- r2 remained primary,
- r2 remained frontend-primary-ready,
- returned r1 was non-primary,
- returned r1 had `frontend_primary_ready=false`,
- returned r1 was admitted only as a SUPPORTING replica,
- pre/post data checks passed,
- iSCSI sessions/process residue was clean.

## Boundary

The iSCSI live scenario proves the storage safety path. It does not yet emit a
managed-volume report bundle with required-frontier evidence, so the
`decision=allowed` product-surface assertion is covered by D3 rather than by
the live chain itself.

This is acceptable for D3/D4 because Phase 47 still has no mutating executor:

- no automatic failback,
- no rebuild traffic,
- no ACK eligibility mutation,
- no frontend promotion,
- no release-image claim.

## Recommendation

D3/D4 can close for the dry-run admission slice. Before any later phase enables
real executor mutation, add a live scenario artifact that emits the
required-frontier managed-volume bundle so the action decision is validated from
the same live run.
