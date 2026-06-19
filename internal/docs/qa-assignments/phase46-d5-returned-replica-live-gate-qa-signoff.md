# Phase 46 D5 Returned-Replica Live Gate QA Sign-off

Status: PASS for host/live returned-replica safety; K8s status-surface gate
remains separate.

Validated source: `f28ecf3`

Lab: m02 via isolated git-bundle clone at
`/tmp/seaweed_block_phase46_f28ecf3_20260619083402`.

## Gates Run

| Gate | Run | Result |
|---|---:|---|
| returned-replica-component-gate.yaml | `20260619-083445-124f` | PASS, 16/16 |
| iscsi-returned-replica-chain.yaml | `20260619-083410-a413` | PASS, 57/57 |

## Live Evidence

The iSCSI returned-replica chain exercised:

```text
r1 primary -> r2 promoted -> r1 returns
```

Verified evidence:

- `r2` remained the only primary:
  - `AuthorityRole=primary`
  - `FrontendPrimaryReady=true`
  - `Healthy=true`
- returned `r1` remained non-primary and frontend-fenced:
  - `authority_non_primary=true`
  - `frontend_primary_ready=false`
  - `healthy=false`
  - `ReplicationRole=replica_ready`
- `r1` was visible to `r2` as a healthy peer after return:
  - `ReplicaID=r1`
  - `State=healthy`
  - `Epoch=2`
  - `ProbeInFlight=false`
  - `Closed=false`
- durable recovery evidence was present:
  - `blockvolume-r1-returned.log`: `durable recovered: recovered LSN=...`
  - `status-durable-r2-primary-after-r1-return.summary`: `operational=true`
- data path survived failover and return:
  - `pre-check-after-failover.log`: `/pre.bin: OK`
  - `post-check.log`: `/post.bin: OK`
- cleanup was clean:
  - no active iSCSI sessions
  - no blockmaster/blockvolume/iscsi-target processes

## Product Fix Proven By The Gate

The first live attempts exposed a real product gap: if an assignment arrived
while durable recovery still blocked local readiness, the host dropped that
assignment and did not replay it after recovery. The returned old primary was
safe (`Healthy=false`, `FrontendPrimaryReady=false`) but not properly projected
as a supporting `replica_ready` replica.

Fix in `c235b86`:

```text
local readiness blocked assignment
-> durable recovery clears block
-> replication volume is installed
-> last blocked assignment is replayed
-> returned replica is admitted as SUPPORTING
-> frontend remains gated
```

The final run confirms the intended log and status:

```text
replaying assignment after local readiness cleared
admitted as SUPPORTING replica r1 ... frontend remains gated
ReplicationRole=replica_ready
FrontendPrimaryReady=false
Healthy=false
```

## Non-Claims

This D5 live gate does not claim automatic failback or automatic rebuild
execution. It proves the returned replica is visible, fenced from frontend use,
and usable as a healthy peer after recovery evidence.

## Product Surface Gate

The K8s product-surface requirement is covered by
`TestOpsReturnedReplicaFromBundleSurfacesAcrossReportExplainDashboard`.

That gate drives one returned-replica bundle through:

- `sw-block ops report --from-bundle`
- `sw-block ops operator-status --from-bundle` using the CRD status DTO writer
- `sw-block ops explain volume --from-bundle`
- `sw-block ops dashboard --from-bundle` and `/operator-snapshot.json`

All four surfaces carry the same returned-replica projection:

```text
managed_volume_returned_replica=pvc-returned replica=r1 state=fenced reason=returned_replica_frontend_fenced
authority.reintegrate_returned_replica mode=dry_run decision=rejected reason=policy_disabled
```

The explain text was tightened to include the volume id on returned-replica
lines, matching report summary shape and avoiding multi-volume ambiguity.

## Verdict

D5 returned-replica safety and product-surface projection are PASS. The next
Phase 46 work is the multi-volume close gate.
